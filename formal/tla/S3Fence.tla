------------------------------ MODULE S3Fence ------------------------------
(***************************************************************************)
(* The ownership path of sediment's S3 durability (native/.../src/s3), *)
(* at the grain of object-store requests.                                  *)
(*                                                                         *)
(* Hosts open a database (prepare), commit (upload_frame + confirm_         *)
(* ownership), checkpoint (start_new_epoch) and publish (seal_epoch,       *)
(* upload_snapshot, manifest PUT, collect_garbage), and may be killed at   *)
(* any step. Every store write may succeed, be refused (412, only when its *)
(* precondition is false), land with its answer lost, not land, or stay in *)
(* flight and land later if its precondition still holds then. Lease       *)
(* expiry is any clock: a host may always take over, and a stale host's    *)
(* lease checks never stop it, so safety rests on the store alone.         *)
(*                                                                         *)
(* A database is a sequence of commits; a log frame carries the whole      *)
(* history it ends (standing for the CRC chain); a snapshot carries the    *)
(* history folded into it. Offsets count frames from 1.                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Node,            \* hosts
    MaxCommits,      \* commit attempts per host, whole behaviour
    MaxCheckpoints,  \* truncating checkpoints per host
    MaxOpens,        \* opens per host
    MaxFaults,       \* lost, failed or delayed answers, whole behaviour
    MaxKills,        \* host kills, whole behaviour
    Delays,          \* may a write stay in flight and land later
    Async,           \* durability: :async (commit locally, upload in order later)
    Patches          \* behaviours switched on by name; {} is the code

VARIABLES
    \* ---- the object store
    man,        \* manifest.json: [present, e, body, ver]
    objs,       \* log objects [e, o, kind ("frame"|"seal"), hist, gen]
    snaps,      \* snapshots [e, hist]
    leaseGen,   \* lease.json's generation (takeover is always possible: any clock)
    inflight,   \* writes whose caller gave up, still able to land
    \* ---- per host
    R,
    \* ---- counters
    ctr,        \* fresh versions and bodies
    faults, kills,
    \* ---- ghosts
    acked,      \* histories some commit was acknowledged for (async: by sync: true
                \* or flush, i.e. once its upload was confirmed)
    committed   \* histories some host committed locally (async), or attempted (sync)

vars == <<man, objs, snaps, leaseGen, inflight, R, ctr, faults, kills, acked, committed>>

-----------------------------------------------------------------------------
NoFrame == [e |-> [s |-> 0, g |-> 0], o |-> 0, kind |-> "none", hist |-> <<>>, gen |-> 0]
NoMan == [present |-> FALSE, e |-> [s |-> 0, g |-> 0], body |-> 0, ver |-> 0]

\* db: the durable history (confirmed in S3); ldb: the local one, which is db
\* in sync mode and runs ahead of it in async mode.
R0 == [pc |-> "off", gen |-> 0, db |-> <<>>, ldb |-> <<>>, img |-> <<>>, e |-> [s |-> 0, g |-> 0], o |-> 1,
       mver |-> 0, poisoned |-> "no", orphans |-> {}, frame |-> NoFrame,
       seal |-> [on |-> FALSE, e |-> [s |-> 0, g |-> 0], o |-> 0],
       snapPend |-> FALSE, unconf |-> {}, nc |-> 0, ncp |-> 0, nopen |-> 0,
       gcBelow |-> 0, stalled |-> FALSE]

Patch(p) == p \in Patches
CanFault == faults < MaxFaults

IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a
Extends1(h, prev) == Len(h) = Len(prev) + 1 /\ SubSeq(h, 1, Len(prev)) = prev

At(e, o) == {x \in objs : x.e = e /\ x.o = o}
Taken(e, o) == At(e, o) # {}
The(S) == CHOOSE x \in S : TRUE
Beyond(e, o) == \E x \in objs : x.e = e /\ x.o > o

SnapOf(e) == {s \in snaps : s.e = e}

MaxOff == MaxCommits * Cardinality(Node) + 2

\* restore.rs fetch_log + logfmt.rs verify_segments, from offset o on top of h.
RECURSIVE Walk(_, _, _, _)
Walk(e, o, h, n) ==
    IF ~Taken(e, o)
    THEN IF Beyond(e, o) THEN [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]
         ELSE [ok |-> TRUE, h |-> h, sealed |-> FALSE, n |-> n]
    ELSE LET x == The(At(e, o)) IN
         IF x.kind = "seal"
         THEN [ok |-> ~Beyond(e, o), h |-> h, sealed |-> TRUE, n |-> n]
         ELSE IF Extends1(x.hist, h) THEN Walk(e, o + 1, x.hist, n + 1)
         ELSE [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]

Restore(m) ==
    IF SnapOf(m.e) = {} THEN [ok |-> FALSE, h |-> <<>>, sealed |-> FALSE, n |-> 0]
    ELSE Walk(m.e, 1, The(SnapOf(m.e)).hist, 0)

-----------------------------------------------------------------------------
(* Store writes with faults. A write's outcome: "ok" (landed, answered),   *)
(* "lost" (landed, answer lost), "fail" (never lands), "late" (in flight). *)

\* "trunc" ("NoChecksum"): the connection breaks right after the headers and the server
\* stores an empty object (SeaweedFS did). With x-amz-checksum-sha256 on every
\* upload the server rejects the body instead: the "fail" outcome.
Outcomes(cond) ==
    IF cond
    THEN {"ok"} \cup (IF CanFault THEN {"lost", "fail"} \cup
                                       (IF Delays THEN {"late"} ELSE {}) \cup
                                       (IF Patch("NoChecksum") THEN {"trunc"} ELSE {})
                                  ELSE {})
    ELSE {"refused"} \cup (IF CanFault THEN {"fail"} \cup
                                          (IF Delays THEN {"late"} ELSE {}) ELSE {})

Charge(out) == faults' = IF out \in {"lost", "fail", "late", "trunc"} THEN faults + 1 ELSE faults

\* What a truncated upload leaves: an empty manifest (restores nothing) or frame.
EmptyMan == [present |-> TRUE, e |-> [s |-> 0, g |-> 0], body |-> 0, ver |-> ctr + 1]
Empty(f) == [f EXCEPT !.hist = <<>>]

NewMan(e, body) == [present |-> TRUE, e |-> e, body |-> body, ver |-> ctr + 1]

-----------------------------------------------------------------------------
(* Opening: s3::prepare_fresh *)

\* Lease::acquire. Any clock: the previous owner may be judged expired.
\* "LeaseHolds" (an assumption for liveness, not a fault): no host opens while another
\* holds the lease, from its own open on, unless it was poisoned (its renewals lapsed),
\* i.e. clocks agree within the lease TTL, so Lease::acquire refuses. A failed open's
\* lease lingers until its TTL in the code; for liveness that is the same as released.
Open(n) ==
    /\ R[n].pc = "off" /\ R[n].nopen < MaxOpens
    /\ Patch("LeaseHolds") => \A m \in Node \ {n} : R[m].pc = "off" \/ R[m].poisoned # "no"
    /\ leaseGen' = leaseGen + 1
    /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.pc = "take", !.gen = leaseGen + 1,
                                        !.nopen = R[n].nopen + 1, !.nc = R[n].nc,
                                        !.ncp = R[n].ncp]]
    /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, kills, acked, committed>>

\* With no manifest nothing was ever acknowledged: only epoch-0 snapshots, which only a
\* bootstrap writes before its manifest, may be there. Log objects or any other
\* snapshot mean a deleted manifest, and the open refuses. "RefuseLeftovers" (the earlier
\* behaviour) refuses any snapshot.
Leftovers == {x \in snaps : x.e.s = 0}
Refuse == objs # {} \/ snaps \ Leftovers # {} \/ (Patch("RefuseLeftovers") /\ snaps # {})

\* GET manifest, then PUT the takeover (If-Match), or bootstrap a new database.
\* "ListBeforeTakeover" restores first and takes over afterwards.
Take(n) ==
    /\ R[n].pc = "take"
    /\ IF ~man.present /\ Refuse
       THEN \* objects without a manifest: refuse to build over them (the open fails)
            /\ R' = [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, acked, committed>>
       ELSE IF ~man.present
       THEN \* bootstrap: snapshot of the empty database, create-only manifest. An
            \* epoch-0 snapshot of an interrupted bootstrap stays for GC;
            \* "DeleteLeftovers" deletes it first.
            LET e == [s |-> 0, g |-> R[n].gen]
                kept == IF Patch("DeleteLeftovers") THEN snaps \ Leftovers ELSE snaps
            IN
            /\ snaps' = kept \cup {[e |-> e, hist |-> <<>>]}
            /\ \E out \in Outcomes(TRUE) :
                 /\ Charge(out)
                 /\ ctr' = ctr + 1
                 /\ man' = IF out \in {"ok", "lost"} THEN NewMan(e, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
                 /\ inflight' = IF out = "late"
                                THEN inflight \cup {[k |-> "man", cond |-> 0,
                                                     rec |-> NewMan(e, ctr + 1)]}
                                ELSE inflight
                 /\ R' = IF out = "ok"
                         THEN [R EXCEPT ![n].pc = "run", ![n].e = e, ![n].o = 1,
                                        ![n].mver = ctr + 1, ![n].db = <<>>, ![n].ldb = <<>>]
                         ELSE [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<objs, acked, committed>>
       ELSE IF Patch("ListBeforeTakeover")
       THEN /\ R' = [R EXCEPT ![n].pc = "restore", ![n].e = man.e, ![n].mver = man.ver]
            /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, acked, committed>>
       ELSE /\ \E out \in Outcomes(TRUE) :
                 /\ Charge(out)
                 /\ ctr' = ctr + 1
                 /\ man' = IF out \in {"ok", "lost"} THEN NewMan(man.e, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
                 /\ inflight' = IF out = "late"
                                THEN inflight \cup {[k |-> "man", cond |-> man.ver,
                                                     rec |-> NewMan(man.e, ctr + 1)]}
                                ELSE inflight
                 /\ R' = IF out = "ok"
                         THEN [R EXCEPT ![n].pc = "restore", ![n].e = man.e,
                                        ![n].mver = ctr + 1]
                         ELSE [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<objs, snaps, acked, committed>>
    /\ UNCHANGED <<leaseGen, kills, committed>>

\* restore::restore: list and verify the epoch, then fold it into a fresh epoch
\* ("ContinueEpoch": keep appending to the restored one unless it is sealed).
RestoreStep(n) ==
    /\ R[n].pc = "restore"
    /\ LET W == Restore([e |-> R[n].e]) IN
       IF ~W.ok
       THEN R' = [R EXCEPT ![n].pc = "off"]
       ELSE IF Patch("ListBeforeTakeover")
       THEN R' = [R EXCEPT ![n].pc = "latetake", ![n].db = W.h, ![n].ldb = W.h, ![n].o = W.n + 1]
       ELSE IF Patch("ContinueEpoch") /\ ~W.sealed
       THEN R' = [R EXCEPT ![n].pc = "run", ![n].db = W.h, ![n].ldb = W.h, ![n].o = W.n + 1]
       ELSE \* every open moves to an epoch of its own
            R' = [R EXCEPT ![n].pc = "compact", ![n].db = W.h, ![n].ldb = W.h]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* Negative control: the takeover PUT after the log was read.
LateTake(n) ==
    /\ R[n].pc = "latetake"
    /\ \E out \in Outcomes(man.ver = R[n].mver) :
         /\ Charge(out)
         /\ ctr' = ctr + 1
         /\ man' = IF out \in {"ok", "lost"} THEN NewMan(man.e, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
         /\ inflight' = IF out = "late"
                        THEN inflight \cup {[k |-> "man", cond |-> R[n].mver,
                                             rec |-> NewMan(man.e, ctr + 1)]}
                        ELSE inflight
         /\ R' = IF out = "ok" THEN [R EXCEPT ![n].pc = "compact", ![n].mver = ctr + 1]
                 ELSE [R EXCEPT ![n].pc = "off"]
    /\ UNCHANGED <<objs, snaps, leaseGen, kills, acked, committed>>

\* checkpoint_local + upload_snapshot + manifest PUT (If-Match the takeover).
Compact(n) ==
    /\ R[n].pc = "compact"
    /\ LET e == [s |-> R[n].e.s + 1, g |-> R[n].gen] IN
       /\ snaps' = snaps \cup {[e |-> e, hist |-> R[n].db]}
       /\ \E out \in Outcomes(man.ver = R[n].mver) :
            /\ Charge(out)
            /\ ctr' = ctr + 1
            /\ man' = IF out \in {"ok", "lost"} THEN NewMan(e, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
            /\ inflight' = IF out = "late"
                           THEN inflight \cup {[k |-> "man", cond |-> R[n].mver,
                                                rec |-> NewMan(e, ctr + 1)]}
                           ELSE inflight
            /\ R' = IF out = "ok"
                    THEN [R EXCEPT ![n].pc = "run", ![n].e = e, ![n].o = 1,
                                   ![n].mver = ctr + 1]
                    ELSE [R EXCEPT ![n].pc = "off"]
    /\ UNCHANGED <<objs, leaseGen, kills, acked, committed>>

-----------------------------------------------------------------------------
(* Committing: S3DurableStorage::upload_frame *)

\* "fenced": another writer's object or manifest; "indeterminate": a commit of
\* ours that reported failure turned out to be in S3 (foreign_object).
PoisonWhy(n, why) == [R EXCEPT ![n].poisoned = why, ![n].pc = "run"]
Poison(n) == PoisonWhy(n, "fenced")
\* Frames sent without a clear answer ("OneOrphan": only the last is kept).
Remember(os, f) == IF Patch("OneOrphan") THEN {f} ELSE os \cup {f}
Running(n) == R[n].pc = "run" /\ R[n].poisoned = "no"

\* Sync: a commit is a create-only PUT of its frame, then (on any error) a GET
\* of the key. Async: the uploader's PUT of the next frame of the local
\* history not yet durable (in log order; a failed upload is retried with the
\* same frame, so a 412 on an identical object is ours).
CommitPut(n) ==
    /\ Running(n) /\ ~R[n].snapPend /\ ~R[n].stalled
    /\ IF Async THEN Len(R[n].ldb) > Len(R[n].db) ELSE R[n].nc < MaxCommits
    /\ LET f == [e |-> R[n].e, o |-> R[n].o, kind |-> "frame",
                 hist |-> IF Async THEN SubSeq(R[n].ldb, 1, Len(R[n].db) + 1)
                          ELSE Append(R[n].db, <<n, R[n].nc + 1>>),
                 gen |-> R[n].gen]
           free == ~Taken(f.e, f.o)
       IN \E out \in Outcomes(free) :
            /\ Charge(out)
            /\ objs' = IF out \in {"ok", "lost"} THEN objs \cup {f}
                       ELSE IF out = "trunc" THEN objs \cup {Empty(f)} ELSE objs
            /\ inflight' = IF out = "late" THEN inflight \cup {[k |-> "obj", rec |-> f]}
                           ELSE inflight
            /\ committed' = IF Async THEN committed ELSE committed \cup {f.hist}
            /\ LET R1 == IF Async THEN R ELSE [R EXCEPT ![n].nc = R[n].nc + 1] IN
               R' = CASE out \in {"ok", "lost"} ->
                           [R1 EXCEPT ![n].pc = "confirm", ![n].frame = f]
                      [] out = "refused" ->
                           \* 412: the key holds something; ours only if identical
                           IF The(At(f.e, f.o)) = f
                           THEN [R1 EXCEPT ![n].pc = "confirm", ![n].frame = f]
                           ELSE IF Patch("RewriteOrphan") /\ The(At(f.e, f.o)) \in R[n].orphans
                           THEN [R1 EXCEPT ![n].pc = "rewrite", ![n].frame = f]
                           ELSE [R1 EXCEPT ![n].poisoned =
                                       IF The(At(f.e, f.o)) \in R[n].orphans
                                       THEN "indeterminate" ELSE "fenced"]
                      [] OTHER -> \* failed: the commit rolls back (async: retried;
                           \* "StallOnFailure": the uploader stops, the writer runs on)
                           [R1 EXCEPT ![n].orphans = Remember(R[n].orphans, f),
                                      ![n].stalled = Async /\ Patch("StallOnFailure")]
    /\ UNCHANGED <<man, snaps, leaseGen, ctr, kills, acked>>

\* Async: a commit returns once it is in the local log (visible to others).
AsyncCommit(n) ==
    /\ Async /\ Running(n) /\ R[n].nc < MaxCommits
    /\ LET h == Append(R[n].ldb, <<n, R[n].nc + 1>>) IN
       /\ R' = [R EXCEPT ![n].ldb = h, ![n].nc = R[n].nc + 1]
       /\ committed' = committed \cup {h}
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked>>

\* Negative control (the pre-audit code): overwrite our orphan with If-Match.
Rewrite(n) ==
    /\ R[n].pc = "rewrite"
    /\ LET f == R[n].frame IN
       /\ objs' = (objs \ At(f.e, f.o)) \cup {f}
       /\ R' = [R EXCEPT ![n].pc = "confirm"]
    /\ UNCHANGED <<man, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* Sync: the commit is acknowledged. Async: the frame is durable; a sync: true
\* commit or a flush waiting for it returns now.
Ack(n) ==
    /\ acked' = acked \cup {R[n].frame.hist}
    /\ R' = [R EXCEPT ![n].pc = "run", ![n].db = R[n].frame.hist,
                      ![n].ldb = IF Async THEN R[n].ldb ELSE R[n].frame.hist,
                      ![n].o = R[n].o + 1, ![n].frame = NoFrame]

\* confirm_ownership: HEAD manifest; ack only if it is still the one we wrote.
Confirm(n) ==
    /\ R[n].pc = "confirm"
    /\ IF Patch("NoConfirm") \/ man.ver = R[n].mver
       THEN Ack(n)
       ELSE \/ /\ \E m \in R[n].unconf : m.body = man.body
               /\ Ack(n)
            \/ /\ ~\E m \in R[n].unconf : m.body = man.body
               /\ R' = Poison(n) /\ acked' = acked
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, committed>>

\* The HEAD itself fails: the frame landed but the commit reports failure.
ConfirmFails(n) ==
    /\ R[n].pc = "confirm" /\ CanFault
    /\ faults' = faults + 1
    /\ R' = [R EXCEPT ![n].pc = "run", ![n].orphans = Remember(R[n].orphans, R[n].frame),
                      ![n].frame = NoFrame]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, kills, acked, committed>>

-----------------------------------------------------------------------------
(* Checkpoint and publication *)

\* truncate() -> start_new_epoch: the DB file now holds db.
\* Async: the uploader drains first, so the old epoch holds every frame
\* ("NoDrain": checkpoint with frames still pending).
Checkpoint(n) ==
    /\ Running(n) /\ R[n].ncp < MaxCheckpoints
    /\ (~Async \/ Patch("NoDrain") \/ R[n].ldb = R[n].db)
    /\ R' = [R EXCEPT ![n].ncp = R[n].ncp + 1, ![n].img = R[n].ldb,
                      ![n].seal = IF R[n].seal.on THEN R[n].seal
                                  ELSE [on |-> TRUE, e |-> R[n].e, o |-> R[n].o],
                      ![n].e = [s |-> R[n].e.s + 1, g |-> R[n].gen],
                      ![n].o = 1, ![n].snapPend = TRUE]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* publish_snapshot, first step: adopt a manifest of ours that landed late.
Published(n, m, ver) ==
    LET behind == m.e # R[n].e IN
    [R EXCEPT ![n].mver = ver, ![n].unconf = {},
              ![n].snapPend = behind,
              ![n].seal = IF behind THEN [on |-> TRUE, e |-> m.e, o |-> 1]
                          ELSE [on |-> FALSE, e |-> [s |-> 0, g |-> 0], o |-> 0],
              ![n].gcBelow = m.e.s,
              ![n].pc = "gc"]

\* publish_snapshot: seal the old epoch first (idempotent: a create-only PUT that finds our
\* own seal succeeds), then check for a manifest of ours that landed without an answer.
PublishBegin(n) ==
    /\ Running(n) /\ R[n].snapPend
    /\ R' = [R EXCEPT ![n].pc = IF Patch("NoSeal") THEN "adopt" ELSE "seal"]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* adopt_manifest_if_ours (only when a manifest PUT of ours went unanswered): the manifest
\* is still the one we hold (go on), or one we sent (adopt it), or another writer's.
Adopt(n) ==
    /\ R[n].pc = "adopt"
    /\ IF R[n].unconf = {} \/ man.ver = R[n].mver
       THEN R' = [R EXCEPT ![n].pc = "snap"]
       ELSE IF \E m \in R[n].unconf : m.body = man.body
       THEN R' = Published(n, man, man.ver)
       ELSE R' = Poison(n)
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* seal_epoch: create-only seal at the old epoch's end offset.
Seal(n) ==
    /\ R[n].pc = "seal"
    /\ LET s == [e |-> R[n].seal.e, o |-> R[n].seal.o, kind |-> "seal", hist |-> <<>>,
                 gen |-> R[n].gen]
           free == ~Taken(s.e, s.o)
       IN \E out \in Outcomes(free) :
            /\ Charge(out)
            /\ objs' = IF out \in {"ok", "lost"} THEN objs \cup {s} ELSE objs
            /\ inflight' = IF out = "late" THEN inflight \cup {[k |-> "obj", rec |-> s]}
                           ELSE inflight
            /\ R' = CASE out \in {"ok", "lost"} -> [R EXCEPT ![n].pc = "adopt"]
                      [] out = "refused" ->
                           IF The(At(s.e, s.o)) = s THEN [R EXCEPT ![n].pc = "adopt"]
                           ELSE IF The(At(s.e, s.o)) \in R[n].orphans
                           THEN PoisonWhy(n, "indeterminate")
                           ELSE Poison(n)
                      [] OTHER -> [R EXCEPT ![n].pc = "run"]
    /\ UNCHANGED <<man, snaps, leaseGen, ctr, kills, acked, committed>>

\* upload_snapshot from the checkpoint's image of the DB file (key unique per
\* epoch; every retry writes the same bytes, whatever the DB file holds by
\* then). "LiveSnapshot": upload the live local state instead.
Snap(n) ==
    /\ R[n].pc = "snap"
    /\ \/ /\ snaps' = snaps \cup {[e |-> R[n].e,
                                     hist |-> IF Patch("LiveSnapshot") THEN R[n].ldb ELSE R[n].img]}
          /\ R' = [R EXCEPT ![n].pc = "man"]
          /\ faults' = faults
       \/ /\ CanFault /\ faults' = faults + 1
          /\ R' = [R EXCEPT ![n].pc = "run"]
          /\ snaps' = snaps
    /\ UNCHANGED <<man, objs, leaseGen, inflight, ctr, kills, acked, committed>>

\* Manifest PUT, If-Match the version we hold.
Man(n) ==
    /\ R[n].pc = "man"
    /\ LET rec == NewMan(R[n].e, ctr + 1) IN
       \E out \in Outcomes(man.ver = R[n].mver) :
         /\ Charge(out)
         /\ ctr' = ctr + 1
         /\ man' = IF out \in {"ok", "lost"} THEN rec
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
         /\ inflight' = IF out = "late"
                        THEN inflight \cup {[k |-> "man", cond |-> R[n].mver, rec |-> rec]}
                        ELSE inflight
         /\ LET kept == IF Patch("NoChain") THEN {} ELSE R[n].unconf \cup {rec} IN
            R' = CASE out = "ok" -> Published(n, rec, rec.ver)
                   [] out = "refused" ->
                        IF \E m \in kept : m.body = man.body
                        THEN Published(n, man, man.ver)
                        ELSE Poison(n)
                   [] OTHER -> [R EXCEPT ![n].pc = "run", ![n].unconf = kept]
    /\ UNCHANGED <<objs, snaps, leaseGen, kills, acked, committed>>

\* collect_garbage: old epochs are deleted ("KeepTails": keep each one's last
\* object as a tombstone, as an earlier version did; not needed for safety).
LastOff(e) == CHOOSE o \in {x.o : x \in {y \in objs : y.e = e}} :
               \A x \in objs : x.e = e => x.o <= o
GC(n) ==
    /\ R[n].pc = "gc"
    /\ objs' = {x \in objs : x.e.s >= R[n].gcBelow \/ (Patch("KeepTails") /\ x.o = LastOff(x.e))}
    /\ snaps' = {s \in snaps : s.e.s >= R[n].gcBelow}
    /\ R' = [R EXCEPT ![n].pc = "run"]
    /\ UNCHANGED <<man, leaseGen, inflight, ctr, faults, kills, acked, committed>>

-----------------------------------------------------------------------------
(* Environment *)

Kill(n) ==
    /\ R[n].pc # "off" /\ kills < MaxKills
    /\ kills' = kills + 1
    /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.nc = R[n].nc, !.ncp = R[n].ncp,
                                        !.nopen = R[n].nopen]]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, acked, committed>>

\* A write the caller gave up on reaches the store; its precondition decides.
Land(w) ==
    /\ inflight' = inflight \ {w}
    /\ IF w.k = "obj"
       THEN /\ objs' = IF Taken(w.rec.e, w.rec.o) THEN objs ELSE objs \cup {w.rec}
            /\ man' = man
       ELSE \* "IgnoreIfMatch": a provider that doesn't enforce If-Match (the probe
            \* refuses such providers at open).
            /\ man' = IF (w.cond = 0 /\ ~man.present)
                         \/ (w.cond # 0 /\ (man.ver = w.cond \/ Patch("IgnoreIfMatch")))
                      THEN w.rec ELSE man
            /\ objs' = objs
    /\ UNCHANGED <<snaps, leaseGen, R, ctr, faults, kills, acked, committed>>

-----------------------------------------------------------------------------
Init ==
    /\ man = NoMan /\ objs = {} /\ snaps = {} /\ leaseGen = 0 /\ inflight = {}
    /\ R = [n \in Node |-> R0]
    /\ ctr = 0 /\ faults = 0 /\ kills = 0 /\ acked = {} /\ committed = {<<>>}

Next ==
    \/ \E n \in Node :
         \/ Open(n) \/ Take(n) \/ RestoreStep(n) \/ LateTake(n) \/ Compact(n)
         \/ CommitPut(n) \/ AsyncCommit(n) \/ Rewrite(n) \/ Confirm(n) \/ ConfirmFails(n)
         \/ Checkpoint(n) \/ PublishBegin(n) \/ Seal(n) \/ Adopt(n) \/ Snap(n) \/ Man(n) \/ GC(n)
         \/ Kill(n)
    \/ \E w \in inflight : Land(w)

Spec == Init /\ [][Next]_vars

\* Liveness: each host keeps taking its own steps (a pool reopening a closed
\* connection, the uploader retrying, a publication going on). No fairness for
\* what users or the environment do: commits, checkpoints, kills, late landings.
Progress(n) ==
    \/ Open(n) \/ Take(n) \/ RestoreStep(n) \/ LateTake(n) \/ Compact(n)
    \/ CommitPut(n) \/ Rewrite(n) \/ Confirm(n) \/ ConfirmFails(n)
    \/ PublishBegin(n) \/ Seal(n) \/ Adopt(n) \/ Snap(n) \/ Man(n) \/ GC(n)
LiveSpec == Spec /\ \A n \in Node : WF_vars(Progress(n))

-----------------------------------------------------------------------------
(* Properties *)

\* Every acknowledged commit is in a restore of the current manifest.
AckedDurable ==
    man.present =>
        LET W == Restore(man) IN \A h \in acked : W.ok /\ IsPrefix(h, W.h)

\* The current manifest always restores (no gap, no broken chain, snapshot present).
RestoreOK == man.present => Restore(man).ok

\* What a restore finds is a history some host committed: a prefix of the
\* commit order, without holes or reordering (async: possibly behind).
RestoreCommitted ==
    man.present => LET W == Restore(man) IN W.ok => W.h \in committed

\* One host: it never takes itself for another writer, whatever the faults.
SoleNeverFenced ==
    Cardinality(Node) = 1 => \A n \in Node : R[n].poisoned # "fenced"

\* Situations each positive config must reach (listed under _POSSIBLE in its .cfg;
\* TLC fails if one is never witnessed), so the invariants can't pass vacuously.
AckAfterTakeover == \E h \in acked : \E n \in Node : R[n].gen >= 2 /\ Len(h) >= 2
AckAfterCheckpoint == acked # {} /\ man.e.s >= 1
Poisoned == \E n \in Node : R[n].poisoned # "no"
LateLanding == inflight # {}
\* Async runs ahead of what is durable by more than one commit.
AsyncAhead == \E n \in Node : Len(R[n].ldb) >= Len(R[n].db) + 2
\* Async commits while a snapshot publication is pending.
CommitWhileSnapPending == \E n \in Node : R[n].snapPend /\ Len(R[n].ldb) > Len(R[n].img)

\* The database was created next to an interrupted bootstrap's snapshot.
BootstrapOverLeftover == man.present /\ man.e.s = 0 /\ \E x \in Leftovers : x.e # man.e

\* A sole async writer is never poisoned: the uploader retries the same frame until it
\* lands, so the sync-mode indeterminate case (a rolled-back commit whose frame later
\* turns up at its offset) can't arise.
AsyncSoleNeverPoisoned ==
    (Async /\ Cardinality(Node) = 1) => \A n \in Node : R[n].poisoned = "no"

\* Liveness (LiveSpec; faults are bounded by MaxFaults, so they stop): every local
\* commit eventually becomes durable unless its writer is killed or poisoned ...
EventuallyDurable ==
    \A n \in Node : \A k \in 1..MaxCommits :
        (Len(R[n].ldb) >= k) ~> (Len(R[n].db) >= k \/ R[n].pc = "off" \/ R[n].poisoned # "no")
\* ... and whenever no host runs (at the start, after a kill), one eventually does.
EventuallyRuns == (\A n \in Node : R[n].pc = "off") ~> (\E n \in Node : Running(n))

\* Action property: the manifest never moves backwards (an older epoch or version),
\* i.e. the database in S3 never rolls back to an earlier state.
ManifestForward ==
    [][man.present => (man'.present /\ man.e.s <= man'.e.s /\ man.ver <= man'.ver)]_man

StateConstraint == ctr <= 12

Perms == Permutations(Node)
=============================================================================
