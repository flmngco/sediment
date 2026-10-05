------------------------------ MODULE S3Fence ------------------------------
(***************************************************************************)
(* The ownership path of sediment's S3 durability (native/.../src/s3), *)
(* at the grain of object-store requests.                                  *)
(*                                                                         *)
(* Hosts open a database (prepare), commit (upload_frame + confirm_         *)
(* ownership), checkpoint (start_new_epoch) and publish (seal_epoch,       *)
(* upload_snapshot, manifest PUT, collect_garbage), destroy it (destroy.rs:*)
(* tombstone, then purge), and may be killed at any step. Every store write may succeed, be refused (412, only when its *)
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
    Readers,         \* reads (replica refreshes, restores) next to the hosts, whole behaviour
    Retain,          \* retain_epochs: past epochs a manifest keeps (GC spares them)
    Destroys,        \* destroys per host
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
    committed,  \* histories some host committed locally (async), or attempted (sync)
    \* ---- the reader
    rd,         \* a replica refresh or restore/3 in progress, and a replica's log cache
    shown,      \* ghost: every history a reader returned
    tomb        \* ghost: the generation of the last destroy that took effect (0: none)

vars == <<man, objs, snaps, leaseGen, inflight, R, ctr, faults, kills, acked, committed>>
rvars == <<rd, shown, tomb>>
allvars == <<vars, rvars>>

-----------------------------------------------------------------------------
NoFrame == [e |-> [s |-> 0, g |-> 0], o |-> 0, kind |-> "none", hist |-> <<>>, gen |-> 0]
\* tomb: a destroy's tombstone (layout.rs Tombstone): no database; every object of an
\* epoch whose generation is at most gen is dead.
NoMan == [present |-> FALSE, e |-> [s |-> 0, g |-> 0], gen |-> 0, ret |-> <<>>, body |-> 0, ver |-> 0,
          tomb |-> FALSE]

\* db: the durable history (confirmed in S3); ldb: the local one, which is db
\* in sync mode and runs ahead of it in async mode.
R0 == [pc |-> "off", gen |-> 0, db |-> <<>>, ldb |-> <<>>, img |-> <<>>, e |-> [s |-> 0, g |-> 0], o |-> 1,
       mver |-> 0, poisoned |-> "no", orphans |-> {}, frame |-> NoFrame,
       seal |-> [on |-> FALSE, e |-> [s |-> 0, g |-> 0], o |-> 0],
       snapPend |-> FALSE, unconf |-> {}, nc |-> 0, ncp |-> 0, nopen |-> 0,
       gcBelow |-> 0, gcKeep |-> {}, stalled |-> FALSE, nd |-> 0]

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
EmptyMan == [present |-> TRUE, e |-> [s |-> 0, g |-> 0], gen |-> 0, ret |-> <<>>, body |-> 0,
             ver |-> ctr + 1, tomb |-> FALSE]
Empty(f) == [f EXCEPT !.hist = <<>>]

\* gen: the lease generation of the writer that wrote it (Manifest::generation); ret: the
\* past epochs it retains, newest first (Manifest::history, at most Retain).
NewMan(e, g, ret, body) ==
    [present |-> TRUE, e |-> e, gen |-> g, ret |-> ret, body |-> body, ver |-> ctr + 1,
     tomb |-> FALSE]
\* A tombstone keeps the epoch sequence number of what it replaced (e.s): the next
\* database starts after it ("SeqFromZero": at 0, as a bootstrap without a manifest).
TombMan(g, sq, body) ==
    [present |-> TRUE, e |-> [s |-> sq, g |-> 0], gen |-> g, ret |-> <<>>, body |-> body,
     ver |-> ctr + 1, tomb |-> TRUE]
FirstSeq(m) == IF m.tomb /\ ~Patch("SeqFromZero") THEN m.e.s + 1 ELSE 0

\* A database: a manifest that is there and isn't a tombstone.
Live(m) == m.present /\ ~m.tomb
\* Objects (log objects and snapshots, by their key's epoch) a tombstone of generation g
\* ended.
Dead(g, x) == x.e.g <= g

Range(sq) == {sq[i] : i \in DOMAIN sq}
\* Manifest::advance from m to epoch e: m's epoch joins the retained ones.
Advanced(m, e) ==
    IF m.e = e \/ ~m.present THEN m.ret
    ELSE LET all == <<m.e>> \o m.ret IN SubSeq(all, 1, IF Len(all) < Retain THEN Len(all) ELSE Retain)
\* Epochs a manifest names: its own and the retained ones.
Retained(m) == {m.e} \cup Range(m.ret)

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
                                        !.ncp = R[n].ncp, !.nd = R[n].nd]]
    /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, kills, acked, committed>>

\* With no manifest nothing was ever acknowledged: only epoch-0 snapshots, which only a
\* bootstrap writes before its manifest, may be there. Log objects or any other
\* snapshot mean a deleted manifest, and the open refuses. "RefuseLeftovers" (the earlier
\* behaviour) refuses any snapshot.
Leftovers == {x \in snaps : x.e.s = 0}
Refuse == objs # {} \/ snaps \ Leftovers # {} \/ (Patch("RefuseLeftovers") /\ snaps # {})

\* GET manifest, then PUT the takeover (If-Match), or bootstrap a new database.
\* "ListBeforeTakeover" restores first and takes over afterwards.
\* A manifest (or tombstone) written with a generation at least ours means a newer lease
\* holder exists: this open is stale and refuses ("TakeAnyGeneration": takes it anyway).
Newer(n) == man.present /\ man.gen >= R[n].gen /\ ~Patch("TakeAnyGeneration")

Take(n) ==
    /\ R[n].pc = "take"
    /\ IF Newer(n)
       THEN /\ R' = [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, acked, committed>>
       ELSE IF ~man.present /\ Refuse
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
                 /\ man' = IF out \in {"ok", "lost"} THEN NewMan(e, R[n].gen, <<>>, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
                 /\ inflight' = IF out = "late"
                                THEN inflight \cup {[k |-> "man", cond |-> 0,
                                                     rec |-> NewMan(e, R[n].gen, <<>>, ctr + 1)]}
                                ELSE inflight
                 /\ R' = IF out = "ok"
                         THEN [R EXCEPT ![n].pc = "run", ![n].e = e, ![n].o = 1,
                                        ![n].mver = ctr + 1, ![n].db = <<>>, ![n].ldb = <<>>]
                         ELSE [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<objs, acked, committed>>
       ELSE IF man.tomb
       THEN \* a destroyed database: purge what the tombstone ended, then bootstrap a new one
            \* in its place (If-Match the tombstone), unless anything else is there
            LET ob == {x \in objs : ~Dead(man.gen, x)}
                sn == {x \in snaps : ~Dead(man.gen, x)}
                e == [s |-> FirstSeq(man), g |-> R[n].gen]
                rec == NewMan(e, R[n].gen, <<>>, ctr + 1)
            IN
            /\ objs' = ob
            /\ IF ob # {} \/ {x \in sn : x.e.s # e.s} # {}
               THEN /\ snaps' = sn
                    /\ R' = [R EXCEPT ![n].pc = "off"]
                    /\ UNCHANGED <<man, inflight, ctr, faults, acked, committed>>
               ELSE /\ snaps' = sn \cup {[e |-> e, hist |-> <<>>]}
                    /\ \E out \in Outcomes(TRUE) :
                         /\ Charge(out)
                         /\ ctr' = ctr + 1
                         /\ man' = IF out \in {"ok", "lost"} THEN rec
                                 ELSE IF out = "trunc" THEN EmptyMan ELSE man
                         /\ inflight' = IF out = "late"
                                        THEN inflight \cup {[k |-> "man", cond |-> man.ver, rec |-> rec]}
                                        ELSE inflight
                         /\ R' = IF out = "ok"
                                 THEN [R EXCEPT ![n].pc = "run", ![n].e = e, ![n].o = 1,
                                                ![n].mver = ctr + 1, ![n].db = <<>>, ![n].ldb = <<>>]
                                 ELSE [R EXCEPT ![n].pc = "off"]
                    /\ UNCHANGED <<acked, committed>>
       ELSE IF Patch("ListBeforeTakeover")
       THEN /\ R' = [R EXCEPT ![n].pc = "restore", ![n].e = man.e, ![n].mver = man.ver]
            /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, acked, committed>>
       ELSE /\ \E out \in Outcomes(TRUE) :
                 /\ Charge(out)
                 /\ ctr' = ctr + 1
                 /\ man' = IF out \in {"ok", "lost"} THEN NewMan(man.e, R[n].gen, man.ret, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
                 /\ inflight' = IF out = "late"
                                THEN inflight \cup {[k |-> "man", cond |-> man.ver,
                                                     rec |-> NewMan(man.e, R[n].gen, man.ret, ctr + 1)]}
                                ELSE inflight
                 /\ R' = IF out = "ok"
                         THEN [R EXCEPT ![n].pc = "restore", ![n].e = man.e,
                                        ![n].mver = ctr + 1]
                         ELSE [R EXCEPT ![n].pc = "off"]
            /\ UNCHANGED <<objs, snaps, acked, committed>>
    /\ UNCHANGED <<leaseGen, kills, committed>>

\* restore::restore: list and verify the epoch, then fold it into a fresh epoch
\* ("ContinueEpoch": keep appending to the restored one unless it is sealed). An
\* unsealed epoch is sealed at the end just listed first ("NoTakeoverSeal": not).
RestoreStep(n) ==
    /\ R[n].pc = "restore"
    /\ LET W == Restore([e |-> R[n].e]) IN
       IF ~W.ok
       THEN R' = [R EXCEPT ![n].pc = "off"]
       ELSE IF Patch("ListBeforeTakeover")
       THEN R' = [R EXCEPT ![n].pc = "latetake", ![n].db = W.h, ![n].ldb = W.h, ![n].o = W.n + 1]
       ELSE IF Patch("ContinueEpoch") /\ ~W.sealed
       THEN R' = [R EXCEPT ![n].pc = "run", ![n].db = W.h, ![n].ldb = W.h, ![n].o = W.n + 1]
       ELSE IF ~W.sealed /\ ~Patch("NoTakeoverSeal")
       THEN R' = [R EXCEPT ![n].pc = "tseal", ![n].db = W.h, ![n].ldb = W.h, ![n].o = W.n + 1]
       ELSE \* every open moves to an epoch of its own
            R' = [R EXCEPT ![n].pc = "compact", ![n].db = W.h, ![n].ldb = W.h]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, acked, committed>>

\* The takeover's seal: create-only at the end of the log it listed, so a late upload
\* of the old writer can't extend the epoch it restored. Taken (that upload, or another
\* host's seal, landed since the listing): list again. Any other error fails the open.
TakeSeal(n) ==
    /\ R[n].pc = "tseal"
    /\ LET s == [e |-> R[n].e, o |-> R[n].o, kind |-> "seal", hist |-> <<>>, gen |-> R[n].gen]
           free == ~Taken(s.e, s.o)
       IN \E out \in Outcomes(free) :
            /\ Charge(out)
            /\ objs' = IF out \in {"ok", "lost"} THEN objs \cup {s} ELSE objs
            /\ inflight' = IF out = "late" THEN inflight \cup {[k |-> "obj", rec |-> s]}
                           ELSE inflight
            /\ R' = CASE out = "ok" -> [R EXCEPT ![n].pc = "compact"]
                      [] out = "refused" ->
                           IF The(At(s.e, s.o)) = s THEN [R EXCEPT ![n].pc = "compact"]
                           ELSE [R EXCEPT ![n].pc = "restore"]
                      [] OTHER -> [R EXCEPT ![n].pc = "off"]
    /\ UNCHANGED <<man, snaps, leaseGen, ctr, kills, acked, committed>>

\* Negative control: the takeover PUT after the log was read.
LateTake(n) ==
    /\ R[n].pc = "latetake"
    /\ \E out \in Outcomes(man.ver = R[n].mver) :
         /\ Charge(out)
         /\ ctr' = ctr + 1
         /\ man' = IF out \in {"ok", "lost"} THEN NewMan(man.e, R[n].gen, man.ret, ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
         /\ inflight' = IF out = "late"
                        THEN inflight \cup {[k |-> "man", cond |-> R[n].mver,
                                             rec |-> NewMan(man.e, R[n].gen, man.ret, ctr + 1)]}
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
            /\ man' = IF out \in {"ok", "lost"} THEN NewMan(e, R[n].gen, Advanced(man, e), ctr + 1)
                        ELSE IF out = "trunc" THEN EmptyMan ELSE man
            /\ inflight' = IF out = "late"
                           THEN inflight \cup {[k |-> "man", cond |-> R[n].mver,
                                                rec |-> NewMan(e, R[n].gen, Advanced(man, e), ctr + 1)]}
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
              ![n].gcBelow = m.e.s, ![n].gcKeep = Range(m.ret),
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
    /\ LET rec == NewMan(R[n].e, R[n].gen, Advanced(man, R[n].e), ctr + 1) IN
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
    /\ objs' = {x \in objs : x.e.s >= R[n].gcBelow \/ x.e \in R[n].gcKeep
                             \/ (Patch("KeepTails") /\ x.o = LastOff(x.e))}
    /\ snaps' = {s \in snaps : s.e.s >= R[n].gcBelow \/ s.e \in R[n].gcKeep}
    /\ R' = [R EXCEPT ![n].pc = "run"]
    /\ UNCHANGED <<man, leaseGen, inflight, ctr, faults, kills, acked, committed>>

-----------------------------------------------------------------------------
(* Readers: replica::stage (a refresh, full or from the replica's log cache) and   *)
(* restore_to (the current state, or a retained past epoch). Read the manifest,    *)
(* then list and verify the log, then read the manifest again and return the       *)
(* history only if the manifest has the same generation and epoch               *)
(* (restore::still_retained). The                                                     *)
(* current epoch is used only from a manifest its     *)
(* writer wrote, or when it is sealed ("ReadTransitional": always; "NoRecheck": no  *)
(* second read).                                                                    *)

NoEpoch == [s |-> 0, g |-> 0]
NoCache == [on |-> FALSE, e |-> NoEpoch, o |-> 0, h |-> <<>>]
RD0 == [pc |-> "idle", m |-> NoMan, t |-> NoEpoch, h |-> <<>>, n |-> 0, sealed |-> FALSE,
        c |-> NoCache, k |-> 0, keys |-> {}, inc |-> FALSE]

Settled(m) == m.gen = m.e.g

\* The past epochs the manifest retains (restore/3 with epoch: or at:).
PastEpochs == Range(man.ret)

ReadMan ==
    /\ rd.pc = "idle" /\ Live(man) /\ rd.k < Readers
    /\ \E t \in {man.e} \cup PastEpochs :
         rd' = [rd EXCEPT !.pc = "list", !.m = man, !.t = t, !.k = rd.k + 1,
                          !.inc = t = man.e /\ rd.c.on /\ rd.c.e = t]
    /\ UNCHANGED shown

\* The LIST, then the GETs of what it listed (and the snapshot download), as two steps:
\* in between, keys can be deleted (GC) and recreated (a late upload). From the log
\* cache when it holds the target epoch (a refresh of the current one): only the keys
\* past it. Whatever goes wrong with the cache, a full listing follows.
ReadList ==
    /\ rd.pc = "list"
    /\ rd' = [rd EXCEPT !.pc = "get",
                        !.keys = {x.o : x \in {y \in objs : y.e = rd.t /\ (~rd.inc \/ y.o >= rd.c.o)}}]
    /\ UNCHANGED shown

\* logfmt::verify_segments_from over the listed keys, with what they hold now.
RECURSIVE WalkKeys(_, _, _, _, _)
WalkKeys(e, o, h, n, K) ==
    IF o \notin K THEN [ok |-> ~\E k \in K : k > o, h |-> h, sealed |-> FALSE, n |-> n]
    ELSE IF ~Taken(e, o) THEN [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]
    ELSE LET x == The(At(e, o)) IN
         IF x.kind = "seal" THEN [ok |-> ~\E k \in K : k > o, h |-> h, sealed |-> TRUE, n |-> n]
         ELSE IF Extends1(x.hist, h) THEN WalkKeys(e, o + 1, x.hist, n + 1, K)
         ELSE [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]

ReadGet ==
    /\ rd.pc = "get"
    /\ LET cur == rd.t = rd.m.e
           W == IF rd.inc THEN WalkKeys(rd.t, rd.c.o, rd.c.h, rd.c.o - 1, rd.keys)
                ELSE IF SnapOf(rd.t) = {} THEN [ok |-> FALSE, h |-> <<>>, sealed |-> FALSE, n |-> 0]
                ELSE WalkKeys(rd.t, 1, The(SnapOf(rd.t)).hist, 0, rd.keys)
           \* a past epoch was closed by whoever replaced it
           usable == ~cur \/ Patch("ReadTransitional") \/ Settled(rd.m) \/ W.sealed
       IN rd' = IF W.ok /\ usable
                THEN [rd EXCEPT !.pc = "check", !.h = W.h, !.n = W.n, !.sealed = W.sealed]
                ELSE IF rd.inc /\ ~W.ok THEN [rd EXCEPT !.pc = "list", !.inc = FALSE]
                ELSE [rd EXCEPT !.pc = "idle"]
    /\ UNCHANGED shown

\* restore::still_retained: the re-read manifest still names the epoch read (the current
\* one or a retained past one). GC deletes only epochs the manifest it publishes no longer
\* names, and what a manifest names only moves forward, so an epoch still named was never
\* collected; its log only grows up to its seal (a checkpoint's, or a takeover's at the
\* end it listed), and what was listed is a prefix of it. Once collected, late uploads
\* (even a stale writer's seal) can recreate keys there. The variants are the earlier rules
\* ("RecheckEpoch": the same generation and epoch, sound but a restore must then finish
\* between two checkpoints) and negative controls ("NoRecheck": none; "RecheckGeneration":
\* the generation only; "RecheckSeal": the same generation and epoch, or a log that ended
\* at its seal; "RecheckPastAlways": a past epoch without looking).
RecheckRules == {"NoRecheck", "RecheckGeneration", "RecheckSeal", "RecheckEpoch", "RecheckPastAlways"}
StillCurrent ==
    \/ Patch("NoRecheck")
    \/ Patch("RecheckGeneration") /\ man.gen = rd.m.gen
    \/ Patch("RecheckSeal") /\ man.gen = rd.m.gen /\ (man.e = rd.m.e \/ rd.sealed)
    \/ Patch("RecheckEpoch") /\ man.gen = rd.m.gen /\ man.e = rd.m.e
    \/ Patch("RecheckPastAlways") /\ (rd.t # rd.m.e \/ rd.t \in Retained(man))
    \/ Patches \cap RecheckRules = {} /\ Live(man) /\ rd.t \in Retained(man)

ReadCheck ==
    /\ rd.pc = "check"
    /\ IF StillCurrent
       THEN /\ shown' = shown \cup {rd.h}
            /\ rd' = [rd EXCEPT !.pc = "idle",
                                !.c = IF rd.t = rd.m.e
                                      THEN [on |-> TRUE, e |-> rd.t, o |-> rd.n + 1, h |-> rd.h]
                                      ELSE rd.c]
       ELSE /\ rd' = [rd EXCEPT !.pc = "idle"]
            /\ UNCHANGED shown

ReaderNext == (ReadMan \/ ReadList \/ ReadGet \/ ReadCheck) /\ UNCHANGED <<vars, tomb>>

-----------------------------------------------------------------------------
(* Destroying: destroy.rs *)

\* The database ends: what was acknowledged, committed and shown goes with it, so the
\* properties then catch any history from before it coming back.
EndDb(g) == acked' = {} /\ committed' = {<<>>} /\ shown' = {} /\ tomb' = g

\* Lease::acquire_to_destroy (any clock: a running writer may be taken over, which is
\* what force does), then GET the manifest: the tombstone PUT is conditional on its
\* version (create-only without one). A manifest or tombstone of a newer generation
\* means a newer lease holder: refuse.
DStart(n) ==
    /\ R[n].pc = "off" /\ R[n].nd < Destroys
    /\ leaseGen' = leaseGen + 1
    /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.pc = IF man.present /\ man.gen >= leaseGen + 1
                                                 THEN "off" ELSE "dtomb",
                                        !.gen = leaseGen + 1, !.mver = man.ver,
                                        !.e = IF man.present THEN man.e ELSE R0.e,
                                        !.nopen = R[n].nopen, !.nc = R[n].nc,
                                        !.ncp = R[n].ncp, !.nd = R[n].nd + 1]]
    /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, kills, acked, committed, rvars>>

\* The tombstone PUT. A lost answer is followed by a GET, which finds our tombstone
\* unless it was replaced since; the model purges in both cases (purging dead objects is
\* always allowed). "DestroyDeletes": delete the manifest instead (unconditionally).
DTomb(n) ==
    /\ R[n].pc = "dtomb"
    /\ IF Patch("DestroyDeletes")
       THEN /\ man' = NoMan
            /\ R' = [R EXCEPT ![n].pc = "dpurge"]
            /\ EndDb(R[n].gen)
            /\ UNCHANGED <<inflight, ctr, faults>>
       ELSE LET rec == TombMan(R[n].gen, R[n].e.s, ctr + 1)
                cond == IF R[n].mver = 0 THEN ~man.present ELSE man.ver = R[n].mver
            IN \E out \in Outcomes(cond) :
                 /\ Charge(out)
                 /\ ctr' = ctr + 1
                 /\ man' = IF out \in {"ok", "lost"} THEN rec
                         ELSE IF out = "trunc" THEN EmptyMan ELSE man
                 /\ inflight' = IF out = "late"
                                THEN inflight \cup {[k |-> "man", cond |-> R[n].mver, rec |-> rec]}
                                ELSE inflight
                 /\ IF out \in {"ok", "lost"} THEN EndDb(R[n].gen)
                    ELSE UNCHANGED <<acked, committed, shown, tomb>>
                 /\ R' = IF out \in {"ok", "lost"} THEN [R EXCEPT ![n].pc = "dpurge"]
                         ELSE [R EXCEPT ![n].pc = "off"]
    /\ UNCHANGED <<objs, snaps, leaseGen, kills, rd>>

\* purge: delete the dead objects one at a time, in any order, and stop at any point
\* (the code deletes what one listing found; a kill can stop it). "PurgeAll": ignore the
\* generation.
Purgeable(g, S) == IF Patch("PurgeAll") THEN S ELSE {x \in S : Dead(g, x)}
DPurge(n) ==
    /\ R[n].pc = "dpurge"
    /\ \/ \E x \in Purgeable(R[n].gen, objs) : objs' = objs \ {x} /\ UNCHANGED <<snaps, R>>
       \/ \E x \in Purgeable(R[n].gen, snaps) : snaps' = snaps \ {x} /\ UNCHANGED <<objs, R>>
       \/ R' = [R EXCEPT ![n].pc = "off"] /\ UNCHANGED <<objs, snaps>>
    /\ UNCHANGED <<man, leaseGen, inflight, ctr, faults, kills, acked, committed, rvars>>

DestroyNext == \E n \in Node : DStart(n) \/ DTomb(n) \/ DPurge(n)

-----------------------------------------------------------------------------
(* Environment *)

Kill(n) ==
    /\ R[n].pc # "off" /\ kills < MaxKills
    /\ kills' = kills + 1
    /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.nc = R[n].nc, !.ncp = R[n].ncp,
                                        !.nopen = R[n].nopen, !.nd = R[n].nd]]
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
    \* A destroy's tombstone landing late ends the database then.
    /\ IF w.k = "man" /\ w.rec.tomb /\ man' = w.rec
       THEN EndDb(w.rec.gen)
       ELSE UNCHANGED <<acked, committed, shown, tomb>>
    /\ UNCHANGED <<snaps, leaseGen, R, ctr, faults, kills, rd>>

-----------------------------------------------------------------------------
Init ==
    /\ man = NoMan /\ objs = {} /\ snaps = {} /\ leaseGen = 0 /\ inflight = {}
    /\ R = [n \in Node |-> R0]
    /\ ctr = 0 /\ faults = 0 /\ kills = 0 /\ acked = {} /\ committed = {<<>>}
    /\ rd = RD0 /\ shown = {} /\ tomb = 0

WriterNext ==
    \/ \E n \in Node :
         \/ Open(n) \/ Take(n) \/ RestoreStep(n) \/ TakeSeal(n) \/ LateTake(n) \/ Compact(n)
         \/ CommitPut(n) \/ AsyncCommit(n) \/ Rewrite(n) \/ Confirm(n) \/ ConfirmFails(n)
         \/ Checkpoint(n) \/ PublishBegin(n) \/ Seal(n) \/ Adopt(n) \/ Snap(n) \/ Man(n) \/ GC(n)
         \/ Kill(n)

Next == (WriterNext /\ UNCHANGED rvars) \/ ReaderNext \/ (\E w \in inflight : Land(w))
        \/ DestroyNext

Spec == Init /\ [][Next]_allvars

\* Liveness: each host keeps taking its own steps (a pool reopening a closed
\* connection, the uploader retrying, a publication going on). No fairness for
\* what users or the environment do: commits, checkpoints, kills, late landings.
Progress(n) ==
    /\ \/ Open(n) \/ Take(n) \/ RestoreStep(n) \/ TakeSeal(n) \/ LateTake(n) \/ Compact(n)
       \/ CommitPut(n) \/ Rewrite(n) \/ Confirm(n) \/ ConfirmFails(n)
       \/ PublishBegin(n) \/ Seal(n) \/ Adopt(n) \/ Snap(n) \/ Man(n) \/ GC(n)
    /\ UNCHANGED rvars
LiveSpec == Spec /\ \A n \in Node : WF_allvars(Progress(n))

-----------------------------------------------------------------------------
(* Properties *)

\* Every acknowledged commit is in a restore of the current manifest.
AckedDurable ==
    Live(man) =>
        LET W == Restore(man) IN \A h \in acked : W.ok /\ IsPrefix(h, W.h)

\* The current manifest always restores (no gap, no broken chain, snapshot present).
RestoreOK == Live(man) => Restore(man).ok

\* What a restore finds is a history some host committed: a prefix of the
\* commit order, without holes or reordering (async: possibly behind).
RestoreCommitted ==
    Live(man) => LET W == Restore(man) IN W.ok => W.h \in committed

\* Every history a reader returned stays: it is a prefix of what the current manifest
\* restores (a replica never shows a commit that later disappears).
ShownDurable ==
    Live(man) => LET W == Restore(man) IN W.ok => \A h \in shown : IsPrefix(h, W.h)

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

\* Reader witnesses: a reader returned a history after a takeover; it used a manifest
\* written by a takeover (its epoch sealed); it read a past epoch; a takeover sealed.
ShownAfterTakeover == \E h \in shown : Len(h) >= 1 /\ man.gen >= 2
ReadDuringTakeover == rd.pc = "check" /\ ~Settled(rd.m)
ReadPastEpoch == rd.pc = "check" /\ rd.t # rd.m.e
\* A read accepted although the writer moved to a new epoch meanwhile (td-beh's case).
ReadAcrossCheckpoint == rd.pc = "check" /\ StillCurrent /\ man.e # rd.m.e
TakeoverSealed == \E x \in objs : x.kind = "seal" /\ x.e.g # x.gen

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

\* After a destroy, a database is always one started after it: its epoch's generation is
\* newer than the tombstone's, so nothing of the destroyed one (or older) comes back.
NewAfterDestroy == Live(man) /\ tomb > 0 => man.e.g > tomb

\* Witnesses: a database created over a tombstone, and a commit acknowledged in it.
OpenOverTomb == tomb > 0 /\ Live(man)
AckAfterDestroy == tomb > 0 /\ acked # {}

StateConstraint == ctr <= 12

Perms == Permutations(Node)
=============================================================================
