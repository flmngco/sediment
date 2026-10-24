---------------------------- MODULE KeyRotation ----------------------------
(***************************************************************************)
(* Key rotation inside one S3 prefix (bead td-9qug),                       *)
(* at the grain of object-store requests, in S3Fence's style.              *)
(*                                                                         *)
(* Every epoch is encrypted with one key, which its manifest record names: *)
(* its snapshot and every log frame. Hosts open a database (lease with any *)
(* clock, manifest takeover with If-Match, key resolution from the         *)
(* manifest taken over, restore, takeover seal, fold into a fresh epoch),  *)
(* commit (create-only frame PUT under the writer's key, manifest-etag     *)
(* ownership confirm), and may be killed at any step. A rekey (S3.rekey/3, *)
(* then a clean close) is an open whose fold re-encrypts and whose epoch   *)
(* advance names the new key, pins every record it keeps and retires the   *)
(* old key; a release_key is an open whose advance drops the records under *)
(* a key that isn't current. Retention trims unpinned records to Retain.   *)
(* An error on an epoch advance PUT is followed by a GET: our manifest is  *)
(* adopted, a foreign one is Fenced (the report names its key, A9), the    *)
(* one we hold makes us resend the same body (A4'), a failed GET is        *)
(* indeterminate, which only a later open resolves (A8). A restore at a    *)
(* time T picks the record whose [started_at, ended_at) covers T, with its *)
(* key. Every store write may succeed, be refused (412, only when its     *)
(* precondition is false), land with its answer lost, not land, or stay in *)
(* flight and land later if its precondition still holds then.            *)
(*                                                                         *)
(* Time is the store's clock (now). Epoch boundaries come from it, as in  *)
(* the code (amendment A5'): the Last-Modified of the seal that closed the *)
(* predecessor, the first snapshot's for epoch 0, the clock of the frames' *)
(* last_modified. The writers' clocks (now + Skew) are read only by the    *)
(* control "WriterClock". Sync durability only, and no checkpoints: every  *)
(* epoch comes from an open, whose epoch advance is the same If-Match      *)
(* manifest PUT a checkpoint's publication uses.                           *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Node,            \* hosts
    Keys,            \* key ids
    K1,              \* the key a new database is created under
    MaxCommits,      \* commit attempts per host, whole behaviour
    MaxOpens,        \* opens per host (plain opens, rekeys, releases)
    MaxRekeys,       \* rekeys, whole behaviour
    MaxReleases,     \* release_key calls, whole behaviour
    MaxFaults,       \* lost, failed or delayed answers, whole behaviour
    MaxKills,        \* host kills, whole behaviour
    MaxTime,         \* clock ticks
    Delays,          \* may a write stay in flight and land later
    Retain,          \* retain_epochs
    Rings,           \* the key rings an open may run with (some may lack a key)
    RekeyTo,         \* the keys a rekey may target (Keys; fewer to keep a config small)
    ReleaseOf,       \* the keys a release_key may name (likewise)
    AllowReuse,      \* rekeys pass allow_reuse: true
    Skew,            \* the writers' clocks minus the store's
    Patches          \* behaviours switched on by name; {} is the design

VARIABLES
    \* ---- the object store
    man,        \* manifest.json
    objs,       \* log objects [e, o, kind ("frame"|"seal"), hist, gen, key, t]
    snaps,      \* snapshots [e, hist, key]
    leaseGen,   \* lease.json's generation (takeover is always possible: any clock)
    inflight,   \* writes whose caller gave up, still able to land
    \* ---- per host
    R,
    \* ---- counters
    ctr,        \* fresh manifest versions
    faults, kills, nrk, nrl,
    \* ---- time
    now,        \* the clock
    hist,       \* ghost: hist[T] is the database as of T (what a restore of the
                \* current manifest returned at the end of tick T)
    \* ---- ghosts
    acked,      \* histories some commit was acknowledged for
    committed,  \* histories some host attempted to commit
    disowned,   \* epochs whose advance must never become current: its open reported it
                \* failed, or Fenced while another epoch was current
    indet,      \* epochs whose open's outcome is unclear (indeterminate, or a crash
                \* while its advance was unanswered), not resolved yet
    settled,    \* epochs of unclear outcomes that a later open resolved: never current
    moved       \* a step since the last tick (two ticks in a row add nothing)

vars == <<man, objs, snaps, leaseGen, inflight, R, ctr, faults, kills, nrk, nrl, now, hist,
          acked, committed, disowned, indet, settled, moved>>
outs == <<disowned, indet, settled>>

-----------------------------------------------------------------------------
NoKey == "none"
NoEpoch == [s |-> 0, g |-> 0]
\* ended_at of the current epoch: later than any time
Inf == MaxTime + 1

\* key: the record of the current epoch's key; st: its started_at; ret: the retained
\* earlier epochs, newest first, each [e, key, st, en, pin] (en: ended_at, the
\* successor's started_at, set by the advance; pin: kept by a rekey until released);
\* retired: retired_keys, every key a rekey rotated away from.
\* released: which retired keys were released since they were retired (a ghost the
\* model keeps in the manifest; the code doesn't store it).
NoMan == [present |-> FALSE, e |-> NoEpoch, gen |-> 0, key |-> NoKey, st |-> 0, ret |-> <<>>,
          retired |-> {}, released |-> {}, ver |-> 0]
NewMan(e, g, k, st, ret, rt, rl) ==
    [present |-> TRUE, e |-> e, gen |-> g, key |-> k, st |-> st, ret |-> ret, retired |-> rt,
     released |-> rl, ver |-> ctr + 1]

NoFrame == [e |-> NoEpoch, o |-> 0, kind |-> "none", hist |-> <<>>, gen |-> 0, key |-> NoKey,
            t |-> 0]
\* An object without its landing time (last_modified): what a GET compares.
Body(x) == [x EXCEPT !.t = 0]

\* op: "open", "rekey" (to tgt) or "release" (of tgt). ring: the keys it holds.
\* pre: the key id of the manifest as read before the lease ("KeyBeforeLease" uses it).
\* ek: the key the epoch's record names (the restore decrypts with it); wk: the key the
\* writer encrypts with. tm: the manifest its takeover wrote. bt: the boundary, the
\* Last-Modified of the seal that closed the restored epoch. sent: its epoch advance
\* (for the GET after an error). res: how its last open ended, and told: the key that
\* report names (a Fenced report names the foreign manifest's key, amendment A9).
\* seen: the unclear outcomes reported before its takeover, which its publication
\* resolves.
R0 == [pc |-> "off", gen |-> 0, op |-> "open", tgt |-> NoKey, ring |-> {}, pre |-> NoKey,
       ek |-> NoKey, wk |-> NoKey, tm |-> NoMan, bt |-> 0, sent |-> NoMan, db |-> <<>>,
       e |-> NoEpoch, o |-> 1, mver |-> 0, poisoned |-> "no", orphans |-> {}, frame |-> NoFrame,
       nc |-> 0, nopen |-> 0, gcBelow |-> 0, gcKeep |-> {}, res |-> "none", told |-> NoKey,
       seen |-> {}]

Patch(p) == p \in Patches

CanFault == faults < MaxFaults

IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a
Extends1(h, prev) == Len(h) = Len(prev) + 1 /\ SubSeq(h, 1, Len(prev)) = prev
Range(sq) == {sq[i] : i \in DOMAIN sq}

At(e, o) == {x \in objs : x.e = e /\ x.o = o}
Taken(e, o) == At(e, o) # {}
The(S) == CHOOSE x \in S : TRUE

SnapOf(e) == {s \in snaps : s.e = e}

\* restore.rs fetch_log_from + logfmt.rs verify_segments over epoch e from offset o on
\* top of h, decrypting with key k, with a cutoff T (Inf: none): the contiguous run of
\* objects that landed by T. A gap before the cutoff, a frame that doesn't decrypt
\* under k, or a broken chain fails it.
RECURSIVE Walk(_, _, _, _, _, _)
Walk(e, o, h, n, k, T) ==
    LET L == {x \in objs : x.e = e /\ x.o >= o} IN
    IF L = {} THEN [ok |-> TRUE, h |-> h, sealed |-> FALSE, n |-> n]
    ELSE LET x == CHOOSE y \in L : \A z \in L : y.o <= z.o IN
         IF x.t > T THEN [ok |-> TRUE, h |-> h, sealed |-> FALSE, n |-> n]
         ELSE IF x.o # o THEN [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]
         ELSE IF x.kind = "seal"
         THEN [ok |-> ~\E y \in L : y.o > o, h |-> h, sealed |-> TRUE, n |-> n]
         ELSE IF x.key = k /\ Extends1(x.hist, h) THEN Walk(e, o + 1, x.hist, n + 1, k, T)
         ELSE [ok |-> FALSE, h |-> h, sealed |-> FALSE, n |-> n]

Fail == [ok |-> FALSE, h |-> <<>>, sealed |-> FALSE, n |-> 0]
\* A restore of epoch e with key k: its snapshot must decrypt under k too.
RestoreAs(e, k, T) ==
    IF SnapOf(e) = {} THEN Fail
    ELSE LET s == The(SnapOf(e)) IN
         IF s.key # k THEN Fail ELSE Walk(e, 1, s.hist, 0, k, T)

\* A restore of the current state, with the key the manifest names.
Restore(m) == RestoreAs(m.e, m.key, Inf)

Live(m) == m.present

\* Every epoch a restore may use, as records.
Cur(m) == [e |-> m.e, key |-> m.key, st |-> m.st, en |-> Inf, pin |-> FALSE]
Recs(m) == {Cur(m)} \cup Range(m.ret)
Retained(m) == {x.e : x \in Recs(m)}

\* key_census: the current key and the keys of the retained epochs.
Census(m) == {x.key : x \in Recs(m)} \ {NoKey}

-----------------------------------------------------------------------------
(* Store writes with faults. A write's outcome: "ok" (landed, answered),   *)
(* "lost" (landed, answer lost), "fail" (never lands), "late" (in flight). *)

Outcomes(cond) ==
    IF cond
    THEN {"ok"} \cup (IF CanFault THEN {"lost", "fail"} \cup (IF Delays THEN {"late"} ELSE {})
                                  ELSE {})
    ELSE {"refused"} \cup (IF CanFault THEN {"fail"} \cup (IF Delays THEN {"late"} ELSE {})
                                       ELSE {})

Charge(out) == faults' = IF out \in {"lost", "fail", "late"} THEN faults + 1 ELSE faults

\* Manifest::advance from m (the manifest the writer's takeover wrote) to an epoch
\* starting at t: m's epoch joins the history, ended at t. A rekey's advance pins every
\* record (the epoch it ends and everything retained then); otherwise retention keeps
\* the pinned records and the newest Retain unpinned ones ("TrimPinned": the design
\* before amendment A1, retention trimming every record of the current key and the
\* key a rekey replaces).
Advance(m, rekey, nk, t) ==
    LET all == <<[Cur(m) EXCEPT !.en = t]>> \o m.ret
        Pin(x) == [x EXCEPT !.pin = TRUE]
        Rank(x, P(_)) == Cardinality({y \in Range(all) : P(y) /\ y.e.s >= x.e.s})
        Unpinned(y) == ~y.pin
        Keep(x) == IF Patch("TrimPinned")
                   THEN x.key \notin {nk, m.key} \/ Rank(x, LAMBDA y : y.key = x.key) <= Retain
                   ELSE x.pin \/ Rank(x, Unpinned) <= Retain
    IN IF rekey /\ ~Patch("TrimPinned")
       THEN [i \in DOMAIN all |-> Pin(all[i])]
       ELSE SelectSeq(all, Keep)

-----------------------------------------------------------------------------
(* Opening: lease, takeover, key resolution, restore, fold *)

\* Lease::acquire. Any clock: the previous owner may be judged expired. The open picks
\* its ring and what it is: a plain open, a rekey to a key in its ring (and RekeyTo), or
\* a release of a key in ReleaseOf. "KeyBeforeLease": it reads the manifest's key id
\* now, before the lease.
Open(n) ==
    /\ R[n].pc = "off" /\ R[n].nopen < MaxOpens
    /\ leaseGen' = leaseGen + 1
    /\ \E op \in {"open", "rekey", "release"}, ring \in Rings, tgt \in Keys \cup {NoKey} :
         /\ op = "open" => tgt = NoKey
         /\ op = "rekey" => nrk < MaxRekeys /\ tgt \in ring \cap RekeyTo
         /\ op = "release" => nrl < MaxReleases /\ tgt \in ReleaseOf
         /\ nrk' = IF op = "rekey" THEN nrk + 1 ELSE nrk
         /\ nrl' = IF op = "release" THEN nrl + 1 ELSE nrl
         /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.pc = "take", !.gen = leaseGen + 1, !.op = op,
                                             !.tgt = tgt, !.ring = ring,
                                             !.pre = IF Patch("KeyBeforeLease") THEN man.key ELSE NoKey,
                                             !.nopen = R[n].nopen + 1, !.nc = R[n].nc]]
    /\ UNCHANGED <<man, objs, snaps, inflight, ctr, faults, kills, now, hist, acked, committed>>

\* With no manifest nothing was ever acknowledged: only epoch-0 snapshots, which only a
\* bootstrap writes before its manifest, may be there.
Leftovers == {x \in snaps : x.e.s = 0}
Refuse == objs # {} \/ snaps \ Leftovers # {}

\* A rekey may target a retired key only with allow_reuse, and only once no pinned record
\* under it remains ("NoRetiredCheck": any key; "ReuseWithPins": allow_reuse ignores the
\* pinned records).
ReuseOK(n) ==
    \/ R[n].tgt \notin man.retired
    \/ Patch("NoRetiredCheck")
    \/ AllowReuse /\ (Patch("ReuseWithPins") \/ ~\E x \in Range(man.ret) : x.pin /\ x.key = R[n].tgt)

\* The writer's clock, used for nothing but "WriterClock" (the design before amendment
\* A5: started_at from the writer's clock); otherwise the store's time t.
WriterOr(n, t) == IF Patch("WriterClock") THEN now + Skew ELSE t

\* A manifest written with a generation at least ours means a newer lease holder.
Newer(n) == man.present /\ man.gen >= R[n].gen

\* An open ends: only what outlives it is kept.
Off(n, why) == [R EXCEPT ![n] = [R0 EXCEPT !.nc = R[n].nc, !.nopen = R[n].nopen, !.op = R[n].op,
                                           !.tgt = R[n].tgt, !.res = why, !.told = R[n].told]]

\* GET manifest; resolve its key id from the ring (refuse by name before writing anything:
\* "unknown key id", which key_for turns into a retry, i.e. a new open); a release of
\* the current key refuses, and so does a rekey to a retired key ("NoRetiredCheck": it
\* doesn't); then PUT the takeover (If-Match), which keeps the epoch, its key, the
\* history and the retired keys. A rekey and a release need a database.
Take(n) ==
    /\ R[n].pc = "take"
    /\ IF Newer(n)
       THEN /\ R' = Off(n, "stale")
            /\ UNCHANGED <<man, snaps, inflight, ctr, faults>>
       ELSE IF ~man.present
       THEN IF R[n].op # "open" \/ K1 \notin R[n].ring \/ Refuse
            THEN /\ R' = Off(n, "refused")
                 /\ UNCHANGED <<man, snaps, inflight, ctr, faults>>
            ELSE \* bootstrap: snapshot of the empty database under K1, create-only manifest
                 LET e == [s |-> 0, g |-> R[n].gen]
                     rec == NewMan(e, R[n].gen, K1, WriterOr(n, now), <<>>, {}, {})
                 IN
                 /\ snaps' = snaps \cup {[e |-> e, hist |-> <<>>, key |-> K1]}
                 /\ \E out \in Outcomes(TRUE) :
                      /\ Charge(out)
                      /\ ctr' = ctr + 1
                      /\ man' = IF out \in {"ok", "lost"} THEN rec ELSE man
                      /\ inflight' = IF out = "late"
                                     THEN inflight \cup {[k |-> "man", cond |-> 0, any |-> FALSE,
                                                          rec |-> rec]}
                                     ELSE inflight
                      /\ R' = IF out = "ok"
                              THEN [R EXCEPT ![n].pc = "run", ![n].e = e, ![n].o = 1,
                                             ![n].mver = ctr + 1, ![n].db = <<>>, ![n].ek = K1,
                                             ![n].wk = K1, ![n].tm = rec, ![n].res = "ok"]
                              ELSE Off(n, "failed")
       ELSE LET wk == IF Patch("KeyBeforeLease") THEN R[n].pre ELSE man.key
                rec == NewMan(man.e, R[n].gen, man.key, man.st, man.ret, man.retired, man.released)
            IN
            IF man.key \notin R[n].ring \/ wk \notin R[n].ring
            THEN /\ R' = Off(n, "key_needed")
                 /\ UNCHANGED <<man, snaps, inflight, ctr, faults>>
            ELSE IF R[n].op = "release" /\ R[n].tgt = man.key /\ ~Patch("ReleaseCurrent")
            THEN /\ R' = Off(n, "refused")
                 /\ UNCHANGED <<man, snaps, inflight, ctr, faults>>
            ELSE IF R[n].op = "rekey" /\ ~ReuseOK(n)
            THEN /\ R' = Off(n, "retired")
                 /\ UNCHANGED <<man, snaps, inflight, ctr, faults>>
            ELSE /\ \E out \in Outcomes(TRUE) :
                      /\ Charge(out)
                      /\ ctr' = ctr + 1
                      /\ man' = IF out \in {"ok", "lost"} THEN rec ELSE man
                      /\ inflight' = IF out = "late"
                                     THEN inflight \cup {[k |-> "man", cond |-> man.ver,
                                                          any |-> FALSE, rec |-> rec]}
                                     ELSE inflight
                      /\ R' = IF out = "ok"
                              THEN [R EXCEPT ![n].pc = "restore", ![n].e = man.e, ![n].seen = indet,
                                             ![n].mver = ctr + 1, ![n].ek = man.key,
                                             ![n].wk = wk, ![n].tm = rec]
                              ELSE Off(n, "failed")
                 /\ UNCHANGED snaps
    /\ UNCHANGED <<objs, leaseGen, kills, nrk, nrl, now, hist, acked, committed>>

\* The Last-Modified of an epoch's seal (a HEAD).
SealTime(e) == The({x \in objs : x.e = e /\ x.kind = "seal"}).t

\* restore::restore with the key the epoch's record names: list and verify the epoch;
\* an unsealed epoch is sealed at the end just listed first.
RestoreStep(n) ==
    /\ R[n].pc = "restore"
    /\ LET W == RestoreAs(R[n].e, R[n].ek, Inf) IN
       R' = IF ~W.ok THEN Off(n, "failed")
            ELSE IF ~W.sealed
            THEN [R EXCEPT ![n].pc = "tseal", ![n].db = W.h, ![n].o = W.n + 1]
            ELSE [R EXCEPT ![n].pc = "compact", ![n].db = W.h, ![n].bt = SealTime(R[n].e)]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, nrk, nrl, now, hist,
                   acked, committed>>

\* The takeover's seal: create-only at the end of the log it listed. Taken (a late upload
\* of the old writer landed since the listing): list again.
TakeSeal(n) ==
    /\ R[n].pc = "tseal"
    /\ LET s == [e |-> R[n].e, o |-> R[n].o, kind |-> "seal", hist |-> <<>>, gen |-> R[n].gen,
                 key |-> NoKey, t |-> now]
           free == ~Taken(s.e, s.o)
       IN \E out \in Outcomes(free) :
            /\ Charge(out)
            /\ objs' = IF out \in {"ok", "lost"} THEN objs \cup {s} ELSE objs
            /\ inflight' = IF out = "late"
                           THEN inflight \cup {[k |-> "obj", cond |-> 0, any |-> FALSE, rec |-> s]}
                           ELSE inflight
            /\ R' = CASE out = "ok" -> [R EXCEPT ![n].pc = "compact", ![n].bt = now]
                      [] out = "refused" ->
                           IF Body(The(At(s.e, s.o))) = Body(s)
                           THEN [R EXCEPT ![n].pc = "compact", ![n].bt = The(At(s.e, s.o)).t]
                           ELSE [R EXCEPT ![n].pc = "restore"]
                      [] OTHER -> Off(n, "failed")
    /\ UNCHANGED <<man, snaps, leaseGen, ctr, kills, nrk, nrl, now, hist, acked, committed>>

\* The fold into a fresh epoch, published with If-Match on the takeover, starting at
\* the boundary (clamped to be monotonic). A plain open writes under the key it
\* resolved, and its record carries the manifest's key (as Manifest::advance carries
\* the database's other fields); a rekey re-encrypts (page by page, checked) and its
\* record names the new key, the old one retired; a release drops the records under
\* its key. "NoIfMatchSwitch": a rekey publishes unconditionally. "ReleaseCurrent": a
\* release of the current key drops it from the record too.
Max2(a, b) == IF a >= b THEN a ELSE b
Advanced(n) ==
    LET op == R[n].op
        tgt == R[n].tgt
        m == R[n].tm
        rk == IF op = "rekey" THEN tgt
              ELSE IF op = "release" /\ tgt = R[n].ek THEN NoKey
              ELSE R[n].ek
        rekey == op = "rekey" /\ tgt # m.key
        st == WriterOr(n, Max2(R[n].bt, m.st))
        base == Advance(m, rekey, rk, st)
        ret == IF op = "release" THEN SelectSeq(base, LAMBDA x : x.key # tgt) ELSE base
        \* a reuse (allow_reuse) takes the key off the retired list
        reused == IF AllowReuse THEN {tgt} ELSE {}
        rt == IF rekey THEN (m.retired \ reused) \cup {m.key} ELSE m.retired
        rl == IF op = "release" THEN m.released \cup {tgt}
              ELSE IF rekey THEN m.released \ ({m.key} \cup reused)
              ELSE m.released
    IN NewMan([s |-> R[n].e.s + 1, g |-> R[n].gen], R[n].gen, rk, st, ret, rt, rl)
\* The data key of the new epoch.
NewKey(n) == IF R[n].op = "rekey" THEN R[n].tgt ELSE R[n].wk

\* Published (also by adopting it after an error).
Published(n, rec) ==
    [R EXCEPT ![n].pc = "gc", ![n].e = rec.e, ![n].o = 1, ![n].mver = rec.ver,
              ![n].wk = NewKey(n), ![n].ek = rec.key, ![n].gcBelow = rec.e.s,
              ![n].gcKeep = Retained(rec), ![n].sent = NoMan, ![n].tm = NoMan, ![n].bt = 0,
              ![n].told = rec.key, ![n].seen = {},
              ![n].res = IF R[n].op = "rekey" /\ R[n].tgt \in R[n].tm.retired THEN "reused" ELSE "ok"]
\* A publication resolves every unclear outcome reported before its takeover: that
\* takeover moved the manifest version, so no advance PUT sent before it can land.
Resolve(n) ==
    /\ settled' = settled \cup R[n].seen
    /\ indet' = indet \ R[n].seen
    /\ UNCHANGED disowned
\* An unclear outcome. "CensusResolves": the caller takes a key_census read at the report
\* for the outcome (the guidance before amendment A8), i.e. the advance as not landed
\* when the census shows another epoch.
Unclear(e) ==
    /\ indet' = indet \cup {e}
    /\ settled' = IF Patch("CensusResolves") /\ man.e # e THEN settled \cup {e} ELSE settled
\* The open ends without its advance published. "failed" says nothing switched; Fenced
\* names the key of the foreign manifest it read (A9), which may already carry our epoch
\* (ours landed with its answer lost and another writer took over), so only another
\* epoch there says ours never will ("FencedNoKey": Fenced says nothing switched, the
\* reading before A9); "indeterminate" is unclear.
Disown(n, why, rec) ==
    /\ R' = [Off(n, why) EXCEPT ![n].told = IF why = "fenced" /\ ~Patch("FencedNoKey")
                                           THEN man.key ELSE NoKey]
    /\ disowned' = IF why = "failed" \/ (why = "fenced" /\ (Patch("FencedNoKey") \/ man.e # rec.e))
                  THEN disowned \cup {rec.e} ELSE disowned
    /\ IF why = "indeterminate" THEN Unclear(rec.e) ELSE UNCHANGED <<indet, settled>>

\* PUT the epoch advance rec (If-Match on what we hold). On an error or a 412, GET it
\* ("NoAdopt": the design before amendment A4, the open fails).
PutAdvance(n, rec) ==
    LET any == R[n].op = "rekey" /\ Patch("NoIfMatchSwitch") IN
    \E out \in Outcomes(any \/ man.ver = R[n].mver) :
       /\ Charge(out)
       /\ man' = IF out \in {"ok", "lost"} THEN rec ELSE man
       /\ inflight' = IF out = "late"
                      THEN inflight \cup {[k |-> "man", cond |-> R[n].mver, any |-> any, rec |-> rec]}
                      ELSE inflight
       /\ IF out = "ok"
          THEN R' = Published(n, rec) /\ Resolve(n)
          ELSE IF Patch("NoAdopt")
          THEN Disown(n, IF out = "refused" THEN "fenced" ELSE "failed", rec)
          ELSE R' = [R EXCEPT ![n].pc = "verify", ![n].sent = rec] /\ UNCHANGED outs

Compact(n) ==
    /\ R[n].pc = "compact"
    /\ LET rec == Advanced(n) IN
       /\ snaps' = snaps \cup {[e |-> rec.e, hist |-> R[n].db, key |-> NewKey(n)]}
       /\ ctr' = ctr + 1
       /\ PutAdvance(n, rec)
    /\ UNCHANGED <<objs, leaseGen, kills, nrk, nrl, now, hist, acked, committed>>

\* Resend the same body after the GET found the manifest we hold.
Resend(n) ==
    /\ R[n].pc = "resend"
    /\ PutAdvance(n, R[n].sent)
    /\ UNCHANGED <<objs, snaps, leaseGen, ctr, kills, nrk, nrl, now, hist, acked, committed>>

\* The GET after an error: our epoch advance is there (adopt it), or the manifest we hold
\* (the PUT hasn't landed, and may still: resend it; "FailUnlanded": report failure, which
\* amendment A4 leaves open), or a foreign one (Fenced: ours can no longer land), or the
\* GET fails (the outcome is unknown: indeterminate).
Verify(n) ==
    /\ R[n].pc = "verify"
    /\ \/ /\ CanFault /\ faults' = faults + 1
          /\ Disown(n, "indeterminate", R[n].sent)
       \/ /\ faults' = faults
          /\ IF man = R[n].sent
             THEN R' = [Published(n, R[n].sent) EXCEPT ![n].res = "adopted"] /\ Resolve(n)
             ELSE IF man.ver = R[n].mver
             THEN IF Patch("FailUnlanded") THEN Disown(n, "failed", R[n].sent)
                  ELSE R' = [R EXCEPT ![n].pc = "resend"] /\ UNCHANGED outs
             ELSE Disown(n, "fenced", R[n].sent)
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, kills, nrk, nrl, now, hist, acked,
                   committed>>

\* collect_garbage after the publication: epochs older than ours that the manifest we
\* published doesn't name, never one of a newer lease generation. A rekey or a release
\* then closes cleanly; an open runs.
GC(n) ==
    /\ R[n].pc = "gc"
    /\ LET Keep(x) == x.e.s >= R[n].gcBelow \/ x.e \in R[n].gcKeep \/ x.e.g > R[n].gen IN
       /\ objs' = {x \in objs : Keep(x)}
       /\ snaps' = {s \in snaps : Keep(s)}
    /\ R' = IF R[n].op = "open" THEN [R EXCEPT ![n].pc = "run", ![n].gcBelow = 0, ![n].gcKeep = {}]
            ELSE Off(n, R[n].res)
    /\ UNCHANGED <<man, leaseGen, inflight, ctr, faults, kills, nrk, nrl, now, hist, acked,
                   committed>>

-----------------------------------------------------------------------------
(* Committing: S3DurableStorage::upload_frame (sync) *)

PoisonWhy(n, why) == [R EXCEPT ![n].poisoned = why, ![n].pc = "run"]
Running(n) == R[n].pc = "run" /\ R[n].poisoned = "no"

\* A create-only PUT of the frame, encrypted under the writer's key, then (on any error)
\* a GET of the key.
CommitPut(n) ==
    /\ Running(n) /\ R[n].nc < MaxCommits
    /\ LET f == [e |-> R[n].e, o |-> R[n].o, kind |-> "frame",
                 hist |-> Append(R[n].db, <<n, R[n].nc + 1>>), gen |-> R[n].gen,
                 key |-> R[n].wk, t |-> now]
           free == ~Taken(f.e, f.o)
       IN \E out \in Outcomes(free) :
            /\ Charge(out)
            /\ objs' = IF out \in {"ok", "lost"} THEN objs \cup {f} ELSE objs
            /\ inflight' = IF out = "late"
                           THEN inflight \cup {[k |-> "obj", cond |-> 0, any |-> FALSE, rec |-> f]}
                           ELSE inflight
            /\ committed' = committed \cup {f.hist}
            /\ LET R1 == [R EXCEPT ![n].nc = R[n].nc + 1] IN
               R' = CASE out \in {"ok", "lost"} ->
                           [R1 EXCEPT ![n].pc = "confirm", ![n].frame = f]
                      [] out = "refused" ->
                           \* 412: the key holds something; ours only if identical
                           IF Body(The(At(f.e, f.o))) = Body(f)
                           THEN [R1 EXCEPT ![n].pc = "confirm", ![n].frame = f]
                           ELSE [R1 EXCEPT ![n].poisoned =
                                       IF Body(The(At(f.e, f.o))) \in R[n].orphans
                                       THEN "indeterminate" ELSE "fenced"]
                      [] OTHER -> \* failed: the commit rolls back
                           [R1 EXCEPT ![n].orphans = R[n].orphans \cup {Body(f)}]
    /\ UNCHANGED <<man, snaps, leaseGen, ctr, kills, nrk, nrl, now, hist, acked>>

\* confirm_ownership: HEAD manifest; ack only if it is still the one we wrote.
Confirm(n) ==
    /\ R[n].pc = "confirm"
    /\ IF man.ver = R[n].mver
       THEN /\ acked' = acked \cup {R[n].frame.hist}
            /\ R' = [R EXCEPT ![n].pc = "run", ![n].db = R[n].frame.hist, ![n].o = R[n].o + 1,
                              ![n].frame = NoFrame]
       ELSE /\ R' = PoisonWhy(n, "fenced")
            /\ UNCHANGED acked
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, nrk, nrl, now, hist,
                   committed>>

\* The HEAD itself fails: the frame landed but the commit reports failure.
ConfirmFails(n) ==
    /\ R[n].pc = "confirm" /\ CanFault
    /\ faults' = faults + 1
    /\ R' = [R EXCEPT ![n].pc = "run", ![n].orphans = R[n].orphans \cup {Body(R[n].frame)},
                      ![n].frame = NoFrame]
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, kills, nrk, nrl, now, hist, acked,
                   committed>>

\* A clean close (nothing pending), or a poisoned writer's connection closed so the
\* pool reopens it.
Close(n) ==
    /\ R[n].pc = "run"
    /\ R' = Off(n, R[n].res)
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, kills, nrk, nrl, now, hist,
                   acked, committed>>

-----------------------------------------------------------------------------
(* Environment *)

\* A crash while the advance is unanswered (between its PUT and a clear answer) leaves
\* an unclear outcome: the caller gets no report.
Kill(n) ==
    /\ R[n].pc # "off" /\ kills < MaxKills
    /\ kills' = kills + 1
    /\ R' = [R EXCEPT ![n] = [R0 EXCEPT !.nc = R[n].nc, !.nopen = R[n].nopen, !.res = "killed"]]
    /\ IF R[n].pc \in {"verify", "resend"} THEN Unclear(R[n].sent.e) ELSE UNCHANGED <<indet, settled>>
    /\ UNCHANGED disowned
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, ctr, faults, nrk, nrl, now, hist, acked,
                   committed>>

\* A write the caller gave up on reaches the store; its precondition decides. A log
\* object's last_modified is when it lands.
Land(w) ==
    /\ inflight' = inflight \ {w}
    /\ IF w.k = "obj"
       THEN /\ objs' = IF Taken(w.rec.e, w.rec.o) THEN objs ELSE objs \cup {[w.rec EXCEPT !.t = now]}
            /\ man' = man
       ELSE /\ man' = IF w.any \/ (w.cond = 0 /\ ~man.present)
                         \/ (w.cond # 0 /\ man.ver = w.cond)
                      THEN w.rec ELSE man
            /\ objs' = objs
    /\ UNCHANGED <<snaps, leaseGen, R, ctr, faults, kills, nrk, nrl, now, hist, acked, committed>>

\* The clock moves on; the database as of the tick that ends is recorded.
Canon == IF Live(man) /\ Restore(man).ok THEN Restore(man).h ELSE <<>>
Tick ==
    /\ now < MaxTime
    /\ hist' = Append(hist, Canon)
    /\ now' = now + 1
    /\ UNCHANGED <<man, objs, snaps, leaseGen, inflight, R, ctr, faults, kills, nrk, nrl, acked,
                   committed>>

-----------------------------------------------------------------------------
Init ==
    /\ man = NoMan /\ objs = {} /\ snaps = {} /\ leaseGen = 0 /\ inflight = {}
    /\ R = [n \in Node |-> R0]
    /\ ctr = 0 /\ faults = 0 /\ kills = 0 /\ nrk = 0 /\ nrl = 0
    /\ now = 1 /\ hist = <<>>
    /\ acked = {} /\ committed = {<<>>} /\ disowned = {} /\ indet = {} /\ settled = {}
    /\ moved = TRUE

Next ==
    \/ /\ \/ \E n \in Node : Compact(n) \/ Resend(n) \/ Verify(n) \/ Kill(n)
          \/ /\ \/ \E n \in Node :
                     \/ Open(n) \/ Take(n) \/ RestoreStep(n) \/ TakeSeal(n) \/ GC(n)
                     \/ CommitPut(n) \/ Confirm(n) \/ ConfirmFails(n) \/ Close(n)
                \/ \E w \in inflight : Land(w)
             /\ UNCHANGED outs
       /\ moved' = TRUE
    \/ Tick /\ moved /\ moved' = FALSE /\ UNCHANGED outs

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Point-in-time restore at T (restore::choose with Target::Time, then the epoch's   *)
(* log up to T), with every key in the ring: the retained record whose              *)
(* [started_at, ended_at) covers T, or a refusal ("NoCoverageCheck": the newest     *)
(* retained epoch that started by T, the rule before key rotation).                 *)

PitrPick(T) ==
    {x \in Recs(man) : x.st <= T /\ (Patch("NoCoverageCheck") \/ T < x.en)}
Newest(S) == CHOOSE x \in S : \A y \in S : y.e.s <= x.e.s
Pitr(T) ==
    IF ~Live(man) \/ PitrPick(T) = {} THEN Fail
    ELSE LET x == Newest(PitrPick(T)) IN RestoreAs(x.e, x.key, T)

-----------------------------------------------------------------------------
(* Properties *)

\* Every acknowledged commit is in a restore of the current manifest (with its key).
AckedDurable ==
    Live(man) => LET W == Restore(man) IN \A h \in acked : W.ok /\ IsPrefix(h, W.h)

\* Every epoch the manifest names (current and retained) restores with the key its record
\* names: the snapshot and every frame are encrypted with that key.
KeyConsistent == Live(man) => \A x \in Recs(man) : RestoreAs(x.e, x.key, Inf).ok

\* A restore at T returns the database as of T, or refuses.
PitrExact == \A T \in 1..(now - 1) : Pitr(T).ok => Pitr(T).h = hist[T]

\* Every key the data of the current or a retained epoch is encrypted with is in the
\* census (so a key the census doesn't list can leave the ring).
DataKeys(e) == {s.key : s \in SnapOf(e)} \cup {x.key : x \in {y \in objs : y.e = e /\ y.kind = "frame"}}
CensusSound == Live(man) => \A e \in Retained(man) : DataKeys(e) \subseteq Census(man)

\* What an open reports about its epoch advance stays true: one reported failed never
\* becomes current, and one reported Fenced while the foreign manifest named another
\* epoch never does either (our PUT can no longer land), so the key a Fenced report names
\* changes only by a later operation (amendments A4, A9). Keyed on the epoch id, unique
\* per open: a takeover of our landed epoch is a new version of the same epoch.
OutcomeFinal == Live(man) => man.e \notin disowned

\* An unclear outcome (indeterminate, or a crash before a clear answer) is resolved only
\* by a later open's publication (a re-run, amendment A8): after it, the unclear advance
\* never becomes current. A census read alone resolves nothing ("CensusResolves").
IndetCensus == Live(man) => man.e \notin settled

\* A key rotated away from is never current again, unless a rekey reused it with
\* allow_reuse (amendment A3).
RetiredStaysRetired == Live(man) => man.key \notin man.retired

\* A rekey keeps its source: every retired key not released since has a pinned record
\* (amendment A1: whatever Retain is).
SourceKept ==
    Live(man) => \A k \in man.retired \ man.released : \E x \in Range(man.ret) : x.pin /\ x.key = k

\* The history is bounded (amendments A1, A7): at most Retain unpinned records, no
\* pinned record under the current key, and one pinned run, of at most Retain + 1
\* records, per key.
HistoryBound ==
    Live(man) =>
        /\ Cardinality({x \in Range(man.ret) : ~x.pin}) <= Retain
        /\ \A x \in Range(man.ret) : x.pin => x.key # man.key
        /\ \A k \in Keys : Cardinality({x \in Range(man.ret) : x.pin /\ x.key = k}) <= Retain + 1

\* What a restore finds is a history some host committed.
RestoreCommitted == Live(man) => LET W == Restore(man) IN W.ok => W.h \in committed

\* Action property: the manifest never moves to an older epoch or version.
ManifestForward ==
    [][man.present => (man'.present /\ man.e.s <= man'.e.s /\ man.ver <= man'.ver)]_man

\* Situations each positive config must reach (_POSSIBLE).
AckAfterRekey == \E x \in objs : x.kind = "frame" /\ x.key # K1 /\ x.hist \in acked
AckAfterTakeover == \E h \in acked : \E n \in Node : R[n].gen >= 2 /\ Len(h) >= 2
OldKeyRetained == \E x \in Range(man.ret) : x.key # man.key
\* A restore at T used an epoch under an old key.
PitrOldKey == \E T \in 1..(now - 1) : PitrPick(T) # {} /\ Newest(PitrPick(T)).key # man.key
                                      /\ Pitr(T).ok
\* Some T is not covered although an older retained epoch started before it.
PitrGap == \E T \in 1..(now - 1) : PitrPick(T) = {} /\ \E x \in Recs(man) : x.st <= T
Released == Live(man) /\ man.released # {}
RekeyFenced == \E n \in Node : R[n].op = "rekey" /\ R[n].res = "fenced"
KeyNeeded == \E n \in Node : R[n].res = "key_needed"
ReuseRefused == \E n \in Node : R[n].res = "retired"
Reused == \E n \in Node : R[n].res = "reused"
Adopted == \E n \in Node : R[n].res = "adopted"
Resent == \E n \in Node : R[n].pc = "resend"
\* An unclear outcome was resolved by a later open.
IndetResolved == settled # {}
\* A rekey reported Fenced although its switch had landed (another writer took over our
\* epoch): the report names the new key.
FencedSwitched == \E n \in Node : R[n].op = "rekey" /\ R[n].res = "fenced" /\ R[n].told = R[n].tgt
LateLanding == inflight # {}

StateConstraint == ctr <= 12

\* For configs (which take no negative numbers): writers' clocks behind the store's.
SkewBehind == -1

Perms == Permutations(Node)
=============================================================================
