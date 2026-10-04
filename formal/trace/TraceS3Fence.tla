---- MODULE TraceS3Fence ----
(* Trace validation: is a real writer's request log (converted by s3trace.py into    *)
(* TraceLog's Trace) a behaviour of S3Fence? Each event must be matched by the model  *)
(* action it corresponds to, with the observed outcome; local steps the store doesn't *)
(* see (a checkpoint, the start of a publication, garbage collection) may happen in    *)
(* between. The trace is accepted when TLC reaches its end (TraceDone).               *)
EXTENDS S3Fence, TraceLog

VARIABLE i
tvars == <<allvars, i>>

\* One writer (the traces come from single-writer runs).
N == CHOOSE n \in Node : TRUE

TraceInit == Init /\ i = 0

\* Local steps: a checkpoint, a publication starting, garbage collection (its deletes
\* are observed as "gc" events, but a pass with nothing to delete sends nothing).
\* Adopt reads the manifest, a read the trace doesn't match as an event. Async commits
\* are local (the uploader's PUTs are the frames; a coalesced segment of several
\* commits is one model frame, whose content the trace doesn't check).
\* A write of a killed writer may still land (the model's late landing).
Internal == (Checkpoint(N) \/ PublishBegin(N) \/ Adopt(N) \/ GC(N) \/ AsyncCommit(N)
             \/ \E w \in inflight : Land(w)) /\ i' = i

Is(ev, a) == a /\ i' = i + 1
Pc == R'[N].pc

Matches(ev) ==
    CASE ev.k = "open" -> Open(N)
      [] ev.k = "close" -> Kill(N)
      [] ev.k = "bootstrap" ->
           Take(N) /\ ~man.present /\ ~Refuse /\ (IF ev.ok THEN Pc = "run" ELSE Pc = "off")
      [] ev.k = "refused" -> Take(N) /\ ~man.present /\ Refuse
      [] ev.k = "takeover" ->
           Take(N) /\ man.present /\ (IF ev.ok THEN Pc = "restore" ELSE Pc = "off")
      [] ev.k = "restore" -> RestoreStep(N) /\ Pc \in {"compact", "tseal"}
      \* The takeover's seal; a 412 is our own seal (same) or a key taken since the listing.
      [] ev.k = "tseal" ->
           /\ TakeSeal(N)
           /\ CASE ev.ok -> Pc = "compact"
                [] ev.st = "412" -> Pc = IF ev.same THEN "compact" ELSE "restore"
                [] ev.st = "none" -> TRUE
                [] OTHER -> Pc = "off"
      [] ev.k = "compact" ->
           Compact(N) /\ (IF ev.ok THEN Pc = "run" /\ R'[N].e = ev.e ELSE Pc = "off")
      [] ev.k = "frame" ->
           /\ R[N].e = ev.e /\ R[N].o = ev.o
           /\ CommitPut(N)
           /\ CASE ev.ok -> Pc = "confirm"
                [] ev.st = "412" -> Pc = "confirm" \/ R'[N].poisoned # "no"
                \* sent by a writer killed before the answer: any outcome (landed, lost,
                \* never landed, or still in flight)
                [] ev.st = "none" -> TRUE
                [] OTHER -> Pc = "run"
      [] ev.k = "confirm" ->
           IF ev.ok
           THEN Confirm(N) /\ (ev.same => (Pc = "run" /\ R'[N].poisoned = "no"))
           ELSE ConfirmFails(N)
      [] ev.k = "seal" ->
           /\ R[N].seal.e = ev.e /\ R[N].seal.o = ev.o
           /\ Seal(N) /\ (IF ev.ok THEN Pc = "adopt" ELSE Pc = "run")
      [] ev.k = "snap" -> R[N].e = ev.e /\ Snap(N) /\ (IF ev.ok THEN Pc = "man" ELSE Pc = "run")
      [] ev.k = "man" -> Man(N) /\ (IF ev.ok THEN Pc = "gc" ELSE Pc = "run")
      \* The deletes of a GC pass: the model's GC step is internal (above), and the code
      \* also collects at open, which the model's open doesn't (a documented gap).
      [] ev.k = "gc" -> UNCHANGED vars
      \* Found in S3 at the end of the run (a killed writer's late PUT): the model must
      \* have that object too.
      [] ev.k = "present" -> UNCHANGED vars /\ \E x \in objs : x.e = ev.e /\ x.o = ev.o

Observed == i < Len(Trace) /\ Is(Trace[i + 1], Matches(Trace[i + 1]))

TraceNext == (Observed \/ Internal) /\ UNCHANGED rvars
TraceSpec == TraceInit /\ [][TraceNext]_tvars

\* Violated exactly when the whole trace was matched: TLC's counterexample is then
\* the model behaviour that explains the real run.
TraceNotDone == i < Len(Trace)
====
