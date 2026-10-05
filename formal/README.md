# formal/

A TLA+ model of the S3 durability ownership path (`native/sediment_nif/src/s3`),
in the style of sqlite_replica's `ReplicaFence`.

`tla/S3Fence.tla` models, at the grain of object-store requests: open (lease with any
clock, manifest takeover, restore, a seal of the old epoch at the restored end, fold into a
fresh epoch), commit (create-only frame PUT,
GET on error, manifest-etag ownership confirm), checkpoint and publication (seal, snapshot,
manifest `If-Match`, GC keeping each old epoch's tail), and kills anywhere. Every write may
succeed, be refused (412, only when its precondition is false), land with its answer lost,
not land, or stay in flight and land later if its precondition still holds then. Lease
checks never protect a stale host, so safety rests on the store alone.

Properties:

- `AckedDurable`: every acknowledged commit is in a restore of the current manifest.
- `RestoreOK`: the current manifest always restores (no gap, no broken chain, snapshot present).
- `SoleNeverFenced`: a single host never takes itself for another writer, whatever the
  faults. It may still stop as *indeterminate*: a commit that reported failure reached S3.
- `RestoreCommitted`: what a restore finds is a history some host committed, i.e. a prefix
  of the commit order without holes or reordering.
- `ShownDurable` (with `Readers`): every history a reader (a replica refresh or a restore)
  returned stays a prefix of what the current manifest restores, so a replica never shows a
  commit that later disappears. See "Readers" below.
- `ManifestForward` (an action property): the manifest never moves to an older epoch or
  version, so the database in S3 never rolls back. A provider ignoring `If-Match` on a late
  manifest PUT breaks it (`NegIgnoreIfMatch`, 4,689 states; `RestoreOK` catches the same
  provider later). Not added, because they hold by construction (every update
  is a union or an append, so no negative control could break them without making the
  model write something the code can't): `acked` and `committed` only grow, and a host's
  durable history only extends.

**Async durability** (`Async = TRUE`, `durability: :async`): a commit only extends the host's
local history (`ldb`, visible to other connections); the uploader then PUTs the frames not yet
durable in log order, through the same create-only upload and ownership confirm, retrying a
failed upload with the same frame. `db` is the durable history; `acked` records what a
`sync: true` commit or a flush may acknowledge (a confirmed upload), so `AckedDurable` becomes
"sync-acknowledged commits are durable" and `RestoreCommitted` the prefix property. A
checkpoint drains the queue first (`NoDrain` skips that and breaks `RestoreOK`: the snapshot
holds the local history while the uploader appends older frames to the new epoch).
Commits go on while the snapshot publication is pending (a failed snapshot upload is
retried later); the snapshot is uploaded from the checkpoint's image of the DB file
(`img`), which later commits and checkpoints don't change (`LiveSnapshot` uploads the live
state instead and breaks `RestoreOK`). Backpressure (`max_lag_ms`, `max_pending_bytes`) bounds the lag and is not modelled; it only
delays commits. Checked: `AsyncSole` 74,823 states, `AsyncSoleDelays` 155,958, `AsyncTakeover`
9,977,074 (two writers, late writes, a fault), all passing `AckedDurable`, `RestoreOK`,
`RestoreCommitted` and `SoleNeverFenced`; the sync configs pass `RestoreCommitted` too.

`Patches` switch on negative controls. Each must break a property; the table records it.

| Config | Hosts | Patches | Expect |
|---|---|---|---|
| `Sole` | 1 | none | pass |
| `SoleDelays` | 1 (late writes) | none | pass |
| `Takeover` | 2 (late writes, 1 fault) | none | pass |
| `TakeoverDeep` | 2 (late writes, 2 faults) | none | pass (long run) |
| `NegNoConfirm` | 2 | `NoConfirm` | `AckedDurable` violated |
| `NegListBeforeTakeover` | 2 | `ListBeforeTakeover` | `AckedDurable` violated |
| `NegRewriteOrphan` | 2 | `RewriteOrphan`, `ContinueEpoch` | `RestoreOK` violated |
| `NegOneOrphan` | 1 | `OneOrphan` | `SoleNeverFenced` violated |
| `NegContinueEpoch` | 1 (late writes) | `ContinueEpoch` | `SoleNeverFenced` violated |
| `AsyncSole` | 1, async | none | pass |
| `AsyncSoleDelays` | 1 (late writes), async | none | pass |
| `AsyncTakeover` | 2 (late writes, 1 fault), async | none | pass |
| `NegNoDrain` | 1, async | `NoDrain` | `RestoreOK` violated |
| `NegLiveSnapshot` | 1, async | `LiveSnapshot` (snapshot the live DB instead of the checkpoint's image) | `RestoreOK` violated |
| `NegIgnoreIfMatch` | 1 (late writes) | `IgnoreIfMatch` (a provider ignoring `If-Match`) | `ManifestForward` violated |
| `NegAsyncContinueEpoch` | 1 (late writes), async | `ContinueEpoch` | `AsyncSoleNeverPoisoned` violated |
| `VacuityControl` | 1 | none | `_POSSIBLE` `LateLanding` never witnessed |
| `NegRefuseLeftovers` | 1 | `RefuseLeftovers` (the earlier rule: any snapshot without a manifest refuses the open) | `_POSSIBLE` `BootstrapOverLeftover` never witnessed |
| `NegDeleteLeftovers` | 1 (late writes) | `DeleteLeftovers` (delete an interrupted bootstrap's snapshot before bootstrapping) | `RestoreOK` violated |
| `NegNoChecksum` | 1 | `NoChecksum` (a provider storing an empty object for a cut-off PUT) | `RestoreOK` violated |
| `NegNoChecksumFence` | 1 | `NoChecksum` | `SoleNeverFenced` violated |
| `LiveSole` | 1, liveness | none | pass (`EventuallyDurable`, `EventuallyRuns`) |
| `LiveAsyncSole` | 1, async, liveness | none | pass |
| `NegLiveStall` | 1, async, liveness | `StallOnFailure` | `EventuallyDurable` violated |
| `NegLiveRefuseLeftovers` | 1, liveness | `RefuseLeftovers` | `EventuallyRuns` violated |
| `LiveTakeover` | 2, liveness, `LeaseHolds` | none | pass |
| `LiveAsyncTakeover` | 2, async, liveness, `LeaseHolds` | none | pass |
| `NegLiveDuel` | 2, liveness, no lease assumption | none | `EventuallyRuns` violated (dueling takeovers) |
| `ReadTakeover` | 2 (late writes, 1 fault), 2 reads, retain 1 | none | pass |
| `NegReadNoRecheck` | 2 (late writes, 1 fault), 2 reads, retain 1 | `NoRecheck` (no second manifest read) | `ShownDurable` violated |
| `NegReadRecheckGeneration` | 2 (late writes, 1 fault), 2 reads, retain 1 | `RecheckGeneration` (the second read compares the generation only) | `ShownDurable` violated |
| `NegReadRecheckSeal` | 2 (late writes, 1 fault), 2 reads, retain 1 | `RecheckSeal` (a log that ended at its seal passes the second read even if the epoch moved on) | `ShownDurable` violated |
| `NegNoTakeoverSeal` | 2 (late writes, 1 fault), 2 reads, retain 1 | `NoTakeoverSeal` | `ShownDurable` violated |
| `NegReadTransitional` | 2 (late writes, 1 fault), 2 reads, retain 1 | `ReadTransitional`, `NoTakeoverSeal` (use a takeover's manifest before its seal) | `ShownDurable` violated |
| `Destroy` | 2 (1 commit and 1 open each, late writes, 1 fault, 1 kill), 1 destroy each | none | pass (6,704,792 states) |
| `AsyncDestroy` | as `Destroy`, async | none | pass (12,054,213 states) |
| `ReadDestroy` | as `Destroy`, no fault or kill, 2 reads | none | pass (1,586,375 states) |
| `NegDestroyDeletes` | 1 (late writes), 1 destroy | `DestroyDeletes` (delete the manifest instead of a tombstone) | `NewAfterDestroy` violated |
| `NegPurgeAll` | 2 (late writes, 1 fault), 1 destroy each | `PurgeAll` (purge ignoring the generation) | `RestoreOK` violated |
| `NegTakeAnyGeneration` | 2 (late writes, 1 fault), 1 destroy each | `TakeAnyGeneration` (an open builds on a manifest or tombstone of a newer lease) | `NewAfterDestroy` violated |
| `NegSeqFromZero` | as `Destroy` | `SeqFromZero` (a database over a tombstone starts at epoch 0), `GCAnyGeneration` | `RestoreOK` violated |
| `DestroyManifestLoss` | as `Destroy`, 2 checkpoints, no kill, `manifest.json` deleted once | none | pass (6,815,519 states) |
| `NegSeqFromManifest` | as `DestroyManifestLoss` | `SeqFromManifest` (the tombstone's sequence number from the manifest only, 0 without one), `GCAnyGeneration` | `RestoreOK` violated |
| `NegGCAnyGeneration` | as `DestroyManifestLoss` | `GCAnyGeneration` (GC collects epochs of newer lease generations too) | `RestoreOK` violated |
| `NegTrustRefused` | as `Destroy` | `TrustRefused` (a tombstone PUT reported refused is taken as refused) | `RefusedMeansIntact` violated |
| `NegAdoptUnchecked` | as `ReadDestroy` | `AdoptUnchecked` (a replica connection adopts the shared generation without a manifest read) | `ReadsAfterDestroy` violated |

### Destroy

`Destroys = n` lets each host run `destroy.rs` up to n times: take the lease (any clock,
so a running writer may be taken over, which is what `force` does), read the manifest,
PUT a tombstone `If-Match` on what it read (every outcome, including a late landing), then
delete the dead objects (log objects and snapshots of epochs whose generation is at most the
tombstone's) one at a time in any order, stopping at any point. An open over a tombstone
deletes the dead objects, refuses if anything else but its own first snapshot is there,
and bootstraps with `If-Match` on the tombstone, its first epoch after the tombstone's
sequence number. Readers treat a tombstone as no database.

The destroy configs are small (one commit and one open per host): the purge's deletes in
any order multiply the states, and a version with two of each did not finish in the disk
space available (over 80M states).

When a tombstone lands (also late), the ghosts restart: `acked`, `committed` and `shown`
are emptied, since the destroyed data is meant to go. The properties then catch any history
from before the destroy coming back. `NewAfterDestroy` adds that a database after a destroy
is one started after it: its epoch's generation is above the last tombstone's.
`ManifestForward` holds across destroys (the tombstone keeps the sequence number).

The generation rule also applies without destroys: an open whose lease is older than the
manifest it reads refuses. That only removes behaviours (stale opens), which is why the
two-writer configs have fewer states than before it.

Three more environment and reader details came from the reviews of the destroy:

- `DTomb` has an extra outcome, `lostrefused`: the PUT lands and the client's retry of it
  gets 412 (object_store retries on 5xx). The code checks a refusal with a GET and goes on
  if its tombstone is there; `RefusedMeansIntact` says a destroy that reported "refused"
  did not replace the manifest (`NegTrustRefused`).
- `ManifestLoss = n` deletes a database's `manifest.json` from outside up to n times (the
  guide's "manifest.json deleted" failure, which a destroy clears up), never while a
  create-only manifest PUT is in flight (that would recreate an old database, a hazard of
  the deletion itself) and never a tombstone (the docs say to keep it). The tombstone's
  sequence number comes from every object's epoch, not only the manifest's
  (`NegSeqFromManifest`), and GC never collects an epoch of a newer lease generation: a
  destroy that read "no manifest", stalled, and landed its create-only tombstone after a
  database had come and lost its manifest again would otherwise let a stale writer's GC
  delete the next database's snapshot (`NegGCAnyGeneration`). The sequence controls
  switch both protections off.
- Replica connections of one process share a restored generation (`rd.g`), which a refresh
  or a new connection adopts after one manifest read shows the same database
  (`ReadAdopt`): manifests carry an identity, the lease generation that created the
  database (`Manifest::database_id`, unique per creation), kept by every later manifest.
  The shared generation may outlive its epoch (`CacheOutlivesEpoch`, a witness in
  `ReadDestroy`): a pool keeps one state until a refresh. `ReadsAfterDestroy` (an
  action property: no read that returns after a tombstone returns an epoch it ended) checks
  it; `ShownDurable` can't, as it only applies while there is a database
  (`NegAdoptUnchecked`).

The negative controls are the designs this replaced, or the rules it needs. Deleting the
manifest instead lets an interrupted bootstrap's late create-only manifest land after the
destroy (`NegDestroyDeletes`). A purge must keep to the tombstone's generations
(`NegPurgeAll`). An open whose lease is older than the manifest or tombstone it reads must
refuse: otherwise a stale opener bootstraps over the tombstone in a generation the purge
deletes (`NegTakeAnyGeneration`). And a database over a tombstone must not restart at epoch
0: a stale writer of the destroyed database, collecting the epochs older than its own,
deletes the new database's first snapshot (`NegSeqFromZero`). The last two were found by
the model while the destroy was designed.

### Bootstrap over leftovers

Without a manifest, an open refuses to build over log objects or any snapshot except
epoch-0 ones. Only a bootstrap writes those before its manifest, and without a manifest
nothing was ever acknowledged. An interrupted first open therefore no longer locks the
prefix. The model follows the code. `NegRefuseLeftovers` (the earlier rule, refusing any
snapshot) never gets past such a leftover. `NegDeleteLeftovers` shows why leftovers are left
for GC instead of deleted first: the interrupted incarnation's manifest create can still be
in flight, land after the delete, and point the database at the deleted snapshot
(`RestoreOK`). Whether an open eventually succeeds after an interruption is liveness (below).

### Truncated uploads

The model's store is the protocol's assumption about the provider, and one of them
failed in practice: SeaweedFS stored an empty object when a PUT's connection broke
right after its headers, even under `If-None-Match`. Every upload therefore carries
`x-amz-checksum-sha256`, and a server rejects a body that doesn't match. In the model that
is the existing `fail` outcome, so the positive configs don't change. `NoChecksum` adds the
provider's old behaviour, a `trunc` outcome storing an empty manifest (restores nothing)
or an empty log frame. It breaks `RestoreOK` (`NegNoChecksum`: an empty manifest or a gap in
the log) and makes a sole writer fence itself on its own empty frame
(`NegNoChecksumFence`: `SoleNeverFenced`), which is what the torture test hit.

### Liveness

`LiveSpec` adds weak fairness on each host's own steps (open, takeover, restore, compaction,
upload, confirm, publication): a pool reopens, and the uploader and publication keep
trying. Commits, checkpoints, kills and late landings get none. TLC's liveness checking is
unsound with a state constraint or symmetry, so the `Live*` configs use neither and are
bounded by their constants alone (faults are at most `MaxFaults`, so they stop).

- `EventuallyDurable`: every local commit eventually becomes durable, unless its writer is
  killed or poisoned. `NegLiveStall` (an uploader that stops after a failed upload while the
  writer runs on, silently) breaks it.
- `EventuallyRuns`: whenever no host runs (at the start, or after a kill), one eventually
  does. `NegLiveRefuseLeftovers` (the earlier rule) breaks it: after an interrupted
  first open, every later open refuses, forever. The invariants couldn't see this.

With two hosts, liveness needs the lease to work. The model's lease accepts any clock (safety
must not depend on it), so without that assumption two hosts can take over from each other
until both give up. `NegLiveDuel` (2 hosts) breaks `EventuallyRuns` that way: each open fences
the other's compaction. `LeaseHolds` is the assumption: no host opens while another holds the
lease, from its open on, unless that one was poisoned. In the code, `Lease::acquire` refuses
while another writer's lease is valid, so the duel needs clocks that disagree by more than
the lease TTL. `LiveTakeover` (sync) and `LiveAsyncTakeover` (async) pass with it.

Liveness configs are kept small (1 commit or 2, 1 checkpoint, 1 fault, 3 or 4 opens). Without
a state constraint or symmetry the state space grows fast: the two-host async duel at 2 faults
ran 16 minutes. `bin/tlc` aborts a run when free disk space drops under 12 GB
(`TLC_MIN_FREE_GB`); shrink the constants rather than let a config run for hours.

Not checked: that a flush returns. The code bounds every flush with its timeout, and in
the model a flush waiting for durability is `EventuallyDurable`.

### Readers

`Readers = n` adds a reader taking up to n reads: `replica::stage` (a refresh, from the
replica's log cache when it holds the epoch, else in full) and `restore_to` (the current
state, or a past epoch the manifest retains). `Retain` is `retain_epochs`: each manifest
names the past epochs it keeps, and GC spares them. A read takes four steps that other
hosts' steps interleave: read the manifest; LIST the epoch's log keys; GET what they hold
then (and the snapshot), so a key collected in between fails the read and one recreated in
between returns its new content; read the manifest again. It returns the history only if
the second read still names the epoch read, as its current epoch or a retained one
(`restore::still_retained`), and it uses the current epoch only from a manifest its
epoch's writer wrote or once that epoch is sealed (`restore::usable`). The ghost `shown`
collects every history returned.

A takeover seals the old epoch at the end it listed, before it downloads the snapshot (the
seal's key taken: list again). Without that seal, the old writer's late upload of a commit
that failed can land in the old epoch after the takeover listed it, and readers of that
epoch (a refresh during the takeover, a restore of that past epoch) return a commit the
database never had (`NegNoTakeoverSeal`, `NegReadTransitional`).

The second manifest read is needed even with the seal: once the epoch is collected (seal
included), late uploads can recreate its keys, even a stale writer's seal, and a refresh
from its cache or a read whose GETs come after the collection takes them for commits
(`NegReadNoRecheck`). An epoch the manifest still names was never collected (GC deletes
only what the manifest it publishes no longer names, and that only moves forward), and its
log is final up to its seal, so a writer that merely moved on meanwhile doesn't matter.
Earlier rules were weaker or too strict: the generation alone misses a late upload into an
epoch the same writer collected (`NegReadRecheckGeneration`; regression test
`a_refresh_ignores_a_late_upload_into_a_collected_epoch`); the same generation and epoch,
or a log that ended at its seal, misses a stale writer's seal in a collected epoch
(`NegReadRecheckSeal`; the same test with a seal); the same generation and epoch is sound
but made every restore finish between two checkpoints, so restores of large, busy
databases failed (`RecheckEpoch`; regression test
`restores_finish_while_the_writer_checkpoints`). Astra's review schedules are
`NegReadNoRecheck` and `NegReadTransitional`.

Not a negative control: skipping the second read for a past epoch only (41M states
without a violation, unfinished). In the model GC deletes an epoch's snapshot with its log,
so a read of a collected past epoch fails at the snapshot download; in the code a later
epoch's delta chain can keep that snapshot (the model has no chains), so the code checks
past epochs too.

Checked: `ReadTakeover` 38,480,211 states. `bin/tlc` runs without TLC's periodic
checkpoints, which copy the state to disk. Run the reader configs with `JAVA_TOOL_OPTIONS=-Xmx3g` on a machine
with less than 24 GB (`ReadTakeover` keeps millions of states queued).

### Reachability

An invariant also holds when the situation it guards never happens. So each positive
config lists, under TLC's `_POSSIBLE` (provisional in TLC 2026.09.22), situations it must
reach, and TLC fails the run if one is never witnessed. `pass` in `bin/check` therefore
means no violation and every listed situation reached.

| Situation | Listed in |
|---|---|
| a commit acknowledged after a takeover (`AckAfterTakeover`) | every positive config |
| a commit acknowledged after a checkpoint (`AckAfterCheckpoint`) | every positive config |
| the database created next to an interrupted bootstrap's snapshot (`BootstrapOverLeftover`) | every positive config |
| a writer poisoned (`Poisoned`) | `Sole`, `SoleDelays`, `Takeover`, `TakeoverDeep`, `AsyncTakeover` |
| a late PUT landing (`LateLanding`) | configs with `Delays` |
| async running 2+ commits ahead of S3 (`AsyncAhead`) | async configs |
| async commits while a snapshot is pending (`CommitWhileSnapPending`) | async configs |
| a reader returned a history after a takeover (`ShownAfterTakeover`) | `ReadTakeover` |
| a reader used a takeover's manifest, its epoch sealed (`ReadDuringTakeover`) | `ReadTakeover` |
| a reader read a past epoch (`ReadPastEpoch`) | `ReadTakeover` |
| a takeover sealed the old epoch (`TakeoverSealed`) | `ReadTakeover` |
| a read accepted after the writer moved to a new epoch (`ReadAcrossCheckpoint`) | `ReadTakeover` |

`VacuityControl` (`Sole`, which has no late writes, asked for `LateLanding`) must fail
with that predicate unwitnessed, which shows the check isn't silent.

`Poisoned` is not listed for `AsyncSole` or `AsyncSoleDelays`: a sole async writer is never
poisoned, checked as the invariant `AsyncSoleNeverPoisoned`. The uploader retries the
same frame until it lands, so the sync-mode indeterminate case (a rolled-back commit
whose frame later turns up at its offset) can't arise. `ContinueEpoch` (a restarted
writer appending to the restored epoch) breaks it (`NegAsyncContinueEpoch`).

GC deletes old epochs entirely. With `KeepTails` (the earlier behaviour: keep each old
epoch's last object as a tombstone) the model passes too; the tombstones became redundant
once acknowledgement required the ownership confirm (`Takeover` with GC deleting everything:
4,643,392 states, pass).

The model treats a snapshot as one object. Incremental snapshots (a full snapshot plus
deltas, listed in the manifest) are outside it: GC keeps every snapshot object a retained
epoch's chain references, a chain object is never rewritten (keys are per epoch), and a
restore checks the rebuilt file against the last link's size and CRC32C. The crash-torture
test (`test/sediment/s3_torture_test.exs`) exercises chains with kills and faults.

`RewriteOrphan` alone does not break anything anymore: since every open moves to a fresh
epoch, an orphan rewrite lands in an epoch nobody reads, and the rewriting writer's
ownership confirm fails. Both rules stay (defense in depth).

Model-found bugs so far, both fixed in the code, with a regression test each:

- Only the last failed attempt at an offset was remembered. A landed frame whose confirm
  failed, followed by a failed retry, made the writer fence itself
  (`every_failed_attempt_at_an_offset_is_remembered`).
- A killed incarnation's late PUT landed in the epoch its restarted successor was
  appending to, and fenced the successor. Every open now moves to a fresh epoch.
- A replica refresh that compared only the manifest's generation took a late upload into
  a collected epoch for a new commit (see "Readers").

## Trace validation

`trace/` checks that what the code really sends is a behaviour of the model. A workload
runs against a real S3 server with the request meter's trace mode on
(`SEDIMENT_S3_TRACE=1`: every request's key and condition, every answer's status and ETag).
`trace/s3trace.py` turns one writer's log into S3Fence events: open, close, bootstrap,
takeover, restore, compaction, frame PUT (epoch, ordinal offset, outcome), ownership
HEAD (did it see our manifest), seal, the takeover's seal, snapshot, manifest, GC. Then
`TraceS3Fence.tla` asks TLC for a behaviour that matches every event with its observed
outcome; a checkpoint, the start of a publication and GC may happen in between (the
store doesn't see them). On rejection, `trace/check` reports the first event the model
can't match.

Every request is classified, and one that fits no rule stops the converter. The report
lists what is not in the model: the probe, lease renewals, reads that only feed the next
event's decision, the snapshot upload's HEAD, and the compaction's snapshot upload (part
of the compaction). Async commits are local steps of the trace spec. The uploader's segment
PUTs are the frames, and a coalesced segment of several commits is one model frame (the
trace doesn't check content). Known modelling gap: the code collects garbage at open, and the
model's open doesn't. Its deletes are matched as observations.

Mapping rules (each counted in the converter's report, none silent):

- An attempt that got no answer, followed by an identical request on the same key, is
  object_store's retry: one logical request with the last attempt's answer. A retry of a
  create-only or `If-Match` PUT whose first attempt landed gets a 412, as the code sees it.
- A frame or seal PUT without an answer takes its outcome from the code's resolving read of
  the key: found means landed (the model's `lost`), otherwise failed.
- Seal PUTs are tagged by the code (`TraceTag`, a request extension, never sent), the
  takeover's seal as `takeover-seal`. Its 412 is our own seal when the code goes on to the
  snapshot download after reading the key back, and a key taken since the listing when it
  lists the log again.
- A failed lease acquisition is no `Open` (the model's `Open` is a successful acquisition).
- Generations are numbered by incarnation (the k-th successful open is generation k). The
  real counter skips when an acquisition landed without an answer.

    formal/trace/run trace/workload_sync.exs          # SeaweedFS unless S3_TEST_* says otherwise
    formal/trace/run trace/workload_async.exs TRUE    # async (the model's Async = TRUE)
    formal/trace/controls [workload] [TRUE]           # the corrupted-log controls below

| Trace (one writer) | Events | Result |
|---|---|---|
| `workload_sync.exs`: bootstrap, commits, checkpoint, close, reopen, commit | 25 | accepted |
| `workload_sync_faults.exs`: a commit whose PUT never lands, one whose answer is lost after it landed, a publication whose manifest answer is lost, reopen | 31 | accepted |
| `workload_sync_retries.exs`: random connection resets with the client's retries on (lost answers, 412 retries, a takeover whose own landed PUT fenced it, failed lease acquisitions) | 101 to 107 | accepted |
| `workload_async.exs`: the same, async (commits are local; the uploader's segments are the frames) | 25 | accepted |
| `workload_async_faults.exs`: uploads failing while S3 is cut off (the same segment retried), a segment whose answer is lost after it landed, a lost manifest answer (re-seal, adopt), reopen | 30 | accepted |
| `workload_async_retries.exs`: random connection resets with the client's retries on | 45 to 57 | accepted |
| `workload_takeover.exs`: writer A (its own OS process, behind a proxy delaying requests up to 3 s) is killed with kill -9 while a frame PUT is in flight; writer B opens directly, takes over (sealing A's epoch), commits. A's PUT either finds B's seal at its key, or lands after B collected A's epoch (B's restore didn't find it, yet it exists at the end): observed as `present`, which the model explains only by a late landing of A's in-flight PUT. Before the takeover seal 3 of 3 runs landed late; with it, 1 of 3 (2 refused by the seal) | 21 to 22 | accepted |
| `workload_import.exs`: `Sediment.S3.import/3` of an existing database with an AUTOINCREMENT table (copied into a new MVCC database, verified by a restore), then an open that takes over, commits, checkpoints and closes. The import is the model's bootstrap with a non-empty first snapshot (the model doesn't look at snapshot contents) | 17 | accepted |
| the takeover log with a `present` object that was never PUT | 21 | rejected at it |
| `controls` (sync and async logs): without one ownership HEAD / without the seal / without the takeover's seal / with a frame moved into the sealed epoch | 24 / 24 / 24 / 25 (async 22 / 22 / 22 / 23) | rejected at that event |

Writers that follow one another (separate processes, e.g. after a kill -9) are one merged
trace: their per-process logs ordered by time, which is exact when they don't overlap. A
request the killed writer sent without getting an answer can be any outcome, including still
in flight (the model's `late`, landing later). Not covered: two writers *overlapping* (a
zombie still writing while another has taken over). The order of their concurrent requests
can't be recovered from millisecond timestamps, and checking every order consistent with each
request's sent-to-answered window is a different tool. That case is covered by the model
check (`Takeover`, `AsyncTakeover`: 2 writers, late writes, 12.9M states) and by the crash
torture's zombie kills (a paused writer resumed after another took over; prefix oracle).

Found: the model's publication adopted a manifest of ours whose answer was lost *before*
re-sealing the old epoch. The code re-seals first (an idempotent create-only PUT), then
adopts. A trace with a lost manifest answer was rejected by the old model right
after the re-seal. It is benign for safety (the re-seal succeeds only on our own seal and
fences on anything else, correct in either order), and the model now follows the code
(`Adopt` after `Seal`).

## What the model does not cover

The model checks the ownership protocol: which objects exist, which conditional writes
succeed, and what a restore finds. It has logical time only, one abstract object per
snapshot and per commit, and no costs. These properties are checked by tests instead:

| Not in the model | Why | Covered by |
|---|---|---|
| Real time: checkpoint freezes, lock hold times, latencies | TLA+ has no real time | `a_checkpoint_does_not_wait_for_the_snapshot_upload`, `close_waits_for_a_slow_snapshot_upload_only_within_its_timeout`, "an S3 open stuck on the network doesn't hold up opens of other files", `busy_wait_test.exs`, `bench/` |
| Clocks: lease expiry and point-in-time restore cutoffs | the lease accepts any clock (safety must not depend on it); `LeaseHolds` is only a liveness assumption | the S3 guide ("Clocks", point-in-time restore); "restore/3 rebuilds a past state"; the Tigris soak found a 227 s host skew this way |
| Costs and request counts | not a behaviour property | the request meter and its independent check (`s3_meter_test.exs`) |
| Bytes: incremental snapshot chains, compression, multipart and its part sizes, encryption | one abstract object per snapshot | CRC32C and size checks at every restore; `incremental.rs`, `incremental_snapshot_sizes_probe`; `a_snapshot_larger_than_its_parts_allow_at_8_mib_still_uploads`; the crash-torture test |
| The encryption choice and the manifest's `encrypted` flag | checked before any write, or a manifest field the model abstracts: an open with the wrong choice is refused before the lease's first manifest write, the takeover keeps the flag as it was, and only the epoch advance after a successful restore records it | `an_s3_database_needs_a_key_or_an_explicit_opt_out`, `a_key_on_an_unencrypted_prefix_is_refused_and_writes_nothing`, `an_unrecorded_encryption_is_recorded_only_by_an_open_that_reads_the_database`, `a_second_open_of_a_database_open_here_needs_the_same_encryption`; S3 tests in `s3_test.exs` |
| Import | the model's bootstrap with a non-empty first snapshot (the model doesn't look at snapshot contents); the same create-only manifest is the commit point | trace validation (`workload_import.exs`), `tests/import.rs` (failure paths, lost manifest race, re-runs) |
| Group commit | a batch uses the same create-only upload and ownership confirm as one frame | `group_commit.rs`, incl. `failed_batch_upload_acknowledges_nothing_it_lost` (after a leader's failed batch, turso let a waiting commit sync the written prefix, so `sync` must keep failing once poisoned) |
| Coalesced async segments (incl. `upload_interval_ms`) | one model frame per upload; a segment is uploaded and retried as one object, and how long the uploader waits before it (the interval) is timing, which the model leaves out: any wait is one of its interleavings | `async_durability.rs`; crash torture (async, prefix oracle) |
| Threads inside one writer (uploader, background publisher, lease renewer) | one writer's steps are serialized by one `pc`, like the writer-state lock in the code: the background publisher and a commit that finds a publication pending both publish under that lock, and no frame of the next epoch is uploaded before it (the model's `CommitPut` waits for `PublishBegin`) | `a_busy_writer_with_constant_checkpoints_never_fences_itself`, `a_checkpoint_does_not_wait_for_the_snapshot_upload`; crash torture |
| Replica pools, point-in-time restore cutoffs, export to SQLite | the reads themselves are in the model (`Readers`: refreshes, restores of the current or a past epoch); a pool shares one read among its connections, a cutoff ends a past epoch's log earlier, an export copies a restore | `replica.rs`, `restore.rs` (`usable`, `still_retained`, `restore_and_seal`); the takeover tests in `tests/replica.rs`; the crash torture's refreshing replica; "refresh/1 brings every connection of a replica pool up to date"; "restore/3 rebuilds a past state"; `tests/export.rs` (every export checked with SQLite) |
| The provider itself | the store is an assumption: conditional writes enforced, checksummed bodies verified | the open's probe (refuses providers ignoring conditional PUTs), `NegIgnoreIfMatch` and `NegNoChecksum*`, "uploads cut off after their headers", suites on SeaweedFS, MinIO and Tigris |
| Whether the code matches the model | a model can drift from the code (the bootstrap rule and the publication order did) | trace validation (below: one writer, sync and async, faults, sequential takeovers), regression tests for every model-found bug |

Running (JDK from `mise.toml`, `tla2tools.jar` in `~/tools` or `$TLA_TOOLS`):

```sh
cd formal/tla && ../bin/tlc S3Fence.tla -config Sole.cfg
formal/bin/check        # every config, compared with the table's expectation (~15 min)
```
