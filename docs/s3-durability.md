# S3 durability for sediment

Status: design verified against turso_core 0.8.1 (crates.io source).

## Goal

A turso database whose durable state of record lives in an S3 bucket/prefix.
The local file is a working copy. With `durability: :async` (the default)
a commit is acknowledged once its logical-log frame is in the local log, and
a background uploader stores it in S3 shortly after, in commit order; a crash
can lose a bounded tail of commits, but a restore always finds a prefix of
the commit order. With `durability: :sync` a commit is acknowledged only
after its frame is stored in S3. Opening the database on a fresh machine
restores it from S3.

## Extension point (verified)

- `turso_core::mvcc::persistent_storage::DurableStorage` is attached with
  `OpenOptions::durable_storage(Arc<dyn DurableStorage>)`. It is only used
  when the database runs in MVCC journal mode.
- MVCC mode is controlled only by the database header version
  (`PRAGMA journal_mode = 'mvcc'` writes it). There is no open option for it.
  `Database::open` reads the header and calls `open_mv_store` with our
  storage. Switching with the pragma on a live DB also uses our storage.
- The default storage is `persistent_storage::Storage` over the file
  `Path::with_extension("db-log")` (`app.db` -> `app.db-log`). We wrap
  `Storage` and delegate everything, using the same log path.
- MVCC refuses an encrypted database with a custom DurableStorage unless
  the storage provides an encryption context. `S3DurableStorage` builds one
  from the `:encryption` open option, gives it to the wrapped `Storage` (so
  log frames are encrypted) and returns it from `encryption_ctx`. The frame
  header keeps the plaintext payload size, so the restore verifier adds the
  cipher's tag and nonce per 32 KiB chunk to find frame boundaries; the CRC
  covers the ciphertext, so verification needs no key. Snapshots are the
  encrypted database file. Local checkpoints/bootstraps open with the key.
- The manifest's `encrypted` field records whether the database is
  encrypted: `true`, `false`, or absent in manifests written before it was
  always recorded. An open refuses a key on `false` and a missing key on
  `true` before writing anything. The takeover keeps the field as it was;
  only the epoch-advance manifest, written after the restore and checkpoint
  under the open's key (or without one) succeeded, records it, so a wrong
  key or a missing one can never mark a database the other way.

### Hook sequence (observed with a tracing wrapper)

Commit (per transaction, including in group-commit batches):
`upgrade_header_for_log_tx` -> `log_tx` -> `on_log_write_complete` ->
`advance_logical_log_offset_after_success` -> `sync`.

- The commit path calls `storage.log_tx(record, None)`. The wrapper passes
  its own `OnSerializationComplete` callback to the inner `Storage::log_tx`.
  That callback receives the framed bytes and `LogTxFrameInfo
  {logical_start_offset, start_crc32c, end_crc32c}`.
- The frame written at offset 0 of an epoch includes the 56-byte log header.
  Concatenating an epoch's frames in offset order reproduces the local
  `-log` file byte for byte.
- `on_log_write_complete` runs before the offset advances and before the
  transaction becomes visible. Returning `Err` aborts the commit: the commit
  state machine is dropped, `discard_pending_log_write` runs, and the
  transaction rolls back. The next commit rewrites the same offset.
- `upgrade_header_for_log_tx` only returns `Some` with the `conn_raw_api`
  feature (portable changes), which we don't enable. The wrapper treats
  `Some` as unsupported.

Checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)` or auto at
`checkpoint_threshold`, default 4,120,000 log bytes):
`on_checkpoint_start` -> rows written to WAL -> WAL backfilled into DB file ->
DB fsync -> `truncate(boundary)` (`Truncated` resets the log to 0 bytes with a
new salt; `Retained` leaves it intact) -> `sync` -> WAL truncated ->
`on_checkpoint_end(Ok)`. The checkpoint locks are held during
`on_checkpoint_end`, so no commit can race it.

In MVCC mode the DB file only changes during a checkpoint, and the WAL is
empty between checkpoints. So the full state is always
`DB file (as of last checkpoint) + logical log`.

## Object layout (under `<prefix>/`)

```
lease.json                           {owner, generation, expires_at_ms}
manifest.json                        {version, generation, epoch, epoch_id, snapshot, ...}
snapshots/<epoch_id>.db              DB file at the start of the epoch (zstd)
snapshots/<epoch_id>.delta           or: its changed 64 KiB segments (zstd)
log/<epoch_id>/<offset:020>          one object per committed frame
log/<epoch_id>/<end offset:020>      seal: "TURSO-S3-SEAL\n{...}", closes the epoch
```

`epoch_id` is `<seq:020>-<generation:010>`. `seq` increases by one per
truncating checkpoint. `generation` is the lease generation of the writer
that started the epoch, so a stale writer can never write into a key range
a newer writer will use.

## Commit path (sync durability)

1. `log_tx` captures the frame bytes and chain info as the pending frame.
2. `on_log_write_complete`:
   - fails fast if the storage is poisoned (fenced) or the lease has lapsed
     and can't be renewed;
   - if a snapshot is pending (see checkpoint), uploads it and writes the
     manifest first;
   - PUTs `log/<epoch_id>/<offset>` with `If-None-Match: *`;
   - confirms ownership: HEADs `manifest.json`. The commit is acked
     only if the etag is still the one this writer last wrote;
   - returns `Err` on any failure, which fails and rolls back the commit.
3. Lost answers:
   - A PUT that errors is followed by a GET. If our bytes are there, the
     PUT landed and the commit is acked.
   - A 412 whose object holds our exact bytes is also success (a retry).
   - Landed objects are never rewritten. If a frame whose commit
     reported failure lands later, the next commit at that offset finds it.
     The storage is then poisoned and must be reopened, and the reopen
     restores that frame. Such a commit is *indeterminate*: reported failed,
     yet durable. This is the usual contract of an ambiguous commit. Every
     attempt at an offset is remembered, not only the last, so the writer
     never takes one of its own frames for another writer's.
     Anything else at the key means another writer, which also poisons.

Cost: one PUT plus one HEAD per commit.

## Commit path (async durability, the default)

1. `log_tx` first waits while the upload queue is over `max_pending_bytes`
   or its oldest frame is older than `max_lag_ms` (backpressure: an outage
   doesn't turn into unbounded loss). Waiting happens here, before the local
   write, because anything that fails after the frame is in the local log
   would leave a frame that is replayed on reopen but never uploaded.
2. `on_log_write_complete` appends the frame to the upload queue with a
   sequence number and returns.
3. The uploader thread takes the queue's head, coalesces contiguous frames
   into one segment of up to 8 MiB, and uploads it exactly like a sync
   commit (create-only PUT, lost-answer handling, ownership confirm). A
   failed segment is retried unchanged; a fence, or 6 failed attempts,
   poisons the writer, and the queued tail is recorded as a loss (reported
   by the next writer of the file until acknowledged).
4. `flush` waits until the durable sequence reaches everything queued so
   far; `sync: true` only until it reaches the sequence of the connection's
   own commit (recorded by the NIF from the committing thread; if turso's
   group commit queued it on another thread, everything queued), and a
   statement or transaction that committed nothing returns at once. A
   checkpoint drains the queue before the epoch changes (at most 10 s,
   otherwise the checkpoint fails and runs later), so no segment is ever
   uploaded into the wrong epoch. Close drains it too (`close_timeout_ms`),
   and the VM's `System.at_exit` hook flushes every open S3 database.

## Checkpoint -> snapshot -> manifest -> GC

- `truncate` drains the async upload queue, copies the backfilled DB file
  to a snapshot image next to it, and, when the log was `Truncated`, starts
  epoch `seq+1` locally, remembers the old epoch's end offset, and marks a
  snapshot (of that image) as pending. `Retained` changes nothing: the old snapshot plus the
  full log is still a valid restore point.
- `on_checkpoint_end(Ok)` only wakes the storage's background thread and
  returns: turso holds its stop-the-world checkpoint lock until this hook
  returns, so no S3 work happens under it. The background thread (or the
  next sync commit, whichever comes first) publishes the pending snapshot:
  1. Seal the old epoch: create-only PUT of a seal object at
     `log/<old epoch>/<end offset>`. If that key holds anything else (not
     our seal, not our own rolled-back frame), another writer appended, so
     we're fenced.
  2. Upload the snapshot image to `snapshots/<new epoch_id>.db`, zstd-compressed
     and streamed (multipart once larger than one part: equal parts of 8 MiB, or
     larger so the object fits in 10,000 parts). With
     incremental snapshots (default), upload instead
     `snapshots/<new epoch_id>.delta`: the 64 KiB segments whose xxh3-128
     hash differs from the previous snapshot's. The manifest then lists the
     chain (`snapshot_base`: a full snapshot, then deltas; the manifest has
     version 2 while any retained epoch is a chain, so older drivers refuse it
     rather than drop the chains from the history they rewrite), and
     the last link's size and CRC32C are the rebuilt file's. A full snapshot
     starts a new chain after 16 links, when more than half the file
     changed, or when the deltas outgrow the full snapshot. The writer keeps
     the hashes keyed by the snapshot object they describe and uses them
     only while the manifest points at that object; at open it hashes the
     restored snapshot before replaying the log.
  3. Write the manifest (`If-Match` on the manifest etag we hold).
  4. GC, on the background thread, outside the writer's state lock, in
     DeleteObjects batches: delete log epochs older than the manifest's epoch that it doesn't
     retain (`retain_epochs`), and older snapshot objects that no retained
     epoch's chain references. A stale writer's late PUT may
     recreate a key in a deleted epoch, but it is never acknowledged (its
     ownership confirm sees the newer manifest) and nothing reads it. An
     earlier version kept each old epoch's seal as a tombstone; the model
     (`KeepTails` patch) shows that isn't needed.
- If any step fails, the snapshot stays pending. The next segment upload
  retries it before its own PUT, from the image, which is unchanged until
  the next checkpoint. No commit is acknowledged into an epoch the manifest doesn't
  reference. A crash in between is harmless: the manifest still references
  the previous snapshot and the complete previous epoch.

## Open / restore

`s3::prepare(config, local_path)`:

0. Probe the store (once per endpoint/region/bucket per VM, `probe.rs`):
   create-only PUT twice (second must be 412), If-Match on a wrong etag
   (412), If-Match on the right one (200), then delete. A store that
   ignores the conditions is refused: fencing depends on them. See
   `guides/s3_providers.md`.
1. Acquire the lease (below).
2. GET `manifest.json`. If present, take it over right away (`If-Match`,
   generation := our lease generation), before reading anything else.
   - Missing: bootstrap. Create the local DB with a plain turso open plus
     `PRAGMA journal_mode = 'mvcc'`, close it, upload it as the epoch-0
     snapshot, then create the manifest with `If-None-Match: *`.
   - Present: download and decompress the snapshot (plaintext size and CRC32C
     checked; manifests without `snapshot_zstd` mean a raw snapshot) to
     `<path>.s3-restore`, removed on any error, and rename it. A chain is
     rebuilt in `<path>.s3-chain` (full snapshot, then each delta applied),
     checked against the last link's size and CRC32C, and renamed
     over `<path>`. Delete `<path>-wal`. Rebuild `<path minus ext>.db-log`
     from the epoch's segments, listed and sorted by offset. A seal may only
     be the last object and isn't written to the local log.
3. Verify the log while rebuilding. The segments must be contiguous
   (`next.offset == prev.offset + prev.len`, starting at 0). Segment 0 must
   start with a valid log header. Every frame's CRC is recomputed from the
   salt-derived seed across all segments (the chain must be continuous). A
   gap, a stray segment, or a CRC mismatch fails the open with
   `Corrupt`. Turso's own recovery would silently treat that as a torn tail.
4. Fold the restored log into the DB file with a local turso checkpoint,
   upload it as the snapshot of a fresh epoch `(seq+1, our generation)`, and
   point the manifest at it (`If-Match` on the takeover). Every open writes
   only to an epoch of its own. A killed earlier incarnation's or a stale
   writer's late PUTs can only land in keys nobody reads anymore, and those
   commits are never acknowledged (see "Why a stale writer..."). This was
   found by the TLA+ model; the cost is one snapshot upload per open.
5. Return `S3DurableStorage`. The caller opens with it and turso's MVCC
   recovery replays the log.

## Single writer (lease)

`lease.json` = `{owner, generation, expires_at_ms}`.

- Acquire: create it with `If-None-Match: *`. If it exists and has expired
  (or is ours), replace it with `If-Match: <etag>` and `generation + 1`.
  Otherwise fail with `LeaseHeld` (owner and expiry in the message).
- A background thread renews every `ttl/3` with `If-Match`. A 412 poisons
  the storage (`Fenced`).
- Before every S3 mutation the writer checks
  `now < expires_at - safety_margin`, renewing synchronously if needed.
- Close releases the lease (expires_at := 0, `If-Match`).
### Why a stale writer can't lose acknowledged data

The lease alone isn't fencing. A writer can pass its lease check and then
stall for longer than the TTL before its PUT reaches S3. Safety therefore
rests on conditions the store evaluates when a write applies, plus one
read after it:

- Takeover happens first. A new writer rewrites the manifest
  (`If-Match`) before it lists the log. A frame that lands after that is
  never acked, because its writer's ownership check sees a foreign
  manifest etag. That holds even if the frame lands at a key GC has freed
  (`stale_commit_landing_in_a_collected_epoch_is_not_acknowledged`).
- A frame that lands *before* the takeover is either listed by the new
  writer's restore, or collides with the new writer's first append or seal
  at that offset (create-only). Either way it's kept, and the new writer
  is at worst fenced and must reopen.
- An epoch ends only with a create-only seal at its end offset, written
  before the manifest moves on, so a restore can tell a closed epoch from
  a live one (`delayed_commit_of_a_stale_writer_cannot_land_after_takeover`).
- The manifest only changes with `If-Match`. A delayed stale manifest PUT
  gets 412 (`delayed_manifest_of_a_stale_writer_is_rejected`). The writer
  remembers every manifest body it sent without an answer (the chain), so
  its own late or lost manifest is adopted instead of fencing it
  (`late_landing_manifest_is_adopted`). A successful manifest write
  discards the chain: every other body was conditional on the replaced
  version. Lease renewals do the same: a lease still carrying our owner and
  generation was written by us.
- Snapshot keys include the writer generation. Snapshots are checked by
  size after upload and by size plus CRC32C (recorded in the manifest) on
  restore.
- GC only deletes epochs older than a manifest the GC-ing writer itself
  published.

These rules came from an audit against sqlite_replica's ReplicaFence TLA+
model and from this protocol's own model,
`formal/tla/S3Fence.tla` (see `formal/README.md`).

## Threading

Hooks run on the thread that steps the statement (a dirty IO scheduler in
the NIF). We block there. A pending `Completion` finished from another
thread would make callers busy-spin in `io.step()`. S3 calls run on a
private multi-thread tokio runtime; the caller waits on a channel. Every
store operation is bounded by `request_timeout * (max_retries + 1)`, so a
store that stops answering fails the commit instead of hanging it (a late
landing is then handled like any lost answer). A panicking client task is
reported as a store error, never as a NIF panic.

Each storage has two threads of its own: the async uploader (above) and a
background thread that publishes a pending snapshot and collects garbage
after a checkpoint. Both hold only a `Weak` reference between passes, so a
storage is dropped when its last connection closes; `close` waits for a
running background pass first.

## Sharing, fencing and recovery in the VM

- One storage per file: a process-wide registry keyed by the canonical
  directory plus file name (so `..` and symlink aliases share it). Every
  S3 connection to a path must use the same behaviour-relevant settings
  (location, owner, lease TTL, group commit, retention, threshold).
- A plain (non-S3) open of a file whose S3 storage is live attaches that
  storage (forced FULL sync under group commit, MVCC guard). An S3 open
  refuses a file that plain connections have open (it would restore over
  it).
- Every step on an S3 connection checks, before running and when done, that
  the storage isn't poisoned and the database is still MVCC. A poisoned
  writer fails reads too: its view may miss another writer's commits.
- `Sediment.Connection` turns "s3 writer fenced" into `{:disconnect, ...}`
  and pings idle connections, so a DBConnection pool reconnects with
  backoff; once nothing holds the poisoned storage, the reconnect restores
  from S3 (or waits for the other writer's lease).
- The NIF monitors each DBConnection process and closes its connection
  when the process dies, so cached statements can't keep a lease alive.

## S3 client

`object_store` with the `aws` feature: conditional puts (`PutMode::Create`
maps to `If-None-Match: *`, `PutMode::Update` to `If-Match`), listing,
retries. Tests use `object_store::memory::InMemory` (same conditional
semantics) wrapped in a fault-injecting store (fail, lost answer, delay,
land-later, ignore-conditions), a seeded randomized fault soak, and real
servers for end-to-end tests: SeaweedFS (`http://127.0.0.1:8333`) and MinIO
(`S3_TEST_ENDPOINT=...`), including a `kill -9` crash test.

## Server facts (verified)

MinIO: same results as SeaweedFS below; it also closes the connection after
a 412, which a retry absorbs.

SeaweedFS:

- `If-None-Match: *` on PUT: 200 first, then 412.
- `If-Match: <etag>` on PUT: 200 when the etag is current, 412 when stale.

## Limitations / later

- Sync durability makes one PUT per commit; async (the default) uploads
  contiguous commits as one segment of up to 8 MiB. With `group_commit: true`
  `on_log_write_complete` only buffers the frame and `sync`, which turso
  runs once per group-commit batch before acknowledging any of its
  transactions, uploads the batch as one segment (same PUT + ownership
  HEAD). turso skips `sync` unless the commit is `synchronous = FULL`, so the
  NIF holds such connections at FULL. A failed batch upload poisons the
  storage (the log already advanced past it). Truncation flushes the batch
  first.
- A snapshot upload on every truncating checkpoint (zstd level 3, streamed
  in multipart parts (8 MiB, larger above about 70 GiB); the manifest records the plaintext size and
  CRC32C plus the stored size and encoding, verified on download). It is a
  delta of the changed 64 KiB segments unless the chain is due for a full
  snapshot, but finding the changes reads the whole local file. Tune with
  `checkpoint_threshold`.
- Every open downloads the snapshot and uploads a new one (fresh epoch per
  open, see "Open / restore"). A local-cache fast path would have to keep
  that property.
- A process holds one storage per path. Opening a path whose storage is
  poisoned fails until every connection to it is closed.
- `owner` defaults to `<hostname>-<os pid>-<random>`. A restarted process waits one lease
  TTL unless it passes a stable `owner`. Two processes configured with the
  same `owner` take over from each other at once; don't share owners.
- Read-only replicas (`mode: :replica`) restore the manifest's
  epoch like a writer but skip the lease, the takeover and every write, and
  retry with a fresh manifest when the writer collects the epoch mid-restore.
  The connections to one replica in a process share a generation: one working
  copy (`.<name>.replica-<pid>-<n>.db` next to the path) and one turso
  `Database`. `Sediment.S3.refresh/1` stages the latest state once and
  publishes it as the new generation; the other connections switch to it at
  their next refresh, and a copy is removed when its last connection has moved
  on. A replica keeps the snapshot it last restored (`.<name>.s3-cache-<hash>`),
  so a refresh downloads only the newer deltas and the log.
- Point-in-time restore (`retain_epochs`, `Sediment.S3.restore/3`):
  GC keeps the epochs the manifest lists in `history` (newest
  first, bounded by `retain_epochs`); only those canonical epochs are
  restorable, never a stale writer's orphaned ones. A time target picks the
  newest retained epoch with `started_at_ms <= T` and replays the
  contiguous run of its segments whose S3 `last_modified <= T`. The result
  is folded into a standalone local file; the S3 prefix isn't rolled back.

## Upstream

Report drafts for turso_core limitations found while building this:
[`upstream/turso-durable-storage-group-commit.md`](upstream/turso-durable-storage-group-commit.md),
[`upstream/turso-mvcc-hot-row-versions.md`](upstream/turso-mvcc-hot-row-versions.md).

