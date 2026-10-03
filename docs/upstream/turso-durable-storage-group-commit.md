# turso_core: DurableStorage has no end-of-batch hook for group commit

Report draft for tursodatabase/turso (turso_core 0.8.1).

## Context

Sediment (an Elixir binding) implements S3 durability as a `DurableStorage`: a commit is
acknowledged only after its logical-log frame is stored remotely
(`on_log_write_complete`). With MVCC group commit, the leader writes each
queued record in turn (`log_tx`, `on_log_write_complete`, advance) and calls
`sync` once for the batch before any member is published. To upload a batch
as one object, the storage buffers frames in `on_log_write_complete` and
uploads them in `sync`.

## Problems

1. `sync` is only called when a member uses `synchronous = FULL`
   (`log_sync_required`). With `NORMAL`, the batch is published without any
   storage call after its last record, so a remote storage can't batch
   safely. Sediment has to force `FULL` on every connection that can
   reach an S3 database.
2. After a leader's failed `sync`, a waiting commit runs `SyncGroupPrefix`
   (another `sync`) and then marks the written prefix durable. A storage that
   failed to store the batch must keep failing `sync` (Sediment poisons
   itself), or the prefix is acknowledged without being stored. The failure
   semantics aren't documented on the trait.
3. Only `BEGIN CONCURRENT` transactions are grouped. Exclusive (default)
   transactions always go one by one.

## Suggested API

A hook called once per group after the last record's write and before
publication, independent of the sync mode (for example
`on_group_write_complete(first_offset, bytes) -> Completion`), with documented
semantics when it fails: the whole group fails and is rolled back. Plus a doc
note on `sync` failure and prefix sync.
