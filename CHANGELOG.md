# Changelog

## 0.1.0 (unreleased)

First release.

### Core API

* `Sediment.Engine` / `Sediment.Native`: the `Exqlite.Sqlite3` API over
  turso_core 0.8.1 (see the exqlite parity guide for differences).
* `DBConnection` implementation: `Sediment`, `Sediment.Connection`,
  `Query`, `Result`, `Stream`, `Pragma`, `Basic`, `Error`, `TypeExtension`.
* Telemetry events (`Sediment.Telemetry`).

### Turso extensions

* MVCC and `BEGIN CONCURRENT`, encryption, vector search
  (`Sediment.Vector`), full-text search, change data capture
  (`Sediment.CDC`), experimental feature flags.

### S3 durability

* S3 databases are encrypted by default: an open with `s3:` (writer or
  replica), `Sediment.S3.restore/3` and `Sediment.S3.import/3` need an
  `:encryption` key or `encryption: false`, the explicit opt-out; without
  either they fail, saying whether the prefix holds an encrypted or an
  unencrypted database. `Sediment.S3.generate_key/0` makes a key.
* Snapshot uploads size their multipart parts by the database: 8 MiB, or
  larger so every snapshot fits in S3's 10,000 parts (up to its 5 TiB object
  limit); every part but the last has the same size, as R2 requires.
* `Sediment.S3.import/3` uploads an existing SQLite or Sediment database
  file into an empty prefix (a logical copy into a new MVCC database).
* The `s3:` option and `Sediment.S3` keep the database's state of record
  in an S3 bucket. Durability is asynchronous by default: commits return once
  local, and a background uploader stores them in commit order with bounded
  lag (`max_lag_ms`, `max_pending_bytes`). `sync: true` on a transaction or
  statement waits for that commit, `Sediment.S3.flush/2` waits for
  everything so far, and `durability: :sync` makes every commit wait. Commits
  lost before upload (a fenced writer, a close that couldn't reach S3) make
  `flush/2` and `sync: true` fail until `Sediment.S3.acknowledge_loss/1`.
* Opening restores from S3, verifying the log's CRC chain and the snapshot
  checksums. A lease allows one writer at a time, fenced by the store's
  conditional writes; every writer probes the provider for them at open.
  Every upload carries `x-amz-checksum-sha256`.
* Checkpoints upload incremental snapshots in the background, off turso's
  checkpoint lock: the changed 64 KiB segments, zstd-compressed, multipart
  above 8 MiB, with a full snapshot again after 16 links. Old objects are
  garbage-collected in batches.
* Read-only replicas (`mode: :replica`, `Sediment.S3.refresh/1`; a pool
  shares one working copy and the last snapshot is cached), point-in-time
  restore (`retain_epochs`, `Sediment.S3.restore/3`), group commit, and
  encryption at rest (`:encryption` encrypts the bucket's objects too).
* Scripts, `mix run` and Mix tasks upload pending async commits before the
  VM exits. Opens are serialized per file, not VM-wide. Hard links to an S3
  database file are not supported.
* `Sediment.S3.request_counts/0` counts S3 requests by operation and price
  class. With `SEDIMENT_S3_METER_DIR`, requests are logged per process and
  refused while a `STOP` file exists (a budget guard for paid providers).
* Verified by a TLA+ model of the protocol (`formal/`: safety, liveness and
  traces of real runs), fault-injection tests and a crash-torture test
  (`mix test --only torture`). Tested on SeaweedFS, MinIO and Tigris.

### Robustness

* A panic inside turso_core fails the statement and closes that connection;
  the VM keeps running. A connection closed under a client timeout, a fenced
  S3 writer or a dead owner process makes the pool reconnect.
* Every call into turso_core runs on a 16 MB per-thread stack, so deep
  expressions and long trigger chains can't overflow a dirty scheduler's
  stack. NIF calls that do I/O run on dirty schedulers.
* Busy-lock waits sleep in the calling process, not on a dirty IO scheduler
  thread, so writers waiting for the lock can't starve its holder or the
  VM's file I/O.
* A turso_core 0.8.1 bug could commit part of a `BEGIN CONCURRENT`
  transaction when an `AUTOINCREMENT` insert got busy: such an insert fails
  as busy, and the transaction must be rolled back.
* Switching an existing database with `AUTOINCREMENT` tables to MVCC (by
  `journal_mode: :mvcc` or the pragma) is refused: turso_core 0.8.1 would
  reuse their ids and silently overwrite rows.
* A file that is already open in the VM can only be opened again with the
  same encryption choice (the same key, or none): other opens used to share
  its decrypted pages.

### Performance

* `Sediment.Connection` runs a statement in one native call (bind, step
  to completion, columns, changes, transaction status); pooled queries are
  faster than exqlite's (see `bench/RESULTS.md`).
* Per-connection prepared-statement cache (64 statements; PRAGMAs are not
  cached).
