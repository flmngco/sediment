# Changelog

## Unreleased

### Fixed

* A `cancel/1` that landed before its query's native call started (waiting
  for a dirty scheduler under load) was lost: every call cleared it. A
  pool's disconnect after a client timeout could then leave an endless
  query running and the connection stuck in close. Now a cancel applies to
  every operation started on the connection before it, until that
  operation ends (its native calls still queued, between busy waits,
  between `fetch_all/3` chunks), and to none started after it; turso's
  interrupt is sent while the operation runs, repeated until its statement
  sees it. A pool's disconnect stops the running query, and makes every
  later one give up, on a normal scheduler before closing, so it also works
  while every dirty scheduler runs a long query.
* `cancel/1` with nothing running no longer leaves turso's interrupt set
  for later statements while another statement is open.
* `Sediment.S3.snapshot/1`, `info/1`, `refresh/1`, `flush/2` and
  `acknowledge_loss/1` given a process that isn't a `DBConnection` pool
  (such as an Ecto repo's pid) waited for the 15 s checkout timeout. They
  now raise `ArgumentError` at once, for an Ecto repo with a pointer to the
  adapter's functions or `Ecto.Adapter.lookup_meta(repo).pid`.

## 0.1.0-beta.3 (2026-10-06)

### Fixed

* Opening a WAL database file (or creating one) next to another
  database's MVCC log led with turso's "Corrupt database: ... The database
  may be corrupted" and only then said whose log it was. The error now
  leads with the shared log and keeps turso's text after it.
* `Sediment.S3.destroy/2` right after the database's pool stopped failed
  with "open in this VM" for a moment: connections of a pool whose
  processes died close on a thread of their own. Destroy now waits for
  connections that are closing (up to their `:close_timeout_ms` and a
  second) and refuses only one that is still open.

## 0.1.0-beta.2 (2026-10-05)

### S3 durability

* `Sediment.S3.destroy/2` deletes a database from S3 (tenant deletion,
  erasure requests): it takes the writer lease (refusing a running writer
  unless `force: true`), replaces the manifest with a marker that there is
  no database, then deletes the snapshots and the log. A destroy that stops
  half way never leaves an older state of the database behind; opening the
  location again creates a new, empty database. 0.1.0-beta.1 refuses to
  open a destroyed prefix.
* `Sediment.S3.exists?/1` tells whether a location holds a database, and
  the `must_exist: true` S3 option makes a writer open fail, writing
  nothing, instead of creating an empty database where there is none.
* Replica connections no longer serve a destroyed database from the
  generation they share in the VM: a refresh or a new connection reuses
  another connection's restored state only if the location still holds the
  same database (manifests now carry a `database_id`), and otherwise
  restores (finding the new database, or none).
* Garbage collection never deletes an epoch of a newer writer lease
  generation, and a new lease starts above the manifest's generation (a lost
  or restored `lease.json` doesn't make opens fail).
* An open, import or destroy that finds the prefix written under a newer
  writer lease than its own fails as fenced instead of building on it.
* Local I/O errors of S3 opens, restores, imports and replica refreshes
  name the database file (`"s3 local io: /data/t1.db: No such file or
  directory"`).

### Fixed

* Distinct database files could share one MVCC log: turso_core names it
  after the file without its extension, so `app.1` and `app.2` both used
  `app.db-log`, replaying and truncating each other's commits, and an S3
  bootstrap of one deleted the other's log. MVCC and S3 opens, switches to
  MVCC, S3 restores, imports and exports now refuse a database file whose
  log another database file in its directory (or one open in the VM) maps
  to, and say which. An existing MVCC database is only refused for another
  MVCC file (not for an export or backup next to it). Log names are
  compared ignoring case, and a symlinked database is checked next to the
  file it points to.
* `experimental: [:attach]` can't be combined with MVCC: such a database
  can't be or become MVCC, and attaching an MVCC database fails (turso
  names an attached database's log the same way).
* An S3 writer's or `Sediment.S3.restore/3`'s local path can't be a
  symlink: the restore replaced the link with a file, leaving the file it
  pointed to as a second database on the same log.
* Opening a database that another OS process has open failed with only
  turso's "Failed locking file ... File is locked by another process". The
  error now says the database is open in another OS process (turso_core
  allows one process per database file).

## 0.1.0-beta.1 (2026-10-04)

First release.

### Installation

* Precompiled NIFs (RustlerPrecompiled) for Linux gnu (glibc 2.28+) and
  musl (x86_64, aarch64), macOS (aarch64, x86_64) and Windows (x86_64),
  checked against the checksums in the Hex package. Rust is only needed for
  `SEDIMENT_BUILD=1` source builds, other targets, and git or path
  dependencies, which always build from source.

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
