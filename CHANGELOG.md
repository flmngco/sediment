# Changelog

## 0.2.0-beta.2 (Unreleased)

### Breaking

* `rekey_many/2` and `release_key_many/3` no longer call `:around` for
  every target: a target the pre-check finds done (see Added) is reported
  without it. A re-run therefore doesn't reopen a database an earlier run
  left closed (failed with `{:around_failed, ...}` or `{:crashed, ...}`):
  reopen those from `:on_result`, or after the run.

* `Sediment.S3.restore/3` refuses what it used to ignore: an option it
  doesn't know (`target: ...` restored the latest state), an `:at` that
  isn't a `DateTime`, and an `:epoch` that isn't an integer from 0 to
  2^64 - 1 or an epoch as results show it. Each returns `{:error, reason}`
  and restores nothing. The native restore refuses any target other than
  the latest state, a time or an epoch, where it restored the latest state.
* `Sediment.export_sqlite/3` refuses an option it doesn't read, naming it
  (never its value), and writes nothing: `at:` or `epoch:` exported the
  latest state. For a point in time, restore to a file, then export it.
* `Sediment.S3.release_key/4` returns `{:error, {:leftover, %{epochs:
  [...], objects: n, message: ...}}}` when deleting the released epochs'
  objects fails, where it returned `{:ok, _}`. The release did happen (the
  manifest no longer retains the epochs), but their objects, encrypted
  under the key, were left in the store with only a log warning. A re-run
  for the key deletes what is left and returns `{:ok, _}` only once none
  is; `release_key_many/3` retries it, and reports the database released.

### Added

* `restore/3` takes `epoch: "SEQ-GENERATION"`, the epoch as results show
  it (`info.epoch`): the generation must match the retained epoch's too.
  `epoch_start: n` restores an epoch's snapshot alone, the state at its
  start (a forced snapshot starts an epoch, so it is a restore point).
  The docs say plainly what `epoch: n` restores: the whole epoch, which is
  the state as the next epoch began (or the live state, for the current
  epoch).
* `key_census/1` returns `fingerprints: %{id => fingerprint}` (16 hex
  digits) and `ciphers: %{id => cipher}` for every key it lists, and
  `key_census_many/2` keeps them by target. `Sediment.S3.check_ring(census,
  ring)` checks that a ring holds the recorded key material and cipher
  under each id (`:ok`, or `{:error, [{id,
  :missing | :mismatch}]}`), without opening: a boot-time check.
* `rekey_many/2` and `release_key_many/3` read each target's manifest
  before `:around` (without the lease): a database under the target key
  already, with the ring's key under that id the recorded one, is
  reported `current` without calling `:around`, so a re-run pauses only
  the databases that need it; a release with no epoch under the key and
  nothing left is reported `unchanged` likewise. A ring that can't open
  the database fails the target before `:around`, not retried: one an open
  would refuse, or one holding under a recorded id a key of another cipher
  (the message names both ciphers) or other key material (both
  fingerprints). The encryption function is
  called once per target, before `:around`.
* `Sediment.S3.space_needed(path, s3_opts, :rekey | :release)`: the bytes
  a rekey or release needs free beside `path` (the restore unless the
  working copy is warm, the rekey's copy, a margin). Both check it before
  they download anything and refuse with a message naming the bytes needed
  and free, where a rekey checked only for its copy, after the download had
  filled the volume. The check counts the rekeys and releases running in
  the VM (each reserves its need until its restore and copy are done), and
  the restore's peak for a chain of deltas (a rebuilt image and the staged
  delta at once). Temporary files of a download a killed VM cut short are
  removed under the lease first.
* `keep_working_copy: false` for `rekey/3`, `release_key/4` and their
  `*_many` variants removes the working copy (the file, its WAL and log,
  the warm sidecar) after a run that returned `{:ok, _}`: an offline run
  through a scratch directory has no use for it.
* `Sediment.S3.orphans(s3_opts)` lists, without the lease, the objects a
  writer's garbage collection would delete (`objects`, `bytes`, `by_key`),
  with the collection's own rule: never a live writer's next snapshot or
  log, nor a delta's base snapshots.

### Fixed

* An open, rekey, release or other operation on an S3 database removes the
  snapshot images a crashed writer left beside its path only once it holds
  the lease. Before, one refused with "lease held" (a second process on the
  same path) had removed the images of the live writer's pending snapshot.
* A query its timeout stopped sometimes failed with "connection_closed"
  (when the disconnect closed the connection before the statement saw the
  interrupt) instead of "interrupted", as `Sediment.Connection` documents.
  It is "interrupted" either way. Only the call that runs the statement
  reports it: a close after the statement or a COMMIT completed (while
  its status or columns were read) is still "connection_closed", as the
  write may have committed.
* A rekey killed while running left its copy, `.<name>.rekey.db` (the
  database's size), until the next rekey. The next open, rekey or release
  of the database removes it, under the file's lock and the lease, and so
  does a restore into that path.

## 0.2.0-beta.1 (2026-10-24)

0.2 breaks 0.1: the S3 manifest is version 4 (it records each epoch's key),
`:encryption` is a key ring, and the options of functions taking keys must
be keyword lists (see below). Data written by 0.1 is not migrated: an S3
prefix written by 0.1 is refused (read its data with 0.1 and export it, or
destroy the prefix).

### Breaking

* The S3 manifest is version 4: it records the key of every epoch. Sediment
  refuses manifests of versions 1 and 2, written by 0.1.0-beta.6 and older,
  with a message saying so (read the data with the older version and
  export it, or destroy the prefix). There is no production data anywhere,
  so no migration is provided. The tombstone a destroy leaves is unchanged.
* `:encryption` is a key ring. `[cipher: c, key: k]` still works, as a ring
  of one key whose id is `"default"`; a database created with it records
  that id. New forms: `[keys: [{"r2", k2}, {"r1", k1}], cipher: c]` (the
  first key is active, an entry may give its own `cipher:`) and
  `[key_for: fun | {m, f, a}, active: "r2"]`. Ids match
  `[a-z0-9][a-z0-9_-]{0,31}` and are not 32 hex digits (an AES-128 key
  would pass as one); a ring can't hold an id or a key twice.
* An open with a wrong key is refused by the key's fingerprint before the
  takeover (`"key default in the ring is not the key that encrypted this
  database ..."`), instead of failing to decrypt after the download.
* The warm-reopen sidecar is version 3 (it records the key's fingerprint):
  the first open after the upgrade restores in full.
* `Sediment.Native` changed for key rings (`s3_refresh/2`, `s3_restore/4`,
  `s3_import/5`, `export_sqlite/5` take a ring).
* The options of every function taking keys or S3 options
  (`Sediment.Engine.open/2`, `Sediment.start_link/1`, `child_spec/1`,
  `export_sqlite/3`, `transaction/3`, `Sediment.Connection.connect/1`,
  `Sediment.S3`'s `refresh/2`, `restore/3`, `import/3`, `rekey/3`,
  `release_key/4`, `destroy/2`) must be a keyword list, without a key
  ring's options (`key:`, `keys:`, `key_for:`, `cipher:`, `active:`) at the
  top level. A map, which some of them accepted, or a ring given as the
  options, is refused with a fixed message; a bare ring given to
  `Sediment.Engine.open/2` used to open an unencrypted database.

### S3 durability

* Key rotation inside one prefix. Each epoch is encrypted with one key,
  which the manifest records with it (id, cipher, and a fingerprint: the
  first 16 bytes of HMAC-SHA256(key, "sediment/key-fingerprint/v1")). An
  open resolves the key from its ring under the writer lease, before
  writing anything, and refuses an unknown id or another key under the id
  by name. Errors never contain key material.
* `Sediment.S3.rekey/3` re-encrypts a database page by page under another
  key of the ring and publishes it as a new epoch (always a full snapshot),
  with `If-Match` on its takeover: a writer that took over meanwhile gets it
  fenced (`{:error, {:fenced, %{current_key: ...}}}`), keeping every
  commit, and a crash before the publication leaves the old key current.
  Only `{:ok, _}` is an outcome: after any error, run the rekey again, and
  keep both keys in the rings until it succeeds. It keeps the epochs it ended (pinned, whatever
  `retain_epochs` is) and retires the old key: a rekey back to it, by id or
  by key under any id, needs `allow_reuse: true` and its epochs released
  first. Like `destroy/2`, it refuses while the database is open in this
  VM; it also refuses while its local file is (without `:s3`, or with
  another prefix). Stop the pool first: opens of the path wait until the
  rekey ends.
* `Sediment.S3.release_key/4` drops the retained epochs under a key and
  deletes their objects; it refuses the current key.
  `Sediment.S3.key_census/1` reads which keys a database still depends on.
* Point-in-time restore uses the key of the epoch it restores, and only an
  epoch whose time range covers the moment asked for: a moment in the time
  of released epochs is refused, never answered with an older state. Epoch
  boundaries come from the store's clock (the Last-Modified of the seal that
  closed the previous epoch), not a writer's.
* An open whose epoch-advance manifest PUT got an error reads the manifest
  back: it adopts its own write if it landed, and sends it again while the
  manifest is still the one it took over, so a write reported as failed
  never lands later. When that can't be told, the error says the outcome is
  unknown (`{:error, {:indeterminate, message}}` from `rekey/3`).
* Replicas and `Sediment.S3.refresh/2` follow a rekey at the next refresh
  when their ring holds the new key; `refresh/2` takes `encryption:` for a
  ring with `key_for`.
* Rotation over many databases: `Sediment.S3.rekey_many/2`,
  `release_key_many/3` and `key_census_many/2` run the operation over a list
  of `{path, s3_opts}` or `%{id:, path:, s3:}` targets, a few at a time
  (`max_concurrency`, 1 by default; 2 for the census), never stopping at a
  failure, and report every target by id (`rekeyed`, `current`, `absent`,
  `failed`, `skipped`; the census by key, with `removable?/3`, false while
  any target is absent or unreadable: a wrong bucket or prefix reads as
  absent; `current: new_id` also needs every target under the new key). A
  result that may still change (fenced, indeterminate, a store error) is
  retried (`attempts`, `backoff`; a retried failure reports every
  attempt's reason); a refusal isn't. `around:` wraps each target's
  operation (close a tenant's pool before, reopen it after), `check:`
  refuses a target, `on_result:` reports progress, `stop_on:` stops
  starting targets; `encryption:` is a ring or a function of the target. A
  re-run finds the done ones `current`. The callbacks get each target with
  its `:encryption` hidden (still usable to reopen it), and their reasons
  and re-raised exceptions pass the key net. An `around:` failing after the
  operation fails the target with `{:around_failed, reason, result}`. Each
  target runs in a monitored, unlinked process trapping exits: a linked
  process exiting fails the target with `{:linked_exit, result}` once its
  operation ends; one killed fails it with `{:crashed, why}` and stops
  further starts, as its native operation may still be running.

### Fixed

* A mistyped `:encryption` never shows a key: an entry given as
  `{key, id}`, a key given as `active:`, `to:`, a release's key id or
  `cipher:`, an entry of the wrong shape, a `key_for` that raises. Errors
  name ring entries by position; they never show a refused id or cipher,
  nor a value of the wrong type; an id that could be a key is shown as
  `<an id that looks like a key>`; a `key_for` that fails is reported by
  the id and the exception's type only, without stack frame arguments.
* Arguments of any shape never show a key either. Every public function
  of `Sediment`, `Sediment.Engine`, `Sediment.S3`, `Sediment.Basic`,
  `Sediment.Pragma`, `Sediment.CDC` and `Sediment.Connection.connect/1`
  runs inside one boundary, from its argument handling to its NIF calls:
  * an exception raised there loses its stack frames' arguments, and one
    that isn't Sediment's own (a `FunctionClauseError` on a map of options,
    a `KeyError`, a `MatchError`) is replaced by an `ArgumentError` naming
    its type only; Sediment's errors and DBConnection's keep their type, and
    the `ArgumentError`s and `RuntimeError`s of Sediment's and Ecto's
    modules their message (an Ecto repo's configuration error says why);
  * a net: every error Sediment returns or raises, and every exit reason
    passing through, has each run of 32 or more hex digits replaced by
    `<redacted>` (keys are hex), in Elixir and in the native code, so a key
    echoed by any message never shows. Fingerprints in messages and in a
    fenced result are shown by their first 16 hex digits. Only text is
    rewritten: a connection state, query or result in an error tuple is
    returned as it is (of `{:error | :disconnect, exception, state}` only
    the exception), and a `Sediment.Error` keeps its `:statement` as given,
    its message and `inspect` showing it redacted. Values of the caller's
    holding such a run (a prefix or path with a dashless UUID) show
    redacted in messages too;
  * messages show a value a caller gave only as an atom, string or number
    that couldn't be a key, otherwise by its type (`"unsupported type:
    %ArgumentError{...}"`, `"argument error: a value of type map"`);
  * an S3 option that doesn't parse is named, never shown (it was echoed:
    `encryption: "<key>"` inside `:s3`), and so is an unknown option, mode
    or journal mode that could be a key.
  * a statement that isn't text (options or a ring given in its place) is
    refused with an error before a telemetry span names it, and
    `Sediment.S3.flush/2` checks its timeout before its span starts:
    telemetry handlers never see such a value (a span's exception event
    carries the stack trace with its arguments);
  * the options DBConnection reads are type-checked with fixed messages:
    `Sediment.transaction/3`'s (`:timeout`, `:pool`, `:queue`, `:log`, ...)
    and a pool's (`:pool_size`, `:pool`, `:queue_target`, `:backoff_type`,
    ..., at `Sediment.start_link/1` and `child_spec/1`; a bad one failed in
    the pool's process, with the value in its crash report).
  `Sediment.transaction/3` runs DBConnection's setup and completion inside
  the boundary and its function outside it: the function's results,
  rollback reasons, exceptions, exits and throws reach the caller as they
  are.
* A transaction begun on a transaction the database rolled back (a
  write-write conflict, `ON CONFLICT ROLLBACK`) and nobody ended:
  * a savepoint (Ecto's SQL sandbox, in a test whose transaction the
    database had rolled back) is refused with `{:error, %Sediment.Error{},
    state}` ("the transaction was rolled back by the database ..."), the
    connection kept, where the sandbox crashed on the `{:error, state}`
    status;
  * a new transaction (on a pooled connection checked in after a `BEGIN`
    run as a query was aborted) ends it and begins, where DBConnection
    disconnected.
* With a `key_for` ring, every open and every rekey of a warm copy
  (a clean close's) downloaded the whole database: the first round, which
  asks `key_for` for the key, deleted the warm sidecar before it stopped.
  The sidecar is now read first and deleted only once the open goes ahead,
  so a `key_for` round, a rekey to the current key (`{:ok, :current}`) or
  a release with nothing to drop keep the copy warm.
* A pool (`Sediment.start_link/1`, `Sediment.child_spec/1`) and a
  connection's state keep the `:encryption` option (and a `:key`, which
  connections refuse) hidden: crash and
  termination reports, `:sys.get_status/1` and `inspect` show
  `#Sediment.S3.Keys.Hidden<redacted>`, never a key.
* `Sediment.S3.restore/3`, `import/3`, `refresh/2`,
  `Sediment.export_sqlite/3` and `Sediment.Engine.deserialize/3` ran turso on the dirty
  scheduler's small stack: a schema with a deep expression (a `CHECK` of
  90 chained additions) could overflow it and kill the VM. They now run
  on turso's own stack, like opens and statements, and a turso panic is
  returned as an error. `rekey/3` and `release_key/4` do the same.
* `Sediment.Engine.open(path, mode: :readonly)` of an MVCC database with
  rows in an `AUTOINCREMENT` table (a restored S3 copy, say) failed with
  "attempt to write a readonly database": turso_core 0.8.3 rewrites each
  such table's sequence when it opens the file. Such a file is now opened
  read-write (never created) with `query_only`; the open needs write access
  to the file and its log, and user data isn't changed.
* `Sediment.S3.import/3` gave the rows of a table with an inline
  `INTEGER PRIMARY KEY DESC` column new rowids: that column doesn't alias
  the rowid, and the copy treated it as if it did. Every table's rowids are
  now copied, and the import compares each table of the new database with
  the source's (every row with its rowid) and refuses the import, naming
  the tables, if one differs. A table with columns named `rowid`,
  `_rowid_` and `oid` is refused.
* `Sediment.S3.import/3` checks the new database's `AUTOINCREMENT`
  sequences against the source's `sqlite_sequence` after the copy, and
  refuses the import, naming the tables, if one is missing or behind.
  sediment 0.1.0-beta.5 (turso_core 0.8.2) imported a sequence standing at
  exactly 1 as never used, so the next insert reused id 1 and replaced the
  row that had it; 0.1.0-beta.6 imports it correctly, and the check refuses
  any future engine regression of that kind.

## 0.1.0-beta.6 (2026-10-21)

### Engine

* turso_core 0.8.3 (was 0.8.1). It brings upstream's MVCC fixes from 0.8.2
  and 0.8.3, among them: the logical log's header upgrade is published only
  after its write succeeds, late commits survive a TRUNCATE checkpoint,
  B-tree deletion and conflict checks are atomic, and a retried `nextval`
  inside `BEGIN CONCURRENT` no longer turns the COMMIT into a no-op. Every
  workaround stays in place: the engine bugs they guard against still
  reproduce on 0.8.3.

### Fixed

* Stopping a pool (`Supervisor.stop/3` on an Ecto repo, `GenServer.stop/1`
  on a `Sediment.start_link/1` pool) with `db_connection` 2.10.1 returned
  before its connections had closed: the S3 lease was released and the
  warm-reopen file written a moment later, so a stop followed by
  `System.halt/1` reopened cold. sediment now requires `db_connection`
  2.10.2 or later, whose pool waits for its connections to close.

## 0.1.0-beta.5 (2026-10-08)

### S3 durability

* Warm reopen: an open whose working copy is still the one a clean close of
  the same database left skips the snapshot download and fetches only the
  log written since (a 100 MB reopen took 220 ms instead of 690 ms on a
  local SeaweedFS). Every doubt (another writer, a changed file, a copy
  ahead of S3, a crash instead of a close) falls back to the full restore.

### Fixed

* An S3 database whose working copy was removed right after its pool
  stopped could, rarely, reopen as an empty database outside MVCC mode
  ("s3 writer fenced: the database left MVCC journal mode ..."): a
  connection of the stopped pool that was still opening created a new file
  in its place. S3 opens no longer create the file in turso's open (the S3
  restore does), and a database whose connections have all closed is
  never handed to a new open: the open waits for its close to finish
  (including the lease release and what a warm reopen needs). An open
  counts as a connection from its start, so one that overlaps the last
  close keeps the database open.
  Closing the last connection now also waits for the snapshot its
  checkpoint queued (within `:close_timeout_ms`), so a reopen right after a
  close restores instead of waiting on that upload.

### Docs

* `Sediment.Connection` and `Sediment.Engine.cancel/1`: a query stopped
  after a DBConnection client timeout returns the same `"interrupted"`
  error as any other interrupt, and how to tell timeouts apart.

## 0.1.0-beta.4 (2026-10-07)

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
