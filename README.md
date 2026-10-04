# Sediment

SQLite-compatible database for Elixir on the Turso engine, with optional
S3-backed durability.

**Status: experimental.** Expect rough edges; see [Naming](#naming) and the
disclaimer below.

Sediment is an Elixir driver for [Turso](https://github.com/tursodatabase/turso),
the SQLite-compatible database engine written in Rust.

`sediment` is to Turso what [`exqlite`][exqlite] is to SQLite: a NIF
(built with [Rustler][rustler] over `turso_core` 0.8.1) plus a
[`DBConnection`][db_connection] implementation. The public API mirrors
exqlite's module by module, so code written against exqlite ports over by
renaming modules:

| exqlite                | sediment               |
| ---------------------- | -------------------------- |
| `Exqlite`              | `Sediment`              |
| `Exqlite.Sqlite3`      | `Sediment.Engine`        |
| `Exqlite.Sqlite3NIF`   | `Sediment.Native`       |
| `Exqlite.Connection`   | `Sediment.Connection`   |
| `Exqlite.Query`        | `Sediment.Query`        |
| `Exqlite.Result`       | `Sediment.Result`       |
| `Exqlite.Error`        | `Sediment.Error`        |
| `Exqlite.Stream`       | `Sediment.Stream`       |
| `Exqlite.Pragma`       | `Sediment.Pragma`       |
| `Exqlite.TypeExtension`| `Sediment.TypeExtension`|
| `Exqlite.Basic`        | `Sediment.Basic`        |

It also exposes Turso-only features: MVCC with `BEGIN CONCURRENT`, page
encryption, vector search, full-text search, change data capture and S3-backed
durability.

If you are looking for the Ecto adapter, see
[`ecto_sediment`](https://github.com/flmngco/ecto_sediment), the
Sediment equivalent of `ecto_sqlite3`.

New to it? Start with the [getting started guide](guides/getting_started.md).

## Naming

You might expect a library like this to be called `turso_ex` or `ecto_turso`. Elixir integrations are often named after what they wrap, and that makes them easy to find. We deliberately didn't do that.

**We love Turso.** The Turso database engine is the foundation of this library. It's an impressive, open-source (MIT) reimplementation of SQLite in Rust, and everything Sediment does stands on that work. But "Turso" is the name of a company and its products, and we don't want to ride on the coattails of their good name:

- **This is not an official Turso project,** and we don't want anyone to assume it is. Sediment is still **experimental**. Bugs, rough edges and integration mistakes here are ours, not Turso's, and they shouldn't reflect on the quality of the Turso engine. Please report them to us, not to Turso.
- **Sediment takes a different path from Turso's own offering.** It uses the open-source engine and adds its own durability layer that stores your database in an S3-compatible bucket you control. Turso Cloud, the company's hosted database, **doesn't work with this library right now.** Naming the library after Turso would suggest otherwise.

So the name is our own. **Sediment** describes how the S3 layer works: committed changes settle into the bucket in layers, as log segments that get compacted into snapshots, and together they make up the solid ground your data rests on.

"Turso" appears in these docs only to describe what Sediment is built on: the engine, its compatibility and its behaviour. See also the disclaimer below: Sediment isn't affiliated with Turso.

> **Not affiliated with Turso.** Sediment is an independent, community-maintained Elixir library built on the open-source Turso database engine (`turso_core`, MIT). "Turso" is a trademark of its respective owner and is used here only to describe compatibility. This project is not endorsed by, sponsored by, or otherwise connected with Turso or its company.
>
> Provided under the MIT License, without warranty of any kind. The underlying engine is pre-1.0; keep independent backups of any data you care about.

## Installation

```elixir
defp deps do
  [
    {:sediment, "~> 0.1"}
  ]
end
```

With Ecto, depend on the adapter instead; it brings `sediment` with it:

```elixir
{:ecto_sediment, "~> 0.1"}
```

Elixir 1.18 or later; the package is tested on Elixir 1.18 with OTP 27 and
Elixir 1.20 with OTP 29, on Linux.

The NIF comes precompiled (via [RustlerPrecompiled][rustler_precompiled])
for these targets, so no Rust toolchain is needed:

| OS | Targets |
|---|---|
| Linux, glibc 2.28+ | `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` |
| Linux, musl (Alpine) | `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` |
| macOS | `aarch64-apple-darwin`, `x86_64-apple-darwin` |
| Windows | `x86_64-pc-windows-msvc` |

The download is checked against the checksums in the Hex package. glibc
2.28 covers Debian 10+, Ubuntu 20.04+, RHEL/Rocky/Alma 8+ and Amazon Linux
2023, but not Amazon Linux 2 (glibc 2.26). On Alpine the NIF links
`libgcc_s`: install `libgcc` if your image doesn't have it.
The test suites run on Linux only; the other targets are built in the
release, not tested.

To build from source instead (any other target, or by policy), set
`SEDIMENT_BUILD=1` when compiling and install a Rust toolchain (1.91 or
later). A git or path dependency on sediment always builds from source:
the precompiled NIFs are only used from the Hex package, which carries
their checksums.

## Caveats

* `Sediment.Engine` does not cache prepared statements;
  `Sediment.Connection` keeps each connection's 64 most recently used
  ones.
* Prepared statements are not immutable. Do not manipulate a statement
  concurrently; keep it isolated to one process.
* Native calls that touch the database run on dirty IO schedulers.
  `interrupt/1` and `cancel/1` only set a flag and run on normal
  schedulers, so they are never queued behind busy dirty schedulers.
* Datetimes are stored without offsets, as ISO 8601 text, like exqlite.
* When storing `BLOB` values, use `{:blob, the_binary}`, otherwise the value
  is stored as text. Binaries that are not valid UTF-8 are stored as blobs,
  because Turso text must be UTF-8.

## Configuration

### Runtime configuration

```elixir
config :sediment,
  default_chunk_size: 100,
  type_extensions: [MyApp.TypeExtension]
```

* `default_chunk_size` - The chunk size used when multi-stepping without an
  explicit chunk size.
* `type_extensions` - An optional list of modules that implement the
  `Sediment.TypeExtension` behaviour.

### Compile-time configuration

Full-text search pulls in [tantivy](https://github.com/quickwit-oss/tantivy)
and adds a few minutes to the first build. It is on by default; to build
without it:

```elixir
config :sediment, Sediment.Native, default_features: false
```

## Usage

The `Sediment.Engine` module works like `Exqlite.Sqlite3`:

```elixir
# We'll just keep it in memory right now
{:ok, conn} = Sediment.Engine.open(":memory:")

# Create the table
:ok = Sediment.Engine.execute(conn, "create table test (id integer primary key, stuff text)")

# Prepare a statement
{:ok, statement} = Sediment.Engine.prepare(conn, "insert into test (stuff) values (?1)")
:ok = Sediment.Engine.bind(statement, ["Hello world"])

# Step is used to run statements
:done = Sediment.Engine.step(conn, statement)

# Prepare a select statement
{:ok, statement} = Sediment.Engine.prepare(conn, "select id, stuff from test")

# Get the results
{:row, [1, "Hello world"]} = Sediment.Engine.step(conn, statement)

# No more results
:done = Sediment.Engine.step(conn, statement)

# Release the statement.
:ok = Sediment.Engine.release(conn, statement)
```

With `DBConnection` (pooling, transactions, streams):

```elixir
{:ok, conn} = Sediment.start_link(database: "app.db", journal_mode: :wal)

Sediment.query!(conn, "create table users (id integer primary key, name text)")
Sediment.query!(conn, "insert into users (name) values (?)", ["Alice"])

{:ok, %Sediment.Result{rows: [[1, "Alice"]]}} =
  Sediment.query(conn, "select id, name from users")

Sediment.transaction(conn, fn conn ->
  Sediment.query!(conn, "update users set name = ? where id = ?", ["Bob", 1])
end)
```

See `Sediment.Connection.connect/1` for all connection options.

## Turso features

See the [Turso extensions guide](guides/turso_extensions.md) for details and
examples.

* **MVCC and `BEGIN CONCURRENT`** - `journal_mode: :mvcc` lets several
  connections write in concurrent transactions;
  `default_transaction_mode: :concurrent` makes transactions use
  `BEGIN CONCURRENT`. Choose it when creating the database: an existing
  database with `AUTOINCREMENT` tables refuses to switch (see
  [Differences from exqlite](#differences-from-exqlite)).
* **Encryption** - `encryption: [cipher: "aegis256", key: hex_key]`.
* **Vector search** - built-in vector functions, and `Sediment.Vector`
  to encode Elixir lists as vector blobs.
* **Full-text search** - `CREATE INDEX ... USING fts` with
  `experimental: [:index_method]`, queried with `fts_match` and `fts_score`.
* **Change data capture** - `Sediment.CDC` reads the changes Turso
  records per connection.
* **S3 durability** - the database's state of record lives in S3: commits
  are uploaded in the background (async by default, `sync: true` or
  `durability: :sync` to wait), and a single writer is fenced by the store's
  conditional writes; also read-only replicas, point-in-time restore and
  encryption at rest. See the [S3 durability guide](guides/s3.md); the
  protocol is model-checked with TLA+ ([formal model](formal/README.md)).
  S3 databases are encrypted by default (`:encryption` with a key, or
  `encryption: false` to opt out explicitly).
  Opening with `s3:` restores the S3 copy over the local file; an empty
  prefix refuses to start over an existing local database, which
  `Sediment.S3.import/3` uploads instead.
* **Experimental flags** - `experimental: [:attach, :views, ...]`.

## Telemetry

`Sediment.Telemetry` lists the events: spans around preparing and
executing statements (`[:sediment, :prepare | :query, ...]`), pooled
connection disconnects with their reason (S3 fencing, a contained
turso_core panic, a closed handle), and spans for the S3 operations. For
connect and disconnect counts, start the pool with a
`DBConnection.TelemetryListener` in `:connection_listeners`.

## Differences from exqlite

### API parity

Every public function of exqlite, per module ("unsupported" functions exist
and return `{:error, :not_supported}`; "no" means the function isn't
provided; `Exqlite.Sqlite3NIF` and `Exqlite.Flags` are internal modules
whose design differs). The [exqlite API parity guide](guides/exqlite_parity.md) lists
each function with the reason for every difference.

| exqlite module | sediment | Functions present | Unsupported or absent |
| --- | --- | --- | --- |
| `Exqlite` | `Sediment` | 27/27 | none |
| `Exqlite.Basic` | `Sediment.Basic` | 8/8 | `disable_load_extension/1` (unsupported), `enable_load_extension/1` (unsupported), `load_extension/2` (unsupported) |
| `Exqlite.Connection` | `Sediment.Connection` | 15/15 | none |
| `Exqlite.Error` | `Sediment.Error` | 1/1 | none |
| `Exqlite.Flags` | none | 0/2 | `put_file_open_flags/1` (internal), `put_file_open_flags/2` (internal) |
| `Exqlite.Pragma` | `Sediment.Pragma` | 12/12 | none |
| `Exqlite.Query` | `Sediment.Query` | 1/1 | none |
| `Exqlite.Result` | `Sediment.Result` | 1/1 | none |
| `Exqlite.Sqlite3` | `Sediment.Engine` | 36/36 | `enable_load_extension/2` (unsupported), `set_authorizer/2` (unsupported), `set_log_hook/1` (unsupported), `set_update_hook/2` (unsupported) |
| `Exqlite.Sqlite3NIF` | `Sediment.Native` | 20/33 | `bind_blob/3` (no), `bind_float/3` (no), `bind_integer/3` (no), `bind_null/2` (no), `bind_text/3` (no), `enable_load_extension/2` (no), `erlang_allocator_enabled/0` (no), `errmsg/1` (no), `errstr/1` (no), `load_nif/0` (no), `set_authorizer/2` (no), `set_log_hook/1` (no), `set_update_hook/2` (no) |

Turso is SQLite compatible at the file format and SQL level, but it is a
different engine. Where Turso cannot behave like SQLite, sediment says so
instead of faking it.

### Not supported

| exqlite                                    | sediment                                 |
| ------------------------------------------ | -------------------------------------------- |
| `Sqlite3.set_update_hook/2`                | returns `{:error, :not_supported}`; use `Sediment.CDC` |
| `Sqlite3.set_authorizer/2`                 | returns `{:error, :not_supported}`           |
| `Sqlite3.set_log_hook/1`                   | returns `{:error, :not_supported}`           |
| `Sqlite3.enable_load_extension/2`          | returns `{:error, :not_supported}`           |
| `Basic.load_extension/2`                   | returns `{:error, :not_supported}`           |
| `Sqlite3NIF.errmsg/1`, `errstr/1`          | not provided; errors are returned directly   |
| connect options `:load_extensions`, `:authorizer` (non-empty) | `connect/1` fails with an error |
| connect option `:key` (SQLCipher)          | `connect/1` fails; use `:encryption`         |
| `fts3`, `fts4`, `fts5` virtual tables      | `no such module`; use Turso FTS indexes      |
| Numeric named parameters (`:42`)           | parse error; use `?NNN` or named parameters with letters |

### Behaves differently

* **MVCC checkpoint threshold.** Non-S3 databases in MVCC mode checkpoint
  automatically every 256 KiB of logical log (`:mvcc_checkpoint_threshold`;
  `nil` for turso_core's ~4 MB default). The default protects
  `AUTOINCREMENT` tables (Ecto's default primary keys), whose inserts slow
  down as the log grows: with a 1 MiB threshold they commit 8x slower than
  with 256 KiB. Tables without `AUTOINCREMENT` go the other way, about 2x
  faster with 4 MiB, at the price of longer checkpoint pauses; see
  `bench/RESULTS.md`.
* **Hot rows in long MVCC transactions.** In MVCC mode, turso_core 0.8.1
  keeps one row version per update, even for updates by the same
  transaction, and every access walks that chain. So a transaction that
  updates one row many times gets slower with each update. An
  `AUTOINCREMENT` table does that on every insert (its `sqlite_sequence`
  row): 500 inserts took 49 ms, and the sixth 500 in the same transaction
  252 ms, against a flat 5 ms without `AUTOINCREMENT`. Prefer
  `INTEGER PRIMARY KEY` without `AUTOINCREMENT` for MVCC tables (Ecto:
  `migration_primary_key: [type: :integer]` in ecto_sediment), or keep such
  transactions short. WAL mode is not affected.
  With several connections inserting into the same `AUTOINCREMENT` table in
  `BEGIN CONCURRENT` transactions, the id allocations also conflict with
  each other: with 4 writers, about 9% of transactions failed with
  `"Write-write conflict"` (none without `AUTOINCREMENT`) and throughput was
  about 5 times lower.
* **Journal modes.** Turso stores data in WAL mode or MVCC mode.
  `journal_mode: :delete | :truncate | :persist | :memory | :off` are accepted
  and have no effect. `:mvcc` is Turso only.
* **Switching an existing database to MVCC.** turso_core 0.8.1 doesn't carry
  `AUTOINCREMENT` sequences over when an existing WAL database switches to
  MVCC: the next insert reuses id 1 and silently overwrites that row. So
  opening such a database with `journal_mode: :mvcc`, or running
  `PRAGMA journal_mode = 'mvcc'` on it, fails with
  `"refusing to switch to MVCC: ..."` and leaves it unchanged. Databases
  without `AUTOINCREMENT` tables switch normally, and databases created in
  MVCC mode (every S3 database) are not affected. To move an existing
  database with `AUTOINCREMENT` tables to MVCC, copy its data into a new
  database opened with `journal_mode: :mvcc` from the start. The check runs
  right before the switch (again after any wait for a lock), but the two
  aren't atomic: don't create `AUTOINCREMENT` tables from other connections
  while a database is being switched.
* **Pragmas.** `:case_sensitive_like`, `:secure_delete`,
  `:wal_auto_check_point`, `:journal_size_limit`, `:soft_heap_limit` and
  `:hard_heap_limit` are accepted for compatibility and have no effect.
  `locking_mode: :normal` (the default) is not applied, because Turso always
  holds the database file exclusively per process. `auto_vacuum: :full |
  :incremental` requires `experimental: [:autovacuum]`.
* **`:progress_handler_steps` / `set_progress_handler_steps/2`** are no-ops:
  `interrupt/1` and `cancel/1` reach Turso's VM directly.
* **Read-only connections.** Turso shares one database instance per file in
  the VM. A read-only open of a file that is already open read-write is
  enforced per connection (`query_only`); writes fail with
  `"attempt to write a readonly database"` as in SQLite.
* **`close/1` finalizes the connection's prepared statements**, so it
  releases the database, its file locks and an S3 lease right away even if
  statement references are still alive (for example cached by Ecto in client
  processes). Using such a statement afterwards returns
  `{:error, :connection_closed}` or `{:error, :invalid_statement}`
  (exqlite may still step it).
* **Busy waits** (a writer waiting for the lock, up to the busy timeout)
  sleep in the calling process, not on a dirty scheduler thread as SQLite's
  busy handler does under exqlite: many waiting writers can't take every
  dirty IO thread and starve the lock holder's commit or other file I/O in
  the VM. `cancel/1` still ends a wait.
* **`serialize/2`** is implemented with `VACUUM INTO`, so the image is a
  compacted copy. **`deserialize/3`** only replaces `"main"`; statements
  prepared before it must be prepared again.
* **Error messages** follow SQLite's wording where Turso's is only cosmetically
  different (`"no such table: t"`, `"database is locked"`), but syntax errors
  read differently: `"unexpected token: a"` instead of
  `near "a": syntax error`.
* **Constant-folded parameters.** turso_core 0.8.1 drops parameters that
  constant folding removed at the end of a statement (`WHERE 0 AND x = ?`)
  from the parameter count. `bind/2` accepts arguments for them when the SQL
  text has exactly that many placeholders, so queries such as Ecto's
  `where: p.id in ^[]` work. Named parameters are not covered by this.
* **Prepared `PRAGMA` statements are not re-prepared after schema changes**
  in turso_core 0.8.1: `PRAGMA table_info(t)` prepared before `DROP TABLE t`
  keeps returning the old columns. Prepare such statements again after DDL.
  `Sediment.Connection`'s statement cache never caches `PRAGMA`s.
* **Schema changes under a write transaction (MVCC).** Like SQLite, turso
  re-prepares a statement whose schema changed, but inside a write
  transaction it gives up after two attempts: a transaction that overlaps
  another connection's DDL can fail with `"Database schema changed"`.
  Roll back and retry the transaction, or run DDL, such as migrations,
  while nothing else writes.
* **`FULL OUTER JOIN` on an indexed column** fails in turso_core 0.8.1 with
  `FULL OUTER JOIN requires an equality condition in the ON clause` when the
  right-hand table's join column has an index (a primary key, say). A unary
  plus avoids the index: `ON +p.id = c.parent_id`.
* **Dropping a column that has its own `REFERENCES`.** turso_core 0.8.1
  refuses `ALTER TABLE child DROP COLUMN parent_id` when `parent_id` was
  declared with a column-level `REFERENCES` (`unknown column "parent_id" in
  foreign key definition`); SQLite drops the constraint with the column.
  This includes rolling back Ecto's `add :parent_id, references(:parents)`.
  Rebuild the table instead: create it without the column, copy the rows,
  drop the old table, rename the new one and recreate its indexes. A
  table-level `FOREIGN KEY (parent_id)` is refused by both engines.
* **Recursive triggers.** `PRAGMA recursive_triggers = on` is accepted, but
  turso_core 0.8.1 fires a trigger once instead of recursing.
* **Infinite floats** (`SELECT 1e999`) are returned as `:inf` and `:"-inf"`,
  like Postgrex; exqlite raises an `ArgumentError` for them.
* **`file:` URIs** are translated to a path and mode (`mode=ro`, `rw`, `rwc`,
  `memory`); other URI parameters are ignored.

## Performance and testing

`bench/` compares sediment with exqlite; see
[bench/RESULTS.md](bench/RESULTS.md). Through a DBConnection
pool (and so Ecto) queries and single-row inserts are faster than exqlite's;
through the low-level API reads are faster and writes 1.2-2x slower
(turso_core's insert path plus two native calls per statement); concurrent
writers in MVCC mode commit about twice as many transactions per second.

`mix test` runs the unit and integration suites. Tagged suites:
`--only s3` (needs an S3 server such as SeaweedFS on `127.0.0.1:8333`),
`--only slow_test` (cancellation and a longer fuzz run; the stress, timeout,
model and fuzz tests also run in the default suite),
`--only soak` (a mixed
five-minute workload, `SOAK_SECONDS` to change it, that checks memory, NIF
resources and scheduler latency stay bounded), and `--only torture` (S3
crash torture, 30 minutes, `TORTURE_MINUTES` to change it; see the S3 guide).
`.github/workflows/ci.yml` runs `mix ci`, `--only s3` and `--only slow_test`
on the oldest and newest supported Elixir/OTP versions, plus `cargo fmt`,
`cargo clippy` and `cargo test`, against a SeaweedFS service container; the
soak and torture suites run locally (they take minutes to half an hour).

## Under the hood

Each `Sediment.Engine.open/2` creates a Turso connection guarded by a mutex
that is held for the duration of every call, like exqlite's per-connection
mutex. Turso's statement stepping is asynchronous; the NIF drives Turso's IO
loop until a row or completion is available, on a dirty IO scheduler. Busy
handler backoff is waited out in short slices so `cancel/1` can abort a
statement that is waiting for a lock.

turso_core compiles expressions and runs trigger programs recursively, which
can need more stack than a dirty scheduler thread has (about 320 KB): a
50-term expression or a chain of 100 triggers would overflow it and crash the
VM. So every call into turso_core runs on a 16 MB stack that each dirty IO
scheduler thread maps once, with a guard page, and reuses. It is committed
lazily, so it costs virtual address space rather than memory.

## Acknowledgements

* Built on the open-source [Turso](https://github.com/tursodatabase/turso)
  database engine ([`turso_core`](https://crates.io/crates/turso_core), MIT).
* Derived from [`exqlite`][exqlite] by Matthew A. Johnston: the API,
  documentation and many tests are ported from it.
* The NIF is built with [Rustler][rustler].

## License

MIT, see [LICENSE](https://github.com/flmngco/sediment/blob/main/LICENSE). It keeps exqlite's copyright notice for the
ported code.

`Cargo.lock` lists `webpki-root-certs` (CDLA-Permissive-2.0), which may trip license
allow-lists that only cover code licenses. It is a wasm32-only dependency of
`rustls-platform-verifier` and is not built into the NIF on any supported target.

[exqlite]: https://github.com/elixir-sqlite/exqlite
[rustler]: https://github.com/rusterlium/rustler
[rustler_precompiled]: https://github.com/philss/rustler_precompiled
[db_connection]: https://github.com/elixir-ecto/db_connection
