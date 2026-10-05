# Turso extensions

Turso-only features exposed by `sediment`, beyond the exqlite API. Most
are enabled per database with `Sediment.Engine.open/2` or connection
options; see `Sediment.Connection.connect/1`.

## MVCC and `BEGIN CONCURRENT`

Open the database in MVCC journal mode to let several connections write in
concurrent transactions. Conflicts are detected per row at write time.

```elixir
{:ok, conn} =
  Sediment.start_link(
    database: "app.db",
    journal_mode: :mvcc,
    default_transaction_mode: :concurrent
  )
```

Choose MVCC when you create the database. An existing WAL database with
`AUTOINCREMENT` tables refuses the switch with
`"refusing to switch to MVCC: ..."` and is left unchanged: turso_core 0.8.1
would reuse their ids after the switch, silently overwriting rows. Tables
without `AUTOINCREMENT` switch fine. To move such a database to MVCC, copy
its data into a new database opened with `journal_mode: :mvcc`.

`default_transaction_mode: :concurrent` makes `Sediment.transaction/3`
(and `Repo.transaction/2`) issue `BEGIN CONCURRENT`. Turso doesn't allow DDL
in a concurrent transaction, so a transaction whose first statement is DDL
(a migration, for example) begins with `BEGIN IMMEDIATE` instead; an explicit
`mode: :concurrent` always uses `BEGIN CONCURRENT`. A write that conflicts
with another open transaction fails with
`%Sediment.Error{message: "Write-write conflict"}`; Turso aborts that
transaction, and the connection stays usable. Retry the whole transaction.

An `INSERT` into an `AUTOINCREMENT` table inside `BEGIN CONCURRENT` that
finds another connection holding the write lock (an exclusive transaction or
a checkpoint) fails with `"Database is busy"` right away instead of waiting
for `:busy_timeout`, and the transaction can then only be rolled back. This
works around a turso_core 0.8.1 bug that would otherwise commit part of the
transaction ([the upstream report](../docs/upstream/turso-sequence-inner-tx-busy.md)). Retry the whole
transaction, as after a conflict, or avoid `AUTOINCREMENT` in MVCC tables
(see "Hot rows" in the README): concurrent `AUTOINCREMENT` inserts also
conflict with each other on the table's sequence, so such transactions fail
with `"Write-write conflict"` far more often than their rows would suggest.

turso_core 0.8.1 can panic when a transaction that starts with DDL (it
begins as `BEGIN IMMEDIATE`) runs at the same time as `BEGIN CONCURRENT`
transactions on other connections that write the same tables. The driver
catches the panic: the statement fails with
`"internal turso error: ...; the connection was closed"`, that connection is
closed (a pool replaces it), and other connections are unaffected. Avoid
running migrations while concurrent writers are active.

At the low level, `Sediment.Engine.open(path, journal_mode: :mvcc)` does
the same.

In MVCC mode Turso keeps a logical log next to the database, named by
replacing the file extension with `.db-log`. Database files in one directory
that differ only after their last dot (`app.db` and `app.sqlite`, or `app.1`
and `app.2`) would share `app.db-log`, and each would replay, checkpoint and
truncate the other's commits. Sediment refuses that with
`"... would share its MVCC log ..."`:

* Opening an existing MVCC database (also an S3 database's local copy, or
  the source of an export or S3 import) fails while another MVCC
  database file with the same name before the last dot exists (a copy of
  it, say), or while a database open in the VM under another name uses the
  log. WAL and legacy files with that name, such as an export or a backup,
  don't count: they never read the log.
* Creating, restoring or switching a database to MVCC there also fails while
  any other database file has that name, since that file couldn't be opened
  next to the new log anymore.

Give each database file its own base name (`app-1.db`, `app-2.db`). Names
are compared ignoring case, as on macOS and Windows volumes. For a symlink,
the log is next to the file it points to, and that is where the check looks.
An S3 writer's or `Sediment.S3.restore/3`'s local path can't be a symlink:
a restore replaces the file at that path, which would turn the link into a
second database next to the file it pointed to.

`ATTACH` (`experimental: [:attach]`) opens the attached file's log the same
way, so MVCC and `:attach` don't mix: a database opened with `:attach`
can't be in MVCC mode (`journal_mode: :mvcc`, an MVCC file, `:s3`, or a
later `PRAGMA journal_mode = 'mvcc'` are refused), and attaching an MVCC
database fails, however the statement names the file. WAL databases attach
as before.

A log is not empty after a clean close, so rename a database together with
its `.db-log` file, and only while it is closed. Two names for one file
are not covered: a hard link (`ln app.db other.db`) is one database with two
log names, and commits in one log aren't seen through the other name. Two
OS processes creating clashing MVCC databases at the same moment aren't
covered either (the check runs per process); once one exists, the other is
refused.

## Encryption

```elixir
Sediment.start_link(
  database: "secret.db",
  encryption: [cipher: "aegis256", key: "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"]
)
```

The key is hex encoded. Supported ciphers: `aes128gcm`, `aes256gcm`,
`aegis128l`, `aegis128x2`, `aegis128x4`, `aegis256`, `aegis256x2`,
`aegis256x4`. Opening
with a wrong key fails with `"Decryption failed for page=1"`; opening without
a key fails with `"File is not a database"`.

While a file is open in the VM, Turso shares its decrypted pages with every
other open of it, so every open must make the same choice: the same cipher
and key (or no key, for an unencrypted file). An open with another key, no
key or `encryption: false` fails with `"... is already open in this VM with
another :encryption choice ..."` without reading anything.

## Vector search

Turso has built-in vector functions (`vector32`, `vector64`, `vector8`,
`vector1bit`, `vector_distance_cos`, `vector_distance_l2`,
`vector_distance_dot`, `vector_extract`, ...). `Sediment.Vector` encodes
Elixir lists as vector blobs:

```elixir
alias Sediment.Vector

Sediment.query!(conn, "create table docs (id integer primary key, embedding blob)")
Sediment.query!(conn, "insert into docs values (?, ?)", [1, Vector.new([0.1, 0.2, 0.3])])

Sediment.query!(conn,
  "select id from docs order by vector_distance_cos(embedding, ?) limit 5",
  [Vector.new([0.1, 0.2, 0.25])])
```

## Full-text search

FTS indexes are an experimental Turso feature, enabled per database:

```elixir
{:ok, conn} = Sediment.start_link(database: "app.db", experimental: [:index_method])

Sediment.query!(conn, "create index docs_fts on docs using fts(body)")

Sediment.query!(conn,
  "select id, fts_score(body, ?1) from docs where fts_match(body, ?1) order by 2 desc",
  ["hello"])
```

Pass the search query once and reference it twice (`?1`): Turso only scores
through the index when `fts_score` and `fts_match` share the same expression.

## Change data capture

Turso can record every change a connection makes in a CDC table.
`Sediment.CDC` enables it and decodes the records:

```elixir
{:ok, conn} =
  Sediment.start_link(
    database: "app.db",
    custom_pragmas: [capture_data_changes_conn: "'full'"]
  )

{:ok, changes} = Sediment.CDC.changes(conn, tables: ["users"], since: last_seen_id)
# [%Sediment.CDC.Change{type: :insert, table: "users", row_id: 1, after: %{"id" => 1, ...}}, ...]
```

## S3 durability

A database can keep its durable state in an S3 bucket: commits are uploaded
in the background (async by default; `sync: true` or `durability: :sync` to
wait), and opening on another machine restores it. One writer at a time
holds a lease.

```elixir
Sediment.start_link(
  database: "/var/lib/app/app.db",
  s3: [bucket: "my-bucket", prefix: "prod/app", region: "eu-central-1"],
  encryption: [cipher: "aegis256", key: System.fetch_env!("DATABASE_ENCRYPTION_KEY")]
)
```

S3 databases run in MVCC journal mode and are encrypted by default:
`:encryption` (a key) or `encryption: false` is required, and the key
encrypts the bucket's objects too. In-memory databases and `mode: :readonly` can't be
combined with `:s3` (`mode: :replica` opens a read-only copy). See the
[S3 durability guide](s3.md) for configuration, semantics, failure modes and
costs, and `Sediment.S3` for the helpers.

## Other experimental features

`experimental:` accepts `:attach`, `:views`, `:vacuum`, `:autovacuum`,
`:generated_columns`, `:without_rowid`, `:index_method`, `:custom_types`,
`:encryption` and `:mvcc_passive_checkpoint`. These map to turso_core's
`DatabaseOpts`. Turso shares one database instance per file within the VM,
so the flags of the first open of a file apply to later opens. `:attach`
can't be combined with MVCC (see "MVCC" above).

