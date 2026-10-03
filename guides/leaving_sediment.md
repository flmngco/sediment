# Leaving Sediment

A Sediment database can always go back to a plain SQLite file:
`Sediment.export_sqlite/3` writes one that SQLite opens, with the same
schema, rows and AUTOINCREMENT sequences. This works for local databases
(WAL or MVCC, encrypted or not) and for S3 databases (the export restores the
latest state into a temporary file; the bucket is only read).

## Exporting

```elixir
# A local database file (stop its writers first)
{:ok, info} = Sediment.export_sqlite("/var/lib/app/app.db", "/tmp/app-sqlite.db")

# An encrypted one: its key
{:ok, info} =
  Sediment.export_sqlite("/var/lib/app/app.db", "/tmp/app-sqlite.db",
    encryption: [cipher: "aegis256", key: System.fetch_env!("DATABASE_ENCRYPTION_KEY")]
  )

# An S3 database: its :s3 options and its key (or encryption: false)
{:ok, info} =
  Sediment.export_sqlite(nil, "/tmp/app-sqlite.db",
    from_s3: [bucket: "my-bucket", prefix: "prod/app", region: "eu-central-1"],
    encryption: [cipher: "aegis256", key: System.fetch_env!("DATABASE_ENCRYPTION_KEY")]
  )
# => %{rows: 48_210, objects: 14, sequences: [{"users", 1042}, ...], dropped_fts: []}
```

With Ecto (ecto_sediment), the repo's configuration says where the database
is:

```console
$ mix ecto.sediment.export_sqlite -r MyApp.Repo -o /tmp/app-sqlite.db
```

Then point the application at the file with ecto_sqlite3 (or exqlite):

```elixir
config :my_app, MyApp.Repo,
  adapter: Ecto.Adapters.SQLite3,
  database: "/var/lib/app/app-sqlite.db"
```

## What the export does

1. Refuses if the destination (or its `-wal`/`-journal`) exists.
2. Creates a private staging directory next to the destination (mode 0700,
   on the same file system), and does everything else inside it, so the
   plaintext of an encrypted database is never readable by other users,
   whatever the umask.
3. Copies the source and its WAL and MVCC log there, or restores the S3
   database there. The source itself is never opened, so it is never written
   to. For a symlink, the WAL and log are looked for next to the link (where
   a writer that opened the link put them) and next to its target; if both
   have one, the export refuses (ambiguous). A database file with several
   hard links is refused, since its WAL may be next to another name: export
   by the path its writers use.
4. Opens that copy (with the key), checkpoints it and, if it is in MVCC
   mode, switches it to WAL. In turso_core 0.8.1, `VACUUM INTO` from an MVCC
   database writes an MVCC header SQLite can't read, resets
   `sqlite_sequence` to the largest id (SQLite would hand out the ids of
   deleted rows again), and copies every row in one transaction, which is
   quadratic for AUTOINCREMENT tables. From WAL none of this happens.
5. Records its row counts and `sqlite_sequence`, drops FTS indexes (with
   `drop_fts: true`, see below), and `VACUUM INTO` a new file: plaintext,
   compact.
6. Checks the new file (`PRAGMA integrity_check`, every table's row count,
   `sqlite_sequence` equal to the source's), then links it to the
   destination (mode 0600) and removes the staging directory. On any error
   nothing is left at the destination.

This chain was verified with SQLite 3.45 (python3's sqlite3 and the sqlite3
CLI) on an encrypted S3 database with AUTOINCREMENT tables whose newest rows
were deleted, a unique index, a cascading foreign key, a trigger, a view, a
CHECK constraint and `user_version`: same rows (checksummed), the next
AUTOINCREMENT id in SQLite right after the deleted ones, every constraint,
trigger and view working. The driver's tests repeat it with SQLite itself on
every run.

## Checking the result

```console
$ sqlite3 /tmp/app-sqlite.db "PRAGMA integrity_check; PRAGMA foreign_key_check;"
ok
$ sqlite3 /tmp/app-sqlite.db "SELECT * FROM sqlite_sequence;"
```

## Limitations

* **Turso FTS indexes** (`CREATE INDEX ... ON t USING fts(...)`) make a file
  unreadable for SQLite ("malformed database schema"). The export refuses a
  database that has any, naming them; `drop_fts: true` (`--drop-fts`) leaves
  them out, and only them: they are recognized by `USING` right after the
  table, so partial indexes, `JOIN ... USING` views and literals containing
  "using" export as they are. Indexes of other Turso index methods are
  refused.
  The indexed text stays; recreate the search in SQLite with an fts5 virtual
  table if needed.
* **Vector columns** are ordinary blobs (the raw little-endian floats, 4 bytes
  per dimension for `vector32`). SQLite has no `vector_*` functions.
* **CDC**: the `turso_cdc` and `turso_cdc_version` tables come along as
  ordinary tables (from WAL and MVCC databases; `turso_cdc`'s
  `sqlite_sequence` row is checked to be at least its largest id).
* **turso's own tables** stay in the file: `__turso_internal_mvcc_meta` and
  one `__turso_internal_seq_...` per AUTOINCREMENT table. SQLite ignores them
  (they are ordinary tables to it; AUTOINCREMENT uses `sqlite_sequence`), and
  Sediment needs them if the file is opened with it again. turso refuses to
  drop them; SQLite can:

  ```console
  $ sqlite3 /tmp/app-sqlite.db "SELECT 'DROP TABLE \"' || name || '\";' FROM sqlite_schema
      WHERE type = 'table' AND name LIKE '\_\_turso\_internal\_%' ESCAPE '\'" | sqlite3 /tmp/app-sqlite.db
  $ sqlite3 /tmp/app-sqlite.db "VACUUM; PRAGMA integrity_check;"
  ```

* A copy of an encrypted database keeps 48 reserved bytes per page (where the
  encryption's nonce and tag were). SQLite handles them; `VACUUM` in SQLite
  keeps them.
* Not verified: Turso's experimental WITHOUT ROWID tables, generated columns
  and custom types.
* The export reads a consistent copy of the file as it is when the export
  starts: stop every process writing it first (an S3 database exported with
  `from_s3` is read as of its latest durable state, and its writer may keep
  running).
