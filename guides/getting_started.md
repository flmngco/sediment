# Getting started

`sediment` is an Elixir driver for [Turso](https://github.com/tursodatabase/turso),
the SQLite-compatible database written in Rust. Its API mirrors
[exqlite](https://hexdocs.pm/exqlite): if you know exqlite, rename
`Exqlite` to `Sediment` and `Exqlite.Sqlite3` to `Sediment.Engine`.

## Installation

```elixir
def deps do
  [
    {:sediment, "~> 0.1"}
  ]
end
```

The native part is compiled from source with Rustler, so you need a Rust
toolchain (`rustup`). The first build takes a few minutes.

For Ecto, use the `ecto_sediment` adapter instead of this package directly.

## A connection pool

`Sediment` implements `DBConnection`, so you get pooling, transactions
and streaming:

```elixir
{:ok, conn} = Sediment.start_link(database: "app.db", journal_mode: :wal)

Sediment.query!(conn, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
Sediment.query!(conn, "INSERT INTO users (name) VALUES (?)", ["Alice"])

%Sediment.Result{columns: ["id", "name"], rows: [[1, "Alice"]]} =
  Sediment.query!(conn, "SELECT id, name FROM users")

{:ok, :done} =
  Sediment.transaction(conn, fn conn ->
    Sediment.query!(conn, "UPDATE users SET name = ? WHERE id = ?", ["Bob", 1])
    :done
  end)
```

In a supervision tree:

```elixir
children = [
  {Sediment, name: MyApp.DB, database: "/var/lib/my_app/app.db", journal_mode: :wal}
]
```

`Sediment.Connection.connect/1` lists every connection option.

## Values

| Elixir | Stored as |
| --- | --- |
| `integer` (64-bit) | INTEGER |
| `float` | REAL |
| `binary` (valid UTF-8) | TEXT |
| `{:blob, binary}`, or a binary that isn't UTF-8 | BLOB |
| `nil` | NULL |
| `atom` | TEXT (the atom's name) |
| `Date`, `Time`, `NaiveDateTime`, UTC `DateTime` | ISO 8601 TEXT |

Results come back as integers, floats, binaries and `nil`. Custom types
can be added with `Sediment.TypeExtension`.

## The low-level API

`Sediment.Engine` works directly with a database handle and prepared
statements, like `Exqlite.Sqlite3`:

```elixir
alias Sediment.Engine

{:ok, db} = Engine.open("app.db")
:ok = Engine.execute(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB)")

{:ok, stmt} = Engine.prepare(db, "INSERT INTO kv (k, v) VALUES (?1, ?2)")
:ok = Engine.bind(stmt, ["greeting", {:blob, "hello"}])
:done = Engine.step(db, stmt)
:ok = Engine.release(db, stmt)

{:ok, stmt} = Engine.prepare(db, "SELECT k, v FROM kv")
{:ok, [["greeting", "hello"]]} = Engine.fetch_all(db, stmt)
:ok = Engine.close(db)
```

A handle may be shared between processes; calls on it are serialized.

## Where to go next

* [Turso extensions](turso_extensions.md): MVCC and `BEGIN CONCURRENT`,
  encryption, vector search, full-text search, change data capture.
* [S3 durability](s3.md): keep the database's state of record in S3.
* [Exqlite API parity](exqlite_parity.md) and the README's "Differences from
  exqlite" section, when porting code from exqlite.
