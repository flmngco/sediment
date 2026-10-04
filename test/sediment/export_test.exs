defmodule Sediment.ExportTest do
  # Sediment.export_sqlite/3; the Rust tests check exports with real SQLite
  # (rusqlite). This one also asks python3's sqlite3, when installed.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  @python3 System.find_executable("python3")

  setup do
    dir = Path.join(System.tmp_dir!(), "export-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp source(dir) do
    path = Path.join(dir, "app.db")
    {:ok, db} = Engine.open(path, experimental: [:views])

    for sql <- [
          "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE)",
          "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT NOT NULL)",
          "CREATE TRIGGER users_log AFTER INSERT ON users BEGIN INSERT INTO log (msg) VALUES (new.email); END",
          "CREATE VIEW emails AS SELECT email FROM users",
          "INSERT INTO users (email) VALUES ('a@x'), ('b@x'), ('c@x')",
          "DELETE FROM users WHERE email = 'c@x'"
        ] do
      :ok = Engine.execute(db, sql)
    end

    :ok = Engine.close(db)
    path
  end

  test "exports a database that SQLite opens, never touching the source", %{dir: dir} do
    source = source(dir)
    before = File.read!(source)
    dest = Path.join(dir, "export.db")

    assert {:ok, %{rows: 5, sequences: sequences, dropped_fts: []}} =
             Sediment.export_sqlite(source, dest)

    assert {"users", 3} in sequences
    assert File.read!(source) == before
    assert File.ls!(dir) |> Enum.reject(&String.starts_with?(&1, "app.db")) == ["export.db"]

    assert {:error, "export target exists: " <> _} = Sediment.export_sqlite(source, dest)
  end

  @tag skip: if(!@python3, do: "python3 isn't installed")
  test "python3's sqlite3 reads the export and continues its ids", %{dir: dir} do
    dest = Path.join(dir, "export.db")
    {:ok, _} = Sediment.export_sqlite(source(dir), dest)

    script = """
    import sqlite3, sys
    c = sqlite3.connect(sys.argv[1])
    print(c.execute("PRAGMA integrity_check").fetchone()[0])
    c.execute("INSERT INTO users (email) VALUES ('d@x')")
    print(c.execute("SELECT max(id) FROM users").fetchone()[0])
    print(c.execute("SELECT count(*) FROM log").fetchone()[0])
    print(c.execute("SELECT count(*) FROM emails").fetchone()[0])
    """

    assert {"ok\n4\n4\n3\n", 0} = System.cmd(@python3, ["-c", script, dest])
  end
end
