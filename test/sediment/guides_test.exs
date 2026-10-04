defmodule Sediment.GuidesTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  test "getting started: pool" do
    {:ok, conn} = Sediment.start_link(database: Temp.path!(), journal_mode: :wal)

    Sediment.query!(conn, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    Sediment.query!(conn, "INSERT INTO users (name) VALUES (?)", ["Alice"])

    %Sediment.Result{columns: ["id", "name"], rows: [[1, "Alice"]]} =
      Sediment.query!(conn, "SELECT id, name FROM users")

    {:ok, :done} =
      Sediment.transaction(conn, fn conn ->
        Sediment.query!(conn, "UPDATE users SET name = ? WHERE id = ?", ["Bob", 1])
        :done
      end)
  end

  test "getting started: supervision tree child spec" do
    start_supervised!({Sediment, name: __MODULE__.DB, database: Temp.path!()})
    assert %{rows: [[1]]} = Sediment.query!(__MODULE__.DB, "SELECT 1")
  end

  test "getting started: low-level API" do
    path = Temp.path!()
    {:ok, db} = Engine.open(path)
    :ok = Engine.execute(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB)")

    {:ok, stmt} = Engine.prepare(db, "INSERT INTO kv (k, v) VALUES (?1, ?2)")
    :ok = Engine.bind(stmt, ["greeting", {:blob, "hello"}])
    :done = Engine.step(db, stmt)
    :ok = Engine.release(db, stmt)

    {:ok, stmt} = Engine.prepare(db, "SELECT k, v FROM kv")
    {:ok, [["greeting", "hello"]]} = Engine.fetch_all(db, stmt)
    :ok = Engine.close(db)
  end

  test "getting started: value table" do
    {:ok, conn} = Sediment.start_link(database: :memory)

    %{rows: [types]} =
      Sediment.query!(
        conn,
        "select typeof(?), typeof(?), typeof(?), typeof(?), typeof(?), typeof(?), typeof(?), typeof(?)",
        [1, 1.5, "text", {:blob, "b"}, <<255>>, nil, :atom, ~D[2024-01-02]]
      )

    assert types == ["integer", "real", "text", "blob", "blob", "null", "text", "text"]
  end
end
