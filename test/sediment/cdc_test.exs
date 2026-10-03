defmodule Sediment.CDCTest do
  use ExUnit.Case, async: true

  alias Sediment.CDC
  alias Sediment.CDC.Change
  alias Sediment.Engine

  setup do
    {:ok, db} = Engine.open(":memory:")
    :ok = CDC.enable(db, :full)
    :ok = Engine.execute(db, "create table t (id integer primary key, v text)")
    [db: db]
  end

  test "records inserts, updates and deletes", %{db: db} do
    :ok = Engine.execute(db, "insert into t values (1, 'a')")
    :ok = Engine.execute(db, "update t set v = 'b' where id = 1")
    :ok = Engine.execute(db, "delete from t where id = 1")

    assert {:ok,
            [
              %Change{type: :insert, table: "t", row_id: 1, before: nil, after: %{"v" => "a"}},
              %Change{type: :update, before: %{"v" => "a"}, after: %{"id" => 1, "v" => "b"}},
              %Change{type: :delete, before: %{"v" => "b"}, after: nil}
            ]} = CDC.changes(db, tables: ["t"])
  end

  test "includes commit records and supports since and limit", %{db: db} do
    :ok = Engine.execute(db, "insert into t values (1, 'a'), (2, 'b')")
    {:ok, all} = CDC.changes(db)
    assert %Change{type: :commit, table: nil} = List.last(all)

    [first | _] = all
    {:ok, rest} = CDC.changes(db, since: first.id, limit: 2)
    assert Enum.map(rest, & &1.id) == all |> tl() |> Enum.take(2) |> Enum.map(& &1.id)
  end

  test "id mode records row ids only" do
    {:ok, db} = Engine.open(":memory:")
    :ok = Engine.execute(db, "create table t (id integer primary key, v text)")
    :ok = CDC.enable(db, :id, table: "my_changes")
    :ok = Engine.execute(db, "insert into t values (7, 'a')")

    assert {:ok, [%Change{type: :insert, row_id: 7, before: nil, after: nil}]} =
             CDC.changes(db, table: "my_changes", tables: ["t"])
  end

  test "disable stops capture", %{db: db} do
    :ok = CDC.disable(db)
    :ok = Engine.execute(db, "insert into t values (1, 'a')")
    assert {:ok, []} = CDC.changes(db, tables: ["t"])
  end

  test "works through DBConnection with custom_pragmas" do
    {:ok, pool} =
      Sediment.start_link(
        database: Temp.path!(),
        custom_pragmas: [capture_data_changes_conn: "'after'"]
      )

    Sediment.query!(pool, "create table t (id integer primary key, v text)")
    Sediment.query!(pool, "insert into t values (?, ?)", [1, "x"])

    assert {:ok, [%Change{type: :insert, after: %{"id" => 1, "v" => "x"}}]} =
             CDC.changes(pool, tables: ["t"])
  end
end
