defmodule Sediment.StatementCacheTest do
  use ExUnit.Case, async: true

  # One connection, so every query goes through the same statement cache
  setup do
    {:ok, conn} = Sediment.start_link(database: Temp.path!(), pool_size: 1)
    %{conn: conn}
  end

  test "a PRAGMA that changes a setting drops cached statements (CHECK constraints)", %{
    conn: conn
  } do
    Sediment.query!(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY CHECK (id > 0))")
    insert = "INSERT INTO t VALUES (?)"

    Sediment.query!(conn, "PRAGMA ignore_check_constraints = ON")
    assert {:ok, _} = Sediment.query(conn, insert, [-1])

    Sediment.query!(conn, "PRAGMA ignore_check_constraints = OFF")

    assert {:error, %Sediment.Error{message: "CHECK constraint failed" <> _}} =
             Sediment.query(conn, insert, [-2])

    assert %{rows: [[-1]]} = Sediment.query!(conn, "SELECT id FROM t")
  end

  test "leading empty statements don't make a PRAGMA cacheable", %{conn: conn} do
    Sediment.query!(conn, "CREATE TABLE t (x INTEGER)")
    columns = fn -> Sediment.query!(conn, "; PRAGMA table_info(t)").rows |> length() end

    assert columns.() == 1
    Sediment.query!(conn, "ALTER TABLE t ADD COLUMN y INTEGER")
    assert columns.() == 2
  end

  test "leading empty statements before DDL still begin an IMMEDIATE transaction" do
    {:ok, conn} =
      Sediment.start_link(
        database: Temp.path!(),
        pool_size: 1,
        journal_mode: :mvcc,
        default_transaction_mode: :concurrent
      )

    assert {:ok, :done} =
             Sediment.transaction(conn, fn c ->
               Sediment.query!(c, ";; CREATE TABLE m (x INTEGER)")
               :done
             end)

    assert %{rows: [[0]]} = Sediment.query!(conn, "SELECT count(*) FROM m")
  end
end
