defmodule Sediment.SavepointTest do
  # mode: :savepoint, as Ecto's SQL sandbox uses it: an outermost
  # DBConnection transaction on a connection already inside a native one.
  use ExUnit.Case, async: true

  for {mode, begin} <- [wal: "BEGIN", mvcc: "BEGIN CONCURRENT"] do
    test "savepoints commit, roll back and contain errors in #{mode} mode" do
      {:ok, pool} =
        Sediment.start_link(database: Temp.path!(), journal_mode: unquote(mode), pool_size: 1)

      Sediment.query!(pool, "create table t (x integer)")
      Sediment.query!(pool, unquote(begin))
      Sediment.query!(pool, "insert into t values (1)")

      savepoint = fn fun -> DBConnection.transaction(pool, fun, mode: :savepoint) end

      assert {:ok, :committed} =
               savepoint.(fn conn ->
                 Sediment.query!(conn, "insert into t values (2)")
                 :committed
               end)

      assert {:error, :undo} =
               savepoint.(fn conn ->
                 Sediment.query!(conn, "insert into t values (3)")
                 DBConnection.rollback(conn, :undo)
               end)

      assert_raise Sediment.Error, fn ->
        savepoint.(fn conn -> Sediment.query!(conn, "insert into nope values (4)") end)
      end

      Sediment.query!(pool, "insert into t values (5)")
      Sediment.query!(pool, "COMMIT")
      assert %{rows: [[1], [2], [5]]} = Sediment.query!(pool, "select x from t order by x")
    end
  end
end
