defmodule Sediment.CancelRaceTest do
  # A cancel can land before the native call of the operation it is meant
  # for starts: under load, the call waits for a dirty scheduler. These
  # tests issue the cancel at that point (after Sediment.Engine started the
  # operation, before the native call) by calling Sediment.Native directly.
  use ExUnit.Case, async: true

  alias Sediment.{Engine, Native}

  @endless "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

  setup do
    {:ok, db} = Engine.open(":memory:")
    on_exit(fn -> Engine.close(db) end)
    %{db: db}
  end

  # The result of `fun`, or :running if it didn't return within 10 s.
  defp within(fun) do
    task = Task.async(fun)

    case Task.yield(task, 10_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> :running
    end
  end

  test "a cancel issued before the native call starts applies to it", %{db: db} do
    {:ok, stmt} = Engine.prepare(db, @endless)

    for call <- [
          fn -> Native.execute(db, @endless) end,
          fn -> Native.step(db, stmt) end,
          fn -> Native.multi_step(db, stmt, 10) end,
          fn -> Native.run_prepared(db, stmt, []) end
        ] do
      :ok = Native.cancel(db)
      assert {:error, "interrupted"} = within(call)
    end

    # Used up: the next operation runs.
    assert {:ok, [[1]]} = Engine.fetch_all(db, elem(Engine.prepare(db, "select 1"), 1))
  end

  test "a cancel issued before Engine starts an operation doesn't apply to it", %{db: db} do
    :ok = Engine.cancel(db)
    assert :ok = Engine.execute(db, "create table t (x)")
    :ok = Engine.cancel(db)
    {:ok, stmt} = Engine.prepare(db, "select 1")
    assert {:row, [1]} = Engine.step(db, stmt)
  end

  test "closing for a pool interrupts a query that started after a cancel", %{db: db} do
    # The pool's cancel landed before the client's query started (and that
    # query's start cleared it): the close that follows must still end it.
    :ok = Native.cancel(db)
    parent = self()

    spawn(fn ->
      send(parent, {:query, Engine.execute(db, @endless)})
    end)

    Process.sleep(200)
    assert :ok = within(fn -> Native.close_interrupting(db) end)
    assert_receive {:query, {:error, _}}, 5_000
  end
end
