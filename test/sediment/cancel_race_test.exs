defmodule Sediment.CancelRaceTest do
  # A cancel can land before the native call of the operation it is meant
  # for starts: under load, the call waits for a dirty scheduler. An
  # operation is admitted (Native.admit/1) when Sediment.Engine starts it;
  # these tests admit, cancel and call Sediment.Native in a chosen order to
  # put the cancel at those points.
  use ExUnit.Case, async: true

  alias Sediment.{Engine, Native}

  @endless "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

  setup do
    {:ok, db} = Engine.open(":memory:")

    # Stops whatever a failed test left running, so it fails, not hangs.
    on_exit(fn ->
      Native.start_closing(db)
      Engine.close(db)
    end)

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

  test "a cancel applies to an operation admitted before it, still queued", %{db: db} do
    {:ok, stmt} = Engine.prepare(db, @endless)

    for call <- [
          &Native.execute(db, @endless, &1),
          &Native.step(db, stmt, &1),
          &Native.multi_step(db, stmt, 10, &1),
          &Native.run_prepared(db, stmt, [], &1)
        ] do
      admission = Native.admit(db)
      :ok = Native.cancel(db)
      assert {:error, "interrupted"} = within(fn -> call.(admission) end)
    end
  end

  test "admitting another operation after a cancel doesn't erase it", %{db: db} do
    # A admitted and queued, the cancel, then B admitted (and run first).
    a = Native.admit(db)
    :ok = Native.cancel(db)
    b = Native.admit(db)

    assert :ok = Native.execute(db, "create table t (x)", b)
    assert {:error, "interrupted"} = within(fn -> Native.execute(db, @endless, a) end)
    assert :ok = Native.execute(db, "insert into t values (1)", b)
  end

  test "a cancel issued before an operation starts doesn't apply to it", %{db: db} do
    :ok = Engine.cancel(db)
    assert :ok = Engine.execute(db, "create table t (x)")
    :ok = Engine.cancel(db)
    {:ok, stmt} = Engine.prepare(db, "select 1")
    assert {:row, [1]} = Engine.step(db, stmt)
    :ok = Engine.cancel(db)
    {:ok, stmt} = Engine.prepare(db, "select 2")
    assert {:ok, [[2]]} = Engine.fetch_all(db, stmt)
  end

  test "a cancel with nothing running doesn't break statements next to an open one", %{db: db} do
    {:ok, cursor} =
      Engine.prepare(
        db,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 300000) SELECT x FROM c"
      )

    assert {:row, [1]} = Engine.step(db, cursor)
    :ok = Engine.cancel(db)
    {:ok, one} = Engine.prepare(db, "select 1")
    assert {:row, [1]} = Engine.step(db, one)
    assert :ok = Engine.execute(db, "create table t (x)")
    assert {:row, [2]} = Engine.step(db, cursor)
  end

  test "a cancel during fetch_all applies to it", %{db: db} do
    {:ok, stmt} =
      Engine.prepare(
        db,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT x FROM c"
      )

    parent = self()
    spawn(fn -> send(parent, {:fetched, Engine.fetch_all(db, stmt, 1)}) end)
    Process.sleep(200)
    :ok = Engine.cancel(db)
    assert_receive {:fetched, {:error, "interrupted"}}, 10_000
  end

  test "start_closing stops a running query and every later one", %{db: db} do
    parent = self()
    spawn(fn -> send(parent, {:query, Engine.execute(db, @endless)}) end)
    Process.sleep(200)

    :ok = Native.start_closing(db)
    assert_receive {:query, {:error, "interrupted"}}, 10_000
    assert {:error, "interrupted"} = within(fn -> Engine.execute(db, @endless) end)
    assert :ok = Engine.close(db)
  end
end
