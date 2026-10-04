defmodule Sediment.BusyWaitTest do
  # Writers waiting for the write lock wait in their own processes, not on
  # dirty IO scheduler threads. With more waiters than dirty IO
  # schedulers (10 by default), sleeping waiters used to take every thread;
  # measured here: ten waiters waited out the whole busy timeout and failed
  # with "database is locked" although the lock was free after 0.5 s (the
  # review also measured the holder's COMMIT and file I/O in the VM stalled
  # for the busy timeout).
  use ExUnit.Case, async: false

  alias Sediment.Engine

  @waiters 12
  @busy_timeout 8_000

  setup do
    dir = Path.join(System.tmp_dir!(), "busy-wait-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    path = Path.join(dir, "w.db")
    {:ok, db} = Engine.open(path)
    :ok = Engine.execute(db, "PRAGMA journal_mode = WAL; CREATE TABLE t (x INTEGER)")
    %{dir: dir, path: path, holder: db}
  end

  defp open(path) do
    {:ok, db} = Engine.open(path)
    :ok = Engine.set_busy_timeout(db, @busy_timeout)
    db
  end

  # One insert through each stepping path: execute/2, step/2 and
  # Sediment.Connection's run_prepared/3.
  defp insert(db, i) do
    case rem(i, 3) do
      0 ->
        Engine.execute(db, "INSERT INTO t VALUES (#{i})")

      1 ->
        {:ok, stmt} = Engine.prepare(db, "INSERT INTO t VALUES (?)")
        :ok = Engine.bind(stmt, [i])
        with :done <- Engine.step(db, stmt), do: :ok

      2 ->
        {:ok, stmt} = Engine.prepare(db, "INSERT INTO t VALUES (?)")
        with {:ok, _, _, _, _} <- Engine.run_prepared(db, stmt, [i]), do: :ok
    end
  end

  test "the lock holder commits at once while more writers than dirty schedulers wait", ctx do
    assert :erlang.system_info(:dirty_io_schedulers) < @waiters
    :ok = Engine.execute(ctx.holder, "BEGIN IMMEDIATE; INSERT INTO t VALUES (0)")

    waiters =
      for i <- 1..@waiters do
        db = open(ctx.path)
        Task.async(fn -> :timer.tc(fn -> insert(db, i) end) end)
      end

    # Let every waiter reach the busy backoff.
    Process.sleep(500)

    {write_us, :ok} =
      :timer.tc(fn -> File.write(Path.join(ctx.dir, "probe"), "file I/O still runs") end)

    {commit_us, :ok} = :timer.tc(fn -> Engine.execute(ctx.holder, "COMMIT") end)
    assert write_us < 1_000_000
    assert commit_us < 1_000_000

    # Every waiter gets the lock soon after the commit. Before, the ten that
    # slept on the dirty IO threads waited out the whole busy timeout (8 s)
    # and failed, although the lock was free after 0.5 s.
    for {micros, result} <- Enum.map(waiters, &Task.await(&1, @busy_timeout + 5_000)) do
      assert result == :ok
      # Well under the 8 s busy timeout the regression waited out; loaded
      # machines took up to 4.4 s here.
      assert micros < 6_000_000
    end

    {:ok, stmt} = Engine.prepare(ctx.holder, "SELECT count(*) FROM t")
    assert {:ok, [[13]]} = Engine.fetch_all(ctx.holder, stmt)
  end

  test "cancel/1 during a busy wait ends it, on every stepping path", ctx do
    :ok = Engine.execute(ctx.holder, "BEGIN IMMEDIATE; INSERT INTO t VALUES (0)")

    for i <- 0..2 do
      db = open(ctx.path)
      task = Task.async(fn -> insert(db, i) end)
      Process.sleep(200)
      :ok = Engine.cancel(db)
      {micros, result} = :timer.tc(fn -> Task.await(task, @busy_timeout + 5_000) end)
      assert micros < 1_000_000
      assert result == {:error, "interrupted"}

      assert :ok = Engine.close(db)
    end

    :ok = Engine.execute(ctx.holder, "COMMIT")
  end

  test "a waiting multi-statement execute resumes where it stopped", ctx do
    :ok = Engine.execute(ctx.holder, "BEGIN IMMEDIATE; INSERT INTO t VALUES (0)")
    db = open(ctx.path)

    task =
      Task.async(fn ->
        Engine.execute(db, "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
      end)

    Process.sleep(300)
    :ok = Engine.execute(ctx.holder, "COMMIT")
    assert Task.await(task, @busy_timeout + 5_000) == :ok

    {:ok, stmt} = Engine.prepare(db, "SELECT x FROM t ORDER BY x")
    assert {:ok, [[0], [1], [2]]} = Engine.fetch_all(db, stmt)
  end
end
