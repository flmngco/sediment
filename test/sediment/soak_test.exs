defmodule Sediment.SoakTest do
  # A long mixed workload that checks nothing leaks and no scheduler blocks:
  #   SOAK_SECONDS=300 mix test --only soak
  use ExUnit.Case, async: false

  alias Sediment.Engine

  @moduletag :soak
  @moduletag timeout: :infinity

  @long_select "WITH RECURSIVE r(i) AS (VALUES(0) UNION ALL SELECT i + 1 FROM r) SELECT count(*) FROM r"

  defp seconds, do: String.to_integer(System.get_env("SOAK_SECONDS", "300"))

  defp snapshot do
    {conns, stmts} = Sediment.Native.resource_counts()

    %{
      memory: :erlang.memory(:total),
      rss: rss(),
      processes: :erlang.system_info(:process_count),
      conns: conns,
      stmts: stmts
    }
  end

  # Resident set size of the VM (includes the NIF's Rust heap), Linux only.
  defp rss do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        [kb] = Regex.run(~r/VmRSS:\s+(\d+) kB/, status, capture: :all_but_first)
        String.to_integer(kb) * 1024

      _ ->
        0
    end
  end

  defp collect_everything do
    for pid <- Process.list(), do: :erlang.garbage_collect(pid)
    # destructors finish on the cleanup thread
    Process.sleep(500)
  end

  # Reports the worst lateness of a 10 ms timer while the workload runs.
  defp scheduler_probe(parent) do
    spawn_link(fn -> probe_loop(parent, 0) end)
  end

  defp probe_loop(parent, worst) do
    receive do
      :stop -> send(parent, {:probe, worst})
    after
      0 ->
        started = System.monotonic_time(:millisecond)
        Process.sleep(10)
        late = System.monotonic_time(:millisecond) - started - 10
        probe_loop(parent, max(worst, late))
    end
  end

  # -- workload -----------------------------------------------------------

  defp pool_ops(pool, worker) do
    case :rand.uniform(6) do
      1 ->
        Sediment.query!(pool, "INSERT INTO items (worker, payload) VALUES (?, ?)", [
          worker,
          :crypto.strong_rand_bytes(:rand.uniform(512))
        ])

      2 ->
        Sediment.query!(pool, "SELECT count(*), max(id) FROM items WHERE worker = ?", [worker])

      3 ->
        Sediment.transaction(pool, &insert_then_delete_or_rollback(&1, worker))

      4 ->
        Sediment.transaction(pool, fn conn ->
          %Sediment.Stream{
            conn: conn,
            query: "SELECT id, payload FROM items",
            params: [],
            options: [max_rows: 50]
          }
          |> Enum.take(:rand.uniform(4))
        end)

      5 ->
        # a unique statement text each time: exercises cache eviction
        Sediment.query!(pool, "SELECT #{:rand.uniform(1_000_000)} + count(*) FROM items")

      6 ->
        {:ok, query} = Sediment.prepare(pool, "", "SELECT id FROM items WHERE id > ? LIMIT 5")
        Sediment.execute!(pool, query, [:rand.uniform(1000)])
    end
  end

  defp insert_then_delete_or_rollback(conn, worker) do
    Sediment.query!(conn, "INSERT INTO items (worker, payload) VALUES (?, 'tx')", [worker])

    if :rand.uniform(3) == 1 do
      Sediment.rollback(conn, :random)
    else
      Sediment.query!(
        conn,
        "DELETE FROM items WHERE id IN (SELECT id FROM items WHERE worker = ? LIMIT 1)",
        [worker]
      )
    end
  end

  defp raw_churn(path) do
    {:ok, db} = Engine.open(path)

    for _ <- 1..:rand.uniform(20) do
      {:ok, stmt} = Engine.prepare(db, "SELECT id FROM items LIMIT 3")
      _ = Engine.multi_step(db, stmt, 2)
      if :rand.uniform(2) == 1, do: Engine.release(db, stmt)
    end

    if :rand.uniform(2) == 1, do: Engine.close(db)
    # otherwise the handle and its statements are left to the GC
  end

  defp interrupt_or_cancel do
    {:ok, db} = Engine.open(":memory:")
    parent = self()

    runner =
      spawn(fn ->
        {:ok, stmt} = Engine.prepare(db, @long_select)
        send(parent, {:ran, Engine.step(db, stmt)})
      end)

    Process.sleep(:rand.uniform(20))

    wait_for_runner = fn wait ->
      if :rand.uniform(2) == 1, do: Engine.interrupt(db), else: Engine.cancel(db)

      receive do
        {:ran, _} -> :ok
      after
        20 -> wait.(wait)
      end
    end

    wait_for_runner.(wait_for_runner)
    Process.exit(runner, :kill)
    Engine.close(db)
  end

  defp worker(pool, path, worker, deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      random_op(pool, path, worker)
      worker(pool, path, worker, deadline)
    end
  end

  defp random_op(pool, path, worker) do
    case :rand.uniform(10) do
      n when n <= 7 -> pool_ops(pool, worker)
      8 -> raw_churn(path)
      9 -> interrupt_or_cancel()
      10 -> spawn(fn -> raw_churn(path) end)
    end
  end

  test "mixed workload keeps memory, resources and schedulers bounded" do
    dir = Temp.mkdir!()
    path = Path.join(dir, "soak.db")
    collect_everything()
    baseline = snapshot()

    {:ok, pool} =
      Sediment.start_link(
        database: path,
        journal_mode: :mvcc,
        pool_size: 8,
        busy_timeout: 10_000
      )

    Sediment.query!(
      pool,
      "CREATE TABLE items (id INTEGER PRIMARY KEY, worker INTEGER, payload BLOB)"
    )

    probe = scheduler_probe(self())
    total = seconds() * 1000
    deadline = System.monotonic_time(:millisecond) + total

    workers =
      for w <- 1..16 do
        Task.async(fn -> worker(pool, path, w, deadline) end)
      end

    # sample memory in the second half of the run
    Process.sleep(div(total, 2))
    collect_everything()
    middle = snapshot()
    Process.sleep(max(div(total, 2) - 1_000, 0))
    collect_everything()
    late = snapshot()

    Enum.each(workers, &Task.await(&1, :infinity))
    send(probe, :stop)
    assert_receive {:probe, worst_lateness}, 5_000

    GenServer.stop(pool)
    collect_everything()
    final = snapshot()

    IO.puts(
      "\nsoak: baseline #{inspect(baseline)}\n      middle #{inspect(middle)}\n" <>
        "      late #{inspect(late)}\n      final #{inspect(final)}\n" <>
        "      worst timer lateness #{worst_lateness} ms"
    )

    # every connection and statement resource is gone
    assert final.conns == baseline.conns
    assert final.stmts == baseline.stmts
    # memory in the second half of the run doesn't keep growing
    assert late.memory < middle.memory * 1.5 + 50_000_000
    assert late.rss < middle.rss * 1.5 + 100_000_000
    assert final.processes <= baseline.processes + 5
    # no scheduler was blocked for long (dirty NIFs, cleanup off-scheduler)
    assert worst_lateness < 2_000
  end
end
