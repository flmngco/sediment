defmodule Sediment.S3RotationManyTest do
  # Key rotation over many databases (Sediment.S3.rekey_many/2,
  # release_key_many/3, key_census_many/2) on a local S3 server:
  # mix test --only s3
  use ExUnit.Case, async: false

  alias Sediment.Engine
  alias Sediment.Native
  alias Sediment.S3

  @moduletag :s3

  @k1 String.duplicate("11", 32)
  @k2 String.duplicate("22", 32)

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  setup do
    base = "rotation-many/#{System.unique_integer([:positive])}-#{System.os_time()}"
    dir = Path.join(System.tmp_dir!(), "s3-rotation-many-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    %{base: base, dir: dir}
  end

  defp s3_opts(prefix) do
    [
      bucket: Sediment.S3Bucket.name(),
      prefix: prefix,
      endpoint: Sediment.S3Bucket.endpoint(),
      region: Sediment.S3Bucket.region(),
      access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
      secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
      owner: "rotation-many",
      lease_ttl_ms: 5_000,
      durability: :sync
    ]
  end

  defp ring(ids), do: [keys: Enum.map(ids, &{&1, key(&1)}), cipher: "aegis256"]

  defp key("r1"), do: @k1
  defp key("r2"), do: @k2

  # `n` databases under key r1, each with one row naming it; their targets.
  defp tenants(n, base, dir) do
    for i <- 1..n do
      target = %{id: "t#{i}", path: Path.join(dir, "t#{i}.db"), s3: s3_opts("#{base}/t#{i}")}

      {:ok, db} =
        Engine.open(Path.join(dir, "seed-t#{i}.db"), s3: target.s3, encryption: ring(["r1"]))

      :ok = Engine.execute(db, "CREATE TABLE t (v TEXT)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES ('t#{i}')")
      :ok = Engine.close(db)
      target
    end
  end

  defp ids(targets), do: Enum.map(targets, & &1.id)

  defp rows(target, ids) do
    {:ok, db} = Engine.open(target.path, s3: target.s3, encryption: ring(ids))
    {:ok, stmt} = Engine.prepare(db, "SELECT v FROM t")
    {:ok, rows} = Engine.fetch_all(db, stmt)
    :ok = Engine.release(db, stmt)
    :ok = Engine.close(db)
    List.flatten(rows)
  end

  # The rekey and release attempts per path (one telemetry span each).
  defp count_attempts do
    test = self()
    handler = {__MODULE__, test}

    :telemetry.attach_many(
      handler,
      [[:sediment, :s3, :rekey, :start], [:sediment, :s3, :release_key, :start]],
      fn _event, _measurements, %{path: path}, _ -> send(test, {:attempt, path}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp attempts(path) do
    receive do
      {:attempt, ^path} -> 1 + attempts(path)
    after
      0 -> 0
    end
  end

  test "rekeys 20 databases four at a time; a re-run finds them current; release; census",
       %{base: base, dir: dir} do
    targets = tenants(20, base, dir)
    all = ids(targets)

    assert %{keys: %{"r1" => %{current: ^all, retained: []}}, unreadable: []} =
             census = S3.key_census_many(targets)

    refute S3.removable?(census, "r1")
    assert S3.removable?(census, "r2")

    assert %{rekeyed: ^all, current: [], absent: [], failed: [], skipped: []} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]), max_concurrency: 4)

    for target <- targets, do: assert(rows(target, ["r2"]) == [target.id])

    census = S3.key_census_many(targets)

    assert %{"r2" => %{current: ^all, retained: []}, "r1" => %{current: [], retained: ^all}} =
             census.keys

    refute S3.removable?(census, "r1")

    assert %{rekeyed: [], current: ^all, failed: []} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]), max_concurrency: 4)

    assert %{released: ^all, unchanged: [], failed: []} =
             S3.release_key_many(targets, "r1",
               encryption: ring(["r2", "r1"]),
               max_concurrency: 4
             )

    census = S3.key_census_many(targets)
    assert census.keys == %{"r2" => %{current: all, retained: []}}
    assert census.targets == all
    assert S3.removable?(census, "r1")
    assert S3.removable?(census, "r1", current: "r2")

    assert %{released: [], unchanged: ^all, failed: []} =
             S3.release_key_many(targets, "r1", encryption: ring(["r2"]))
  end

  test "a refused database is reported, not retried; the others are rekeyed",
       %{base: base, dir: dir} do
    [t1, t2, t3] = targets = tenants(3, base, dir)
    {:ok, open} = Engine.open(t2.path, s3: t2.s3, encryption: ring(["r1"]))
    count_attempts()

    {time, result} =
      :timer.tc(fn ->
        S3.rekey_many(targets, encryption: ring(["r2", "r1"]), backoff: 5_000, attempts: 3)
      end)

    assert %{rekeyed: ["t1", "t3"], failed: [{"t2", reason}]} = result
    assert reason =~ "open in this VM"
    assert attempts(t2.path) == 1
    assert time < 5_000_000
    :ok = Engine.close(open)

    assert rows(t1, ["r2"]) == ["t1"]
    assert rows(t3, ["r2"]) == ["t3"]

    # The re-run finishes it.
    assert %{rekeyed: ["t2"], current: ["t1", "t3"], failed: []} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]))
  end

  test "a transient fault at the publication is retried, and the rekey done",
       %{base: base, dir: dir} do
    [_t1, t2] = targets = tenants(2, base, dir)
    count_attempts()
    :armed = Native.s3_test_hook(t2.s3, "publish", :store_fail)
    test = self()

    assert %{rekeyed: ["t1", "t2"], failed: []} =
             S3.rekey_many(targets,
               encryption: ring(["r2", "r1"]),
               backoff: 10,
               on_result: fn target, result -> send(test, {:result, target.id, result}) end
             )

    assert attempts(t2.path) == 2
    assert_received {:result, "t2", {:ok, %{from: "r1", to: "r2"}}}
    assert rows(t2, ["r2"]) == ["t2"]
  end

  # A crash at the publication (fenced, retried) leaves the lease taken, so
  # the retry is refused ("lease held"): the failure keeps both reasons, the
  # first saying the rekey may have switched.
  test "a retried failure reports every attempt's reason", %{base: base, dir: dir} do
    [t1] = targets = tenants(1, base, dir)
    :armed = Native.s3_test_hook(t1.s3, "publish", :crash)

    assert %{failed: [{"t1", {:attempts, [first, last]}}]} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]), backoff: 10)

    assert first =~ "s3 writer fenced: test hook: crashed at publish"
    assert last =~ "s3 lease held"
  end

  test "around: runs once per target, around its rekey; it can refuse one; stop_on: stops",
       %{base: base, dir: dir} do
    targets = tenants(3, base, dir)
    test = self()

    around = fn target, rekey ->
      send(test, {:before, target.id})

      if target.id == "t3" do
        {:error, :kept_open}
      else
        result = rekey.()
        send(test, {:after, target.id, result})
        result
      end
    end

    assert %{rekeyed: ["t1", "t2"], failed: [{"t3", :kept_open}]} =
             S3.rekey_many(targets,
               encryption: fn _target -> ring(["r2", "r1"]) end,
               around: around
             )

    for id <- ["t1", "t2"] do
      assert_received {:before, ^id}
      assert_received {:after, ^id, {:ok, %{to: "r2"}}}
    end

    assert_received {:before, "t3"}
    refute_received {:before, _}
    refute_received {:after, _, _}

    # Stopped after the first result: the rest not started.
    assert %{current: ["t1"], skipped: ["t2", "t3"]} =
             S3.rekey_many(targets,
               encryption: ring(["r2", "r1"]),
               stop_on: fn "t1", {:ok, :current} -> true end
             )
  end

  # A tenant's reopen failing after its rekey, and a process linked to
  # around: exiting, each fail their target alone, the rekey's outcome
  # kept; the caller, not trapping exits, gets the report.
  test "around: failing after the rekey, or its process exiting, fails that target alone",
       %{base: base, dir: dir} do
    targets = tenants(4, base, dir)
    test = self()

    around = fn
      %{id: "t1"}, rekey ->
        rekey.()
        {:error, :tenant_reopen_failed}

      %{id: "t2"}, rekey ->
        try do
          rekey.()
        after
          raise "reopen failed"
        end

      %{id: "t3"}, rekey ->
        spawn_link(fn -> exit(:tenant_worker_failed) end)
        rekey.()

      _target, rekey ->
        rekey.()
    end

    {caller, ref} =
      spawn_monitor(fn ->
        report =
          S3.rekey_many(targets,
            encryption: ring(["r2", "r1"]),
            max_concurrency: 2,
            around: around
          )

        send(test, {:report, report})
      end)

    assert_receive {:report, report}, 60_000
    assert_receive {:DOWN, ^ref, :process, ^caller, :normal}

    assert %{
             rekeyed: ["t4"],
             failed: [
               {"t1", {:around_failed, :tenant_reopen_failed, {:ok, %{from: "r1", to: "r2"}}}},
               {"t2",
                {:around_failed, {:callback_raised, :around, RuntimeError},
                 {:ok, %{from: "r1", to: "r2"}}}},
               {"t3", {:linked_exit, {:ok, %{from: "r1", to: "r2"}}}}
             ],
             skipped: []
           } = report

    # The rekeys whose reopen failed happened: a re-run finds them current.
    assert %{current: ["t1", "t2", "t3", "t4"], failed: []} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]))
  end

  # The rekeys running at once (one telemetry span each), at most.
  defp most_at_once do
    counts = :atomics.new(2, [])
    handler = {__MODULE__, :at_once, self()}

    :telemetry.attach_many(
      handler,
      [[:sediment, :s3, :rekey, :start], [:sediment, :s3, :rekey, :stop]],
      fn
        [_, _, _, :start], _, _, _ ->
          now = :atomics.add_get(counts, 1, 1)
          if now > :atomics.get(counts, 2), do: :atomics.put(counts, 2, now)

        [_, _, _, :stop], _, _, _ ->
          :atomics.sub(counts, 1, 1)
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    fn -> :atomics.get(counts, 2) end
  end

  # A query on a database of its own, again and again until told: the
  # slowest one's time, in ms.
  defp slowest_query(dir) do
    test = self()

    spawn_link(fn ->
      {:ok, db} = Engine.open(Path.join(dir, "other.db"))
      send(test, {:slowest, query_loop(db, 0)})
      Engine.close(db)
    end)
  end

  defp query_loop(db, slowest) do
    {micros, :ok} = :timer.tc(fn -> Engine.execute(db, "SELECT 1") end)
    slowest = max(slowest, div(micros, 1000))

    receive do
      :done -> slowest
    after
      5 -> query_loop(db, slowest)
    end
  end

  # More targets than dirty I/O schedulers, each with a linked helper
  # exiting mid-rekey: each worker finishes its rekey before its slot is
  # handed on, so one rekey runs at a time and other queries stay fast.
  test "a linked exit mid-rekey never frees the slot of a rekey still running",
       %{base: base, dir: dir} do
    n = :erlang.system_info(:dirty_io_schedulers) + 2
    targets = tenants(n, base, dir)
    at_once = most_at_once()
    querier = slowest_query(dir)

    around = fn _target, rekey ->
      spawn_link(fn ->
        Process.sleep(5)
        exit(:tenant_worker_failed)
      end)

      rekey.()
    end

    report =
      S3.rekey_many(targets,
        encryption: ring(["r2", "r1"]),
        max_concurrency: 1,
        around: around
      )

    send(querier, :done)
    assert_receive {:slowest, slowest}, 10_000

    assert %{failed: failed, skipped: []} = report
    assert length(failed) == n
    for {_id, reason} <- failed, do: assert({:linked_exit, {:ok, %{to: "r2"}}} = reason)
    assert at_once.() == 1
    assert slowest < 2_000
  end

  # Killed mid-rekey, a worker may leave its native rekey running: no
  # further target starts; once that rekey ends, a re-run finishes.
  test "a worker killed mid-rekey stops the run, the rest skipped", %{base: base, dir: dir} do
    [t1 | _] = targets = tenants(3, base, dir)
    test = self()

    around = fn %{id: id}, rekey ->
      send(test, {:rekeying, id, self()})
      rekey.()
    end

    run =
      Task.async(fn ->
        S3.rekey_many(targets, encryption: ring(["r2", "r1"]), around: around)
      end)

    assert_receive {:rekeying, "t1", worker}, 10_000
    Process.sleep(20)
    Process.exit(worker, :kill)

    assert %{failed: [{"t1", {:crashed, why}}], skipped: ["t2", "t3"]} =
             Task.await(run, 30_000)

    assert why =~ "may still be running"
    refute_received {:rekeying, _, _}

    # Once t1's rekey ends (a re-run refused as busy meanwhile).
    assert eventually(fn ->
             match?(%{failed: []}, S3.rekey_many([t1], encryption: ring(["r2", "r1"])))
           end)

    assert %{failed: []} = S3.rekey_many(targets, encryption: ring(["r2", "r1"]))
  end

  defp eventually(fun, tries \\ 100) do
    cond do
      fun.() -> true
      tries == 1 -> false
      true -> Process.sleep(200) && eventually(fun, tries - 1)
    end
  end

  # The pre-check: a target that needs nothing is reported without around:
  # (which would close it for nothing), one whose ring holds other key
  # material under a recorded id fails before it.
  test "a re-run calls around: only for the targets that need it", %{base: base, dir: dir} do
    targets = [t1, t2, t3] = tenants(3, base, dir)
    test = self()

    around = fn target, run ->
      send(test, {:around, target.id})
      run.()
    end

    assert %{rekeyed: ["t1", "t2"]} =
             S3.rekey_many([t1, t2], encryption: ring(["r2", "r1"]), around: around)

    flush_around()

    assert %{current: ["t1", "t2"], rekeyed: ["t3"], failed: []} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]), around: around)

    assert flush_around() == ["t3"]

    assert %{current: ["t1", "t2", "t3"]} =
             S3.rekey_many(targets, encryption: ring(["r2", "r1"]), around: around)

    assert flush_around() == []

    # Released once; a re-run needs nothing.
    assert %{released: ["t1", "t2", "t3"]} =
             S3.release_key_many(targets, "r1", encryption: ring(["r2", "r1"]), around: around)

    assert Enum.sort(flush_around()) == ["t1", "t2", "t3"]

    assert %{unchanged: ["t1", "t2", "t3"], failed: []} =
             S3.release_key_many(targets, "r1", encryption: ring(["r2", "r1"]), around: around)

    assert flush_around() == []
  end

  test "a ring with other key material under a recorded id fails before around:",
       %{base: base, dir: dir} do
    [t1, t2] = tenants(2, base, dir)
    test = self()

    around = fn target, run ->
      send(test, {:around, target.id})
      run.()
    end

    assert %{rekeyed: ["t1"]} = S3.rekey_many([t1], encryption: ring(["r2", "r1"]))

    # Under r2 already, the ring's r2 isn't the recorded one; under r1, the
    # ring's r1 isn't.
    other = String.duplicate("33", 32)
    wrong_r2 = [keys: [{"r2", other}, {"r1", @k1}], cipher: "aegis256"]
    wrong_r1 = [keys: [{"r2", @k2}, {"r1", other}], cipher: "aegis256"]

    for {target, ring, id} <- [{t1, wrong_r2, "r2"}, {t2, wrong_r1, "r1"}] do
      assert %{failed: [{_, message}]} =
               S3.rekey_many([target], encryption: ring, around: around, backoff: 0)

      assert message =~ "the ring's key #{id} (fingerprint "
      assert message =~ "is not the key the database records under #{id}"
      refute message =~ other
    end

    assert flush_around() == []
    assert rows(t2, ["r1"]) == ["t2"]
  end

  defp flush_around do
    receive do
      {:around, id} -> [id | flush_around()]
    after
      0 -> []
    end
  end

  # A wrong prefix (an unset environment variable, say) reads every target
  # absent, with nothing failed: the key the real databases need is never
  # reported removable.
  test "a misaddressed fleet never makes the old key removable", %{base: base, dir: dir} do
    real = tenants(2, base, dir)
    wrong = for t <- real, do: %{t | s3: s3_opts("#{base}-wrong/#{t.id}")}

    assert %{rekeyed: [], absent: ["t1", "t2"], failed: []} =
             S3.rekey_many(wrong, encryption: ring(["r2", "r1"]))

    census = S3.key_census_many(wrong)
    assert %{keys: keys, absent: ["t1", "t2"], targets: ["t1", "t2"]} = census
    assert keys == %{}
    refute S3.removable?(census, "r1")
    refute S3.removable?(census, "r1", current: "r2")

    # One real database rekeyed, one target misaddressed: still kept.
    [t1, _t2] = real
    assert %{rekeyed: ["t1"]} = S3.rekey_many([t1], encryption: ring(["r2", "r1"]))
    %{failed: []} = S3.release_key_many([t1], "r1", encryption: ring(["r2"]))
    mixed = S3.key_census_many([t1, List.last(wrong)])
    assert %{absent: ["t2"]} = mixed
    refute S3.removable?(mixed, "r1")
    refute S3.removable?(mixed, "r1", current: "r2")
  end

  test "an empty location is reported absent", %{base: base, dir: dir} do
    target = %{id: "none", path: Path.join(dir, "none.db"), s3: s3_opts("#{base}/none")}

    assert %{absent: ["none"], failed: []} =
             S3.rekey_many([target], encryption: ring(["r2", "r1"]))

    assert %{absent: ["none"], keys: keys} = S3.key_census_many([target])
    assert keys == %{}
  end
end
