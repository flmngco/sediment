defmodule Sediment.S3ManyTest do
  # Sediment.S3.removable?/3 on census results: never true while a target
  # was absent (a misaddressed fleet reads all absent) or unreadable.
  use ExUnit.Case, async: true

  alias Sediment.S3

  defp census(fields) do
    Map.merge(
      %{keys: %{}, absent: [], unencrypted: [], unreadable: [], targets: []},
      Map.new(fields)
    )
  end

  test "a key no target uses, every target read: removable" do
    census = census(keys: %{"r2" => %{current: ["a", "b"], retained: []}}, targets: ["a", "b"])
    assert S3.removable?(census, "r1")
    assert S3.removable?(census, "r1", current: "r2")
    refute S3.removable?(census, "r2")
  end

  test "a misaddressed fleet (every target absent) never makes a key removable" do
    census = census(absent: ["a", "b"], targets: ["a", "b"])
    refute S3.removable?(census, "r1")
    refute S3.removable?(census, "r1", current: "r2")
  end

  test "a census of no target never makes a key removable" do
    refute S3.removable?(census(targets: []), "r1")
    refute S3.removable?(census(targets: []), "r1", current: "r2")
  end

  test "one absent or unreadable target is enough to keep the key" do
    keys = %{"r2" => %{current: ["a"], retained: []}}
    refute S3.removable?(census(keys: keys, absent: ["b"], targets: ["a", "b"]), "r1")

    refute S3.removable?(
             census(keys: keys, unreadable: [{"b", "s3 store: ..."}], targets: ["a", "b"]),
             "r1"
           )
  end

  test "retained or current anywhere keeps the key" do
    keys = %{
      "r2" => %{current: ["a", "b"], retained: []},
      "r1" => %{current: [], retained: ["b"]}
    }

    refute S3.removable?(census(keys: keys, targets: ["a", "b"]), "r1")

    keys = %{"r2" => %{current: ["a"], retained: []}, "r1" => %{current: ["b"], retained: []}}
    refute S3.removable?(census(keys: keys, targets: ["a", "b"]), "r1")
  end

  test "current: needs every target under the new key" do
    # b unencrypted: r1 isn't needed, but not every target is under r2
    census =
      census(
        keys: %{"r2" => %{current: ["a"], retained: []}},
        unencrypted: ["b"],
        targets: ["a", "b"]
      )

    assert S3.removable?(census, "r1")
    refute S3.removable?(census, "r1", current: "r2")
    refute S3.removable?(census(targets: []), "r1", current: "r2")
  end

  test "anything but a census is refused" do
    assert_raise ArgumentError, ~r/expects the result of key_census_many/, fn ->
      S3.removable?(%{keys: %{}}, "r1")
    end
  end

  # A caller trapping exits gets the task's :DOWN, not its result: the run
  # reports the target instead of waiting forever.
  test "a target's task killed from outside is reported as crashed" do
    test = self()

    around = fn _target, _rekey ->
      send(test, {:task, self()})
      Process.sleep(:infinity)
    end

    run =
      Task.async(fn ->
        Process.flag(:trap_exit, true)
        ring = [keys: [{"r1", String.duplicate("ab", 32)}], cipher: "aegis256"]

        S3.rekey_many([%{id: "a", path: "/tmp/a.db", s3: [bucket: "b"]}],
          encryption: ring,
          around: around
        )
      end)

    assert_receive {:task, pid}, 5_000
    Process.exit(pid, :kill)
    assert %{failed: [{"a", {:crashed, why}}]} = Task.await(run, 5_000)
    assert why =~ "may still be running"
  end

  # A store that can't be reached: each rekey fails at once.
  defp target(id) do
    s3 = [
      bucket: "b",
      prefix: "p/#{id}",
      endpoint: "http://127.0.0.1:1",
      region: "us-east-1",
      access_key_id: "a",
      secret_access_key: "s"
    ]

    %{id: id, path: Path.join(Temp.path!(), "#{id}.db"), s3: s3}
  end

  defp ring, do: [keys: [{"r1", String.duplicate("ab", 32)}], cipher: "aegis256"]

  # In a process of its own, not trapping exits: its report, or how it died.
  defp run_in_caller(fun) do
    test = self()
    {pid, ref} = spawn_monitor(fn -> send(test, {:report, self(), fun.()}) end)

    receive do
      {:report, ^pid, report} -> report
      {:DOWN, ^ref, :process, ^pid, reason} -> {:caller_died, reason}
    after
      30_000 -> flunk("no report")
    end
  end

  # The worker traps exits: the rekey and around: run to their end (a
  # native rekey never outlives its slot), the target failed after.
  test "a process linked to around: exiting fails its target alone, once its rekey ends" do
    test = self()

    around = fn
      %{id: "a"}, rekey ->
        spawn_link(fn -> exit(:tenant_worker_failed) end)
        Process.sleep(50)

        try do
          rekey.()
        after
          send(test, :a_reopened)
        end

      _target, rekey ->
        Process.sleep(100)
        rekey.()
    end

    report =
      run_in_caller(fn ->
        S3.rekey_many([target("a"), target("b")],
          encryption: ring(),
          attempts: 1,
          max_concurrency: 2,
          around: around
        )
      end)

    assert %{
             failed: [{"a", {:linked_exit, {:error, "s3 " <> _}}}, {"b", "s3 " <> _}],
             skipped: []
           } =
             report

    assert_received :a_reopened
  end

  # Killed, a worker may leave its native operation running: its slot isn't
  # handed on.
  test "a worker killed stops further starts: its target crashed, the rest skipped" do
    test = self()

    around = fn %{id: id}, _rekey ->
      send(test, {:running, id, self()})
      Process.sleep(:infinity)
    end

    caller =
      Task.async(fn ->
        S3.rekey_many([target("a"), target("b"), target("c")],
          encryption: ring(),
          around: around
        )
      end)

    assert_receive {:running, "a", worker}, 5_000
    Process.exit(worker, :kill)

    assert %{failed: [{"a", {:crashed, _}}], skipped: ["b", "c"]} = Task.await(caller, 5_000)
    refute_received {:running, _, _}
  end

  # The caller dying leaves no database closed: the target running
  # finishes, around: reopening it, and no further one starts.
  test "a caller that dies leaves the targets running to finish; none further starts" do
    test = self()

    around = fn %{id: id}, rekey ->
      send(test, {:closed, id, self()})
      receive do: (:go -> :ok)

      try do
        rekey.()
      after
        send(test, {:reopened, id})
      end
    end

    caller =
      spawn(fn ->
        S3.rekey_many([target("a"), target("b")],
          encryption: ring(),
          attempts: 1,
          around: around
        )
      end)

    assert_receive {:closed, "a", worker}, 5_000
    ref = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^ref, :process, ^caller, :killed}
    send(worker, :go)
    assert_receive {:reopened, "a"}, 10_000
    refute_receive {:closed, "b", _}, 300
  end

  test "an around: failing after its rekey fails the target, the rekey's result kept" do
    for {around, reason} <- [
          {fn _, rekey ->
             rekey.()
             {:error, :reopen_failed}
           end, :reopen_failed},
          {fn _, rekey ->
             try do
               rekey.()
             after
               raise "reopen failed"
             end
           end, {:callback_raised, :around, RuntimeError}}
        ] do
      assert %{failed: [{"a", {:around_failed, ^reason, {:error, "s3 " <> _}}}]} =
               S3.rekey_many([target("a")], encryption: ring(), attempts: 1, around: around)
    end

    # Returning the rekey's own result: that result.
    assert %{failed: [{"a", "s3 " <> _}]} =
             S3.rekey_many([target("a")],
               encryption: ring(),
               attempts: 1,
               around: fn _, rekey -> rekey.() end
             )
  end
end
