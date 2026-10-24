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
    assert %{failed: [{"a", :crashed}]} = Task.await(run, 5_000)
  end
end
