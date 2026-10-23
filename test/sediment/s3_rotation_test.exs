defmodule Sediment.S3RotationTest do
  # Key rotation end to end on a local S3 server: mix test --only s3
  use ExUnit.Case, async: false

  alias Sediment.Engine
  alias Sediment.Native
  alias Sediment.S3

  @moduletag :s3

  @k1 String.duplicate("11", 32)
  @k2 String.duplicate("22", 32)
  @k3 String.duplicate("33", 32)

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  setup do
    prefix = "rotation/#{System.unique_integer([:positive])}-#{System.os_time()}"
    dir = Path.join(System.tmp_dir!(), "s3-rotation-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    %{s3: s3_opts(prefix), dir: dir}
  end

  defp s3_opts(prefix, extra \\ []) do
    Keyword.merge(
      [
        bucket: Sediment.S3Bucket.name(),
        prefix: prefix,
        endpoint: Sediment.S3Bucket.endpoint(),
        region: Sediment.S3Bucket.region(),
        access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
        secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
        owner: "rotation",
        lease_ttl_ms: 5_000,
        durability: :sync
      ],
      extra
    )
  end

  defp ring(ids), do: [keys: Enum.map(ids, &{&1, key(&1)}), cipher: "aegis256"]

  defp key("r1"), do: @k1
  defp key("r2"), do: @k2
  defp key("r3"), do: @k3

  defp open!(path, s3, ids) do
    {:ok, db} = Engine.open(path, s3: s3, encryption: ring(ids))
    db
  end

  defp rows(db) do
    {:ok, stmt} = Engine.prepare(db, "SELECT v FROM t ORDER BY id")
    {:ok, rows} = Engine.fetch_all(db, stmt)
    :ok = Engine.release(db, stmt)
    List.flatten(rows)
  end

  defp seed(s3, dir, values) do
    db = open!(Path.join(dir, "seed.db"), s3, ["r1"])
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    Enum.each(values, &(:ok = Engine.execute(db, "INSERT INTO t (v) VALUES ('#{&1}')")))
    :ok = Engine.close(db)
  end

  defp insert(s3, dir, ids, value) do
    db = open!(Path.join(dir, "w-#{System.unique_integer([:positive])}.db"), s3, ids)
    :ok = Engine.execute(db, "INSERT INTO t (v) VALUES ('#{value}')")
    :ok = Engine.close(db)
  end

  defp restored_rows(path, s3, ids, opts) do
    with {:ok, info} <- S3.restore(path, s3, [encryption: ring(ids)] ++ opts) do
      {:ok, db} = Engine.open(path, encryption: ring([info.key]))
      rows = rows(db)
      :ok = Engine.close(db)
      {:ok, info.key, rows}
    end
  end

  defp refute_keys(message) when is_binary(message) do
    for k <- [@k1, @k2, @k3], do: refute(message =~ String.slice(k, 0, 16))
    message
  end

  # A store timestamp has a resolution of one second.
  defp moment do
    Process.sleep(1_100)
    at = DateTime.utc_now()
    Process.sleep(1_100)
    at
  end

  test "rekey round trip, the census and PITR on both sides of it", %{s3: s3, dir: dir} do
    s3 = Keyword.put(s3, :retain_epochs, 4)
    seed(s3, dir, ["a"])
    before = moment()
    path = Path.join(dir, "rekey.db")

    assert {:ok, %{from: "r1", to: "r2", epoch: _}} =
             S3.rekey(path, s3, encryption: ring(["r2", "r1"]))

    assert {:ok, :current} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert {:ok, %{current: "r2", retained: %{"r1" => [_]}}} = S3.key_census(s3)

    insert(s3, dir, ["r2"], "b")
    after_rekey = moment()

    assert {:ok, "r1", ["a"]} =
             restored_rows(Path.join(dir, "p1.db"), s3, ["r2", "r1"], at: before)

    assert {:ok, "r2", ["a", "b"]} =
             restored_rows(Path.join(dir, "p2.db"), s3, ["r2", "r1"], at: after_rekey)

    assert {:error, message} =
             S3.restore(Path.join(dir, "p3.db"), s3, encryption: ring(["r2"]), at: before)

    assert refute_keys(message) =~ "needs key r1, which is not in the ring"

    # The old ring alone no longer opens it.
    assert {:error, message} =
             Engine.open(Path.join(dir, "old.db"), s3: s3, encryption: ring(["r1"]))

    assert refute_keys(message) =~ "unknown key id r2 (ring: r1)"
  end

  test "ring errors are refused by name", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    swapped = [keys: [{"r1", @k2}, {"r2", @k1}]]

    assert {:error, message} = Engine.open(Path.join(dir, "x.db"), s3: s3, encryption: swapped)

    assert refute_keys(message) =~
             "key r1 in the ring is not the key that encrypted this database"

    assert {:error, message} =
             Engine.open(Path.join(dir, "y.db"), s3: s3, encryption: ring(["r2", "r3"]))

    assert refute_keys(message) =~ "unknown key id r1 (ring: r2, r3)"

    assert {:error, message} =
             Engine.open(Path.join(dir, "z.db"), s3: s3, encryption: [keys: [{"R1", @k1}]])

    assert refute_keys(message) =~ "invalid key id"

    assert {:error, message} =
             Engine.open(Path.join(dir, "z.db"),
               s3: s3,
               encryption: [keys: [{"r1", @k1}, {"r2", @k1}]]
             )

    assert refute_keys(message) =~ "are the same key"
  end

  test "key_for is asked only for the keys an open needs", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    test = self()

    key_for = fn id ->
      send(test, {:key_for, id})
      if id in ["r1", "r2"], do: key(id)
    end

    lazy = [key_for: key_for, active: "r2"]
    {:ok, db} = Engine.open(Path.join(dir, "lazy.db"), s3: s3, encryption: lazy)
    assert rows(db) == ["a"]
    :ok = Engine.close(db)
    assert_received {:key_for, "r1"}
    refute_received {:key_for, _}

    assert {:ok, %{from: "r1", to: "r2"}} =
             S3.rekey(Path.join(dir, "r.db"), s3, encryption: lazy)

    # key_for has no key for r2: the open names it.
    assert {:error, message} =
             Engine.open(Path.join(dir, "lazy2.db"),
               s3: s3,
               encryption: [key_for: {__MODULE__, :only_r1, []}, active: "r1"]
             )

    assert refute_keys(message) =~ "unknown key id r2"
  end

  @doc false
  def only_r1("r1"), do: {:ok, @k1}
  def only_r1(_id), do: nil

  test "a VM-shared storage refuses an open whose ring lacks its key", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "shared.db")
    db = open!(path, s3, ["r1"])
    assert {:error, message} = Engine.open(path, s3: s3, encryption: ring(["r2"]))
    assert refute_keys(message) =~ "under key r1, which is not in the ring"
    other = open!(path, s3, ["r2", "r1"])
    :ok = Engine.close(other)
    # Open here: a rekey waits for no one.
    assert {:error, message} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert message =~ "is open in this VM"
    :ok = Engine.close(db)
  end

  defp paused_rekey(path, s3, ring) do
    :armed = Native.s3_test_hook(s3, "publish", :pause)
    task = Task.async(fn -> S3.rekey(path, s3, encryption: ring) end)
    wait_paused(s3, 300)
    task
  end

  defp wait_paused(_s3, 0), do: flunk("the rekey never reached its publication")

  defp wait_paused(s3, n) do
    unless Native.s3_test_hook(s3, "publish", :state) == :paused do
      Process.sleep(20)
      wait_paused(s3, n - 1)
    end
  end

  test "a takeover during a rekey fences it: nothing switched, no commit lost", %{
    s3: s3,
    dir: dir
  } do
    seed(s3, dir, ["before"])
    task = paused_rekey(Path.join(dir, "r.db"), s3, ring(["r2", "r1"]))

    # Another writer (the same owner takes over at once) commits meanwhile.
    insert(s3, dir, ["r1"], "during")

    Native.s3_test_hook(s3, "publish", :release)
    assert {:error, message} = Task.await(task, 30_000)
    assert message =~ "fenced"
    assert {:ok, %{current: "r1", retained: %{}}} = S3.key_census(s3)
    db = open!(Path.join(dir, "check.db"), s3, ["r1"])
    assert rows(db) == ["before", "during"]
    :ok = Engine.close(db)
  end

  test "a crash before the publication leaves the old key working; a re-run succeeds", %{
    s3: s3,
    dir: dir
  } do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    :armed = Native.s3_test_hook(s3, "publish", :crash)
    assert {:error, message} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert message =~ "crashed"
    assert {:ok, %{current: "r1"}} = S3.key_census(s3)

    assert {:error, message} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert message =~ "lease held"

    insert(s3, dir, ["r1"], "b")

    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    db = open!(path, s3, ["r2"])
    assert rows(db) == ["a", "b"]
    :ok = Engine.close(db)
  end

  test "a replica picks up a rekey at its next refresh", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    replica_opts = Keyword.merge(s3, mode: :replica, owner: "reader")

    {:ok, replica} =
      Engine.open(Path.join(dir, "replica.db"), s3: replica_opts, encryption: ring(["r2", "r1"]))

    assert rows(replica) == ["a"]

    assert {:ok, %{to: "r2"}} =
             S3.rekey(Path.join(dir, "w.db"), s3, encryption: ring(["r2", "r1"]))

    insert(s3, dir, ["r2"], "b")
    assert {:ok, _} = S3.refresh(replica)
    assert rows(replica) == ["a", "b"]
    :ok = Engine.close(replica)

    assert {:error, message} =
             Engine.open(Path.join(dir, "old.db"), s3: replica_opts, encryption: ring(["r1"]))

    assert refute_keys(message) =~ "needs key r2, which is not in the ring"
  end

  test "retention keeps the old key's epochs until release_key; a gap is refused", %{
    s3: s3,
    dir: dir
  } do
    seed(s3, dir, ["under r1"])
    path = Path.join(dir, "r.db")

    # retain_epochs 0: exactly the rekeyed epoch stays.
    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert {:ok, %{current: "r2", retained: %{"r1" => [_]}}} = S3.key_census(s3)

    s3 = Keyword.put(s3, :retain_epochs, 4)
    insert(s3, dir, ["r2"], "under r2")
    during_r2 = moment()
    assert {:ok, %{to: "r3"}} = S3.rekey(path, s3, encryption: ring(["r3", "r2", "r1"]))
    assert {:ok, %{current: "r3", retained: retained}} = S3.key_census(s3)
    assert Map.keys(retained) == ["r1", "r2"]

    assert {:error, message} = S3.release_key(path, s3, "r3", encryption: ring(["r3"]))
    assert message =~ "key r3 is the current key"

    assert {:ok, %{key: "r2", epochs: [_ | _]}} =
             S3.release_key(path, s3, "r2", encryption: ring(["r3"]))

    assert {:ok, %{current: "r3", retained: retained}} = S3.key_census(s3)
    refute Map.has_key?(retained, "r2")

    assert {:error, message} =
             S3.restore(Path.join(dir, "gap.db"), s3,
               encryption: ring(["r3", "r2", "r1"]),
               at: during_r2
             )

    assert message =~ "is not covered by a retained epoch"

    assert {:ok, %{key: "r1", epochs: [_]}} =
             S3.release_key(path, s3, "r1", encryption: ring(["r3"]))

    assert {:ok, %{current: "r3", retained: retained}} = S3.key_census(s3)
    assert Map.keys(retained) -- ["r3"] == []

    # r2 is retired: back to it only with allow_reuse (its epochs are gone).
    assert {:error, message} = S3.rekey(path, s3, encryption: ring(["r2", "r3"]))
    assert message =~ "was retired"

    assert {:ok, %{to: "r2"}} =
             S3.rekey(path, s3, encryption: ring(["r2", "r3"]), allow_reuse: true)
  end

  test "key_census of an empty prefix, and of a destroyed database", %{s3: s3, dir: dir} do
    assert {:ok, :no_database} = S3.key_census(s3)
    seed(s3, dir, ["a"])
    assert {:ok, %{current: "r1", retained: %{}}} = S3.key_census(s3)
    assert {:ok, _} = S3.destroy(s3)
    assert {:ok, :no_database} = S3.key_census(s3)

    assert {:error, message} = S3.rekey(Path.join(dir, "r.db"), s3, encryption: ring(["r2"]))
    assert message =~ "no database"
  end

  test "an unencrypted database has no key to rotate", %{s3: s3, dir: dir} do
    plain = Keyword.put(s3, :encryption, false)
    {:ok, db} = Engine.open(Path.join(dir, "p.db"), s3: plain)
    :ok = Engine.close(db)
    assert {:ok, %{current: nil, retained: %{}}} = S3.key_census(s3)
    assert {:error, message} = S3.rekey(Path.join(dir, "r.db"), s3, encryption: ring(["r1"]))
    assert message =~ "is unencrypted"
  end
end
