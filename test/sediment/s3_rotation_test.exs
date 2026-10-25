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

  test "a key_for ring reopens a clean close's warm copy, and a no-op rekey keeps it",
       %{s3: s3, dir: dir} do
    path = Path.join(dir, "warm.db")
    sidecar = Path.join(dir, ".warm.db.s3-warm")

    gets = fn ->
      case S3.request_counts() do
        %{"GetObject" => %{count: count}} -> count
        _ -> 0
      end
    end

    db = open!(path, s3, ["r1"])
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")

    # 50 sync commits: a full restore downloads the snapshot and 50 log
    # objects.
    for _ <- 1..50, do: :ok = Engine.execute(db, "INSERT INTO t (v) VALUES (randomblob(2000))")
    :ok = Engine.close(db)
    assert File.exists?(sidecar)

    # The first round needs r1 (KeyNeeded) and keeps the copy warm for the
    # second.
    before = gets.()
    {:ok, db} = Engine.open(path, s3: s3, encryption: [key_for: &key/1, active: "r1"])
    reads = gets.() - before
    :ok = Engine.close(db)
    assert reads < 10, "a key_for open of a warm copy read #{reads} objects"
    assert File.exists?(sidecar)

    assert {:ok, :current} = S3.rekey(path, s3, encryption: ring(["r1"]))
    assert File.exists?(sidecar)
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

  test "rekey and release_key refuse a local file open in this VM, whatever its prefix", %{
    s3: s3,
    dir: dir
  } do
    seed(s3, dir, ["a"])
    other = s3_opts(s3[:prefix] <> "-other")
    other_dir = Path.join(dir, "other")
    File.mkdir_p!(other_dir)
    seed(other, other_dir, ["b"])
    path = Path.join(dir, "busy.db")

    # Open with S3 under another prefix: the maintenance would restore over it.
    db = open!(path, s3, ["r1"])
    :ok = Engine.execute(db, "INSERT INTO t (v) VALUES ('c')")
    assert {:error, message} = S3.rekey(path, other, encryption: ring(["r2", "r1"]))
    assert message =~ "#{path} is open in this VM"

    assert {:error, message} =
             S3.release_key(path, other, "r1", encryption: ring(["r2", "r1"]))

    assert message =~ "#{path} is open in this VM"
    assert rows(db) == ["a", "c"]
    :ok = Engine.close(db)

    # Open without S3.
    {:ok, plain} = Engine.open(path, encryption: ring(["r1"]))
    assert {:error, message} = S3.rekey(path, other, encryption: ring(["r2", "r1"]))
    assert message =~ "#{path} is open in this VM"
    assert rows(plain) == ["a", "c"]
    :ok = Engine.close(plain)

    # Closed: it goes ahead.
    assert {:ok, %{from: "r1", to: "r2"}} = S3.rekey(path, other, encryption: ring(["r2", "r1"]))
  end

  test "a deep CHECK expression rekeys and restores: turso runs on its own stack", %{
    s3: s3,
    dir: dir
  } do
    # integrity_check and the schema parse compile every CHECK recursively:
    # on a dirty scheduler's stack, 90 terms would overflow it (and kill the
    # VM), under turso's expression depth limit.
    sum = Enum.map_join(1..90, " + ", fn _ -> "1" end)
    path = Path.join(dir, "deep.db")
    db = open!(path, s3, ["r1"])
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT CHECK (#{sum} > 0))")
    :ok = Engine.execute(db, "INSERT INTO t (v) VALUES ('deep')")
    :ok = Engine.close(db)

    assert {:ok, %{from: "r1", to: "r2"}} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    db = open!(path, s3, ["r2"])
    assert rows(db) == ["deep"]
    :ok = Engine.close(db)

    assert {:ok, "r2", ["deep"]} =
             restored_rows(Path.join(dir, "deep-restored.db"), s3, ["r2"], [])

    assert {:ok, %{key: "r1", epochs: [_ | _]}} =
             S3.release_key(path, s3, "r1", encryption: ring(["r2", "r1"]))
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

  # A rekey holds its file (and one dirty I/O scheduler) for its whole run.
  # Opens of that file fail at once, holding no scheduler: twice as many as
  # the VM has dirty I/O schedulers, retrying all along, leave the other
  # databases' queries running; a pool over the file backs off and connects
  # once the rekey is done.
  test "an open of a file a rekey holds fails at once; a pool connects after it", %{
    s3: s3,
    dir: dir
  } do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    both = ring(["r2", "r1"])
    task = paused_rekey(path, s3, both)

    {time, result} = :timer.tc(fn -> Engine.open(path, s3: s3, encryption: both) end)
    assert {:error, "busy: a rekey of this database is running; retry once it ends"} = result
    assert time < 500_000

    {:ok, other} = Engine.open(Path.join(dir, "other.db"))
    :ok = Engine.execute(other, "CREATE TABLE x (v)")
    deadline = System.monotonic_time(:millisecond) + 1_000

    openers =
      for _ <- 1..(2 * :erlang.system_info(:dirty_io_schedulers)) do
        Task.async(fn ->
          Stream.repeatedly(fn -> Engine.open(path, s3: s3, encryption: both) end)
          |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
          |> Enum.uniq()
        end)
      end

    for i <- 1..20 do
      {time, :ok} = :timer.tc(fn -> Engine.execute(other, "INSERT INTO x VALUES (#{i})") end)
      assert time < 500_000, "a query on another database waited #{div(time, 1000)} ms"
    end

    for task <- openers do
      assert [{:error, "busy: a rekey" <> _}] = Task.await(task, 10_000)
    end

    :ok = Engine.close(other)

    {:ok, pool} =
      Sediment.start_link(
        database: path,
        s3: s3,
        encryption: both,
        pool_size: 1,
        backoff_min: 50,
        backoff_max: 200
      )

    ExUnit.CaptureLog.capture_log(fn ->
      Process.sleep(300)
      assert {:error, _} = Sediment.query(pool, "SELECT v FROM t", [], timeout: 100)
      Native.s3_test_hook(s3, "publish", :release)
      assert {:ok, %{to: "r2"}} = Task.await(task, 30_000)
      assert %{rows: [["a"]]} = connected_query!(pool, "SELECT v FROM t", 100)
    end)

    GenServer.stop(pool)
  end

  # A pool reconnecting with backoff drops queued requests meanwhile.
  defp connected_query!(_pool, _sql, 0), do: flunk("the pool never connected")

  defp connected_query!(pool, sql, n) do
    case Sediment.query(pool, sql) do
      {:ok, result} ->
        result

      {:error, %DBConnection.ConnectionError{}} ->
        Process.sleep(100)
        connected_query!(pool, sql, n - 1)
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
    # Fenced, naming the key the manifest has now; only a re-run resolves it.
    assert {:error, {:fenced, %{current_key: "r1", fingerprint: fingerprint, message: message}}} =
             Task.await(task, 30_000)

    assert byte_size(fingerprint) == 16, "a fenced result shows 16 hex digits"
    assert refute_keys(message) =~ "s3 writer fenced"
    assert {:ok, :current} = S3.rekey(Path.join(dir, "r.db"), s3, encryption: ring(["r1", "r2"]))
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

  # A release whose delete fails committed: it reports the objects left
  # (orphans/1 lists them), a re-run deletes them, and only then is ok.
  test "a release whose delete fails reports :leftover; a re-run sweeps it", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    both = ring(["r2", "r1"])
    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: both)
    assert {:ok, %{objects: 0}} = S3.orphans(s3)

    :armed = Native.s3_test_hook(s3, "sweep", :store_fail)

    assert {:error, {:leftover, %{epochs: [_ | _] = epochs, objects: n, message: message}}} =
             S3.release_key(path, s3, "r1", encryption: both)

    assert n > 0
    assert message =~ "released epochs #{Enum.join(epochs, ", ")}; #{n} objects are left"
    assert message =~ "run release_key again"
    assert {:ok, %{current: "r2", retained: retained}} = S3.key_census(s3)
    refute Map.has_key?(retained, "r1")
    assert {:ok, %{objects: ^n, bytes: bytes, by_key: %{nil => ^n}}} = S3.orphans(s3)
    assert bytes > 0

    :armed = Native.s3_test_hook(s3, "sweep", :store_fail)

    assert {:error, {:leftover, %{epochs: [], objects: ^n}}} =
             S3.release_key(path, s3, "r1", encryption: both)

    assert {:ok, %{epochs: []}} = S3.release_key(path, s3, "r1", encryption: both)
    assert {:ok, %{objects: 0, bytes: 0, by_key: by_key}} = S3.orphans(s3)
    assert by_key == %{}
    assert {:ok, :no_database} = S3.orphans(s3_opts("rotation/none-#{System.os_time()}"))
  end

  # Released over many: the retried run (its objects deleted) reports the
  # database released, not unchanged.
  test "release_key_many retries a leftover and reports it released", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    both = ring(["r2", "r1"])
    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: both)
    :armed = Native.s3_test_hook(s3, "sweep", :store_fail)

    assert %{released: ["t"], failed: []} =
             S3.release_key_many([%{id: "t", path: path, s3: s3}], "r1",
               encryption: both,
               backoff: 0
             )

    assert {:ok, %{objects: 0}} = S3.orphans(s3)
  end

  test "the census lists fingerprints; check_ring tells the right key material without opening",
       %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert {:ok, census} = S3.key_census(s3)
    assert %{"r1" => fp1, "r2" => fp2} = census.fingerprints
    assert fp1 =~ ~r/\A[0-9a-f]{16}\z/ and fp2 =~ ~r/\A[0-9a-f]{16}\z/
    refute fp1 == fp2

    assert :ok = S3.check_ring(census, ring(["r2", "r1"]))
    assert :ok = S3.check_ring({:ok, census}, ring(["r1", "r2"]))
    assert {:error, [{"r1", :missing}]} = S3.check_ring(census, ring(["r2"]))

    swapped = [keys: [{"r2", @k2}, {"r1", @k3}], cipher: "aegis256"]
    assert {:error, [{"r1", :mismatch}]} = S3.check_ring(census, swapped)

    asked = [
      key_for: fn "r1" -> @k1 end,
      keys: [{"r2", @k2}],
      active: "r2",
      cipher: "aegis256"
    ]

    assert :ok = S3.check_ring(census, asked)
    assert :ok = S3.check_ring({:ok, :no_database}, false)

    many = S3.key_census_many([%{id: "t", s3: s3}])
    assert %{"t" => %{"r1" => ^fp1, "r2" => ^fp2}} = many.fingerprints
    assert :ok = S3.check_ring(many, ring(["r2", "r1"]))
    assert {:error, [{"r1", :mismatch}]} = S3.check_ring(many, swapped)
  end

  # The space a rekey or release checks before it downloads anything is
  # what space_needed/3 says; below it, the operation refuses, unchanged.
  test "rekey and release check the free space space_needed/3 names, before downloading",
       %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    path = Path.join(dir, "r.db")
    margin = 64 * 1024 * 1024

    assert {:ok, rekey} = S3.space_needed(path, s3, :rekey)
    assert {:ok, release} = S3.space_needed(path, s3, :release)
    assert rekey > release and release > margin

    on_exit(fn -> Native.s3_test_free_space(nil) end)
    :ok = Native.s3_test_free_space(rekey - 1)

    assert {:error, message} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))
    assert message =~ "rekey needs about #{rekey} bytes free in #{dir}"
    assert message =~ "only #{rekey - 1} are"
    refute File.exists?(path)
    assert {:ok, %{current: "r1"}} = S3.key_census(s3)

    :ok = Native.s3_test_free_space(nil)
    assert {:ok, %{to: "r2"}} = S3.rekey(path, s3, encryption: ring(["r2", "r1"]))

    # Warm now: the restore needs no snapshot.
    assert {:ok, warm} = S3.space_needed(path, s3, :release)
    assert {:ok, cold} = S3.space_needed(Path.join(dir, "cold.db"), s3, :release)
    assert warm < cold

    :ok = Native.s3_test_free_space(warm - 1)

    assert {:error, "s3 config: release_key needs about " <> _} =
             S3.release_key(path, s3, "r1", encryption: ring(["r2", "r1"]))

    assert {:ok, %{retained: %{"r1" => _}}} = S3.key_census(s3)
    assert {:ok, :no_database} = S3.space_needed(path, s3_opts("rotation/none"), :rekey)
  end

  test "keep_working_copy: false removes the working copy after a run", %{s3: s3, dir: dir} do
    seed(s3, dir, ["a"])
    both = ring(["r2", "r1"])

    files = fn path ->
      [
        path,
        path <> "-wal",
        Path.rootname(path) <> ".db-log",
        Path.join(dir, ".#{Path.basename(path)}.s3-warm")
      ]
    end

    kept = Path.join(dir, "kept.db")
    assert {:ok, %{to: "r2"}} = S3.rekey(kept, s3, encryption: both)
    assert File.exists?(kept) and File.exists?(Path.join(dir, ".kept.db.s3-warm"))

    for {name, run} <- [
          {"current.db", &S3.rekey(&1, s3, encryption: both, keep_working_copy: false)},
          {"released.db",
           &S3.release_key(&1, s3, "r1", encryption: both, keep_working_copy: false)}
        ] do
      path = Path.join(dir, name)
      assert {:ok, _} = run.(path)
      refute Enum.any?(files.(path), &File.exists?/1), name
    end

    assert %{rekeyed: [], current: ["t"]} =
             S3.rekey_many([%{id: "t", path: kept, s3: s3}],
               encryption: both,
               keep_working_copy: false
             )

    assert_raise ArgumentError, ~r/:keep_working_copy must be true or false/, fn ->
      S3.rekey(kept, s3, encryption: both, keep_working_copy: :no)
    end

    # Nothing lost: the data is in S3.
    assert {:ok, "r2", ["a"]} = restored_rows(Path.join(dir, "check.db"), s3, ["r2"], [])
  end
end
