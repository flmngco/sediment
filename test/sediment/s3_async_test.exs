defmodule Sediment.S3AsyncTest do
  # Async S3 durability (durability: :async) from the Elixir side: sync: true,
  # flush/2 and the info fields. A second node restoring while the writer is
  # still open shows what is durable.
  use ExUnit.Case, async: false

  alias Sediment.{Engine, S3}

  @moduletag :s3

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  defp s3(prefix, extra \\ []) do
    [
      bucket: Sediment.S3Bucket.name(),
      prefix: prefix,
      endpoint: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333"),
      region: Sediment.S3Bucket.region(),
      access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
      encryption: false,
      secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
      owner: "async-writer",
      durability: :async
    ]
    |> Keyword.merge(extra)
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "s3async-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    [
      dir: dir,
      prefix: "elixir/async/#{System.unique_integer([:positive])}-#{System.os_time()}"
    ]
  end

  defp restored_count(dir, s3, name) do
    path = Path.join(dir, name)
    {:ok, _} = S3.restore(path, s3)
    {:ok, db} = Engine.open(path)
    {:ok, stmt} = Engine.prepare(db, "select count(*) from t")
    {:ok, [[n]]} = Engine.fetch_all(db, stmt)
    :ok = Engine.close(db)
    n
  end

  test "sync: true transactions and statements are durable when they return", %{
    dir: dir,
    prefix: prefix
  } do
    s3 = s3(prefix)
    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3, pool_size: 1)
    Sediment.query!(pool, "create table t (x integer)", [], sync: true)

    {:ok, :done} =
      Sediment.transaction(
        pool,
        fn conn ->
          Sediment.query!(conn, "insert into t values (1)")
          :done
        end,
        sync: true
      )

    assert restored_count(dir, s3, "a.db") >= 1

    Sediment.query!(pool, "insert into t values (2)", [], sync: true)
    assert restored_count(dir, s3, "b.db") == 2
  end

  test "flush/2 makes everything committed so far durable; info reports the lag", %{
    dir: dir,
    prefix: prefix
  } do
    s3 = s3(prefix)
    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3, pool_size: 2)
    Sediment.query!(pool, "create table t (x integer)")
    for i <- 1..20, do: Sediment.query!(pool, "insert into t values (?)", [i])

    assert :ok = S3.flush(pool, 10_000)
    assert restored_count(dir, s3, "c.db") == 20

    assert {:ok, info} = S3.info(pool)
    assert info.durability == "async"
    assert info.durable_offset == info.committed_offset
    assert info.pending_bytes == 0
  end

  test "durability: :sync keeps every commit durable when it returns", %{
    dir: dir,
    prefix: prefix
  } do
    s3 = s3(prefix, durability: :sync)
    {:ok, db} = Engine.open(Path.join(dir, "w.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer); insert into t values (1)")
    assert restored_count(dir, s3, "d.db") == 1
    assert {:ok, %{durability: "sync"}} = S3.info(db)
    assert :ok = S3.flush(db, 1_000)
    :ok = Engine.close(db)
  end

  test "upload_interval_ms batches uploads; flush/2 doesn't wait for it", %{
    dir: dir,
    prefix: prefix
  } do
    s3 = s3(prefix, upload_interval_ms: 900)
    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "a.db"), s3: s3, pool_size: 1)
    Sediment.query!(pool, "create table t (x)")
    :ok = S3.flush(pool)
    for i <- 1..20, do: Sediment.query!(pool, "insert into t values (#{i})")
    {:ok, info} = S3.info(pool)
    assert info.pending_frames > 0
    {micros, :ok} = :timer.tc(fn -> S3.flush(pool) end)
    assert micros < 600_000
    assert restored_count(dir, s3, "r.db") == 20
    GenServer.stop(pool)

    assert {:error, %Sediment.Error{message: message}} =
             Sediment.Connection.connect(
               database: Path.join(dir, "b.db"),
               s3: s3(prefix <> "-x", upload_interval_ms: 1_000)
             )

    assert message =~ "below max_lag_ms"
  end

  test "durability defaults to :async", %{dir: dir, prefix: prefix} do
    s3 = Keyword.delete(s3(prefix), :durability)
    {:ok, db} = Engine.open(Path.join(dir, "default.db"), s3: s3)
    assert {:ok, %{durability: "async"}} = S3.info(db)
    :ok = Engine.close(db)
  end

  # Closing drains pending uploads (close_timeout_ms, twice); the pool's
  # shutdown must outlast that, or the database is still closing after
  # the pool stopped.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "stopping a pool returns once the database is closed, uploads pending or not", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.merge(s3(prefix),
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        # an upload attempt hangs instead of failing, so the drain waits
        request_timeout_ms: 60_000,
        max_retries: 0,
        # longer than DBConnection's default shutdown (5 s)
        close_timeout_ms: 6_000
      )

    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3, pool_size: 1)
    Sediment.query!(pool, "create table t (x integer)")
    :ok = S3.flush(pool, 10_000)
    :ok = Sediment.TcpProxy.cut(proxy)
    Sediment.query!(pool, "insert into t values (1)")

    {micros, :ok} = :timer.tc(fn -> GenServer.stop(pool) end)
    # the close waited out its drain instead of being killed at 5 s, and
    # nothing is left open when stop returns
    assert micros >= 5_900_000
    assert open_files(dir) == []
    :ok = Sediment.TcpProxy.resume(proxy)
  end

  # Under a supervisor (as in an application, or an Ecto repo), the pool's
  # own child spec must carry the shutdown: its supervisor would kill it at
  # the default 5 s, leaving the connections to close in the background.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a supervised pool (child_spec/1) also stops only once the database is closed", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.merge(s3(prefix),
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        request_timeout_ms: 60_000,
        max_retries: 0,
        close_timeout_ms: 6_000
      )

    opts = [database: Path.join(dir, "w.db"), s3: s3, pool_size: 1, name: :supervised_pool]
    {:ok, sup} = Supervisor.start_link([Sediment.child_spec(opts)], strategy: :one_for_one)
    Sediment.query!(:supervised_pool, "create table t (x integer)")
    :ok = S3.flush(:supervised_pool, 10_000)
    :ok = Sediment.TcpProxy.cut(proxy)
    Sediment.query!(:supervised_pool, "insert into t values (1)")

    {micros, :ok} = :timer.tc(fn -> Supervisor.stop(sup) end)
    assert micros >= 5_900_000
    assert open_files(dir) == []
    :ok = Sediment.TcpProxy.resume(proxy)
  end

  # A plain pool on a file whose S3 database is open here shares its storage,
  # so its sync: true must wait for S3 too.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "sync: true on a plain pool sharing an S3 database waits for S3", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.put(s3(prefix), :endpoint, "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}")

    path = Path.join(dir, "w.db")
    {:ok, s3_pool} = Sediment.start_link(database: path, s3: s3, pool_size: 1)
    Sediment.query!(s3_pool, "create table t (x integer)", [], sync: true)
    {:ok, plain} = Sediment.start_link(database: path, pool_size: 1)

    :ok = Sediment.TcpProxy.cut(proxy)

    spawn(fn ->
      Process.sleep(600)
      Sediment.TcpProxy.resume(proxy)
    end)

    {micros, _} =
      :timer.tc(fn -> Sediment.query!(plain, "insert into t values (1)", [], sync: true) end)

    assert micros >= 500_000
    assert restored_count(dir, Keyword.put(s3, :endpoint, s3(prefix)[:endpoint]), "r.db") == 1
    GenServer.stop(plain)
    GenServer.stop(s3_pool)
  end

  # Commits a fenced writer had queued are lost; its pool reconnects from
  # what is durable, and flush/2 must not then certify them.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "after a loss at a fence, flush and sync: true fail on the recovered pool until acknowledged",
       %{dir: dir, prefix: prefix} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.put(s3(prefix), :endpoint, "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}")

    {:ok, pool} =
      Sediment.start_link(
        database: Path.join(dir, "w.db"),
        s3: s3,
        pool_size: 1,
        idle_interval: 100,
        backoff_min: 50,
        backoff_max: 200
      )

    Sediment.query!(pool, "create table t (x integer)", [], sync: true)
    :ok = Sediment.TcpProxy.cut(proxy)
    for i <- 1..5, do: Sediment.query!(pool, "insert into t values (?)", [i])

    # a restarted node (same owner) takes over from what is durable
    {:ok, successor} = Engine.open(Path.join(dir, "successor.db"), s3: s3(prefix))
    :ok = Engine.close(successor)
    :ok = Sediment.TcpProxy.resume(proxy)

    # the pool gets fenced, reconnects, and keeps reporting the loss
    reason =
      Enum.find_value(1..200, fn _ ->
        with {:error, reason} when is_binary(reason) <- S3.flush(pool, 2_000),
             true <- reason =~ "were lost" do
          reason
        else
          _ -> Process.sleep(100) && nil
        end
      end)

    assert reason =~ "acknowledge_loss"
    assert {:ok, %{lost: %{durable_offset: _}}} = S3.info(pool)
    assert Sediment.query!(pool, "select count(*) from t").rows == [[0]]

    assert {:error, %Sediment.Error{message: message}} =
             Sediment.query(pool, "insert into t values (6)", [], sync: true)

    assert message =~ "were lost"

    # A read commits nothing: it doesn't wait for S3, and doesn't fail.
    assert {:ok, %{rows: [[_]]}} =
             Sediment.query(pool, "select count(*) from t", [], sync: true)

    assert {:ok, %{committed_offset: _}} = S3.acknowledge_loss(pool)
    assert {:ok, %{lost: nil}} = S3.info(pool)
    assert :ok = S3.flush(pool, 10_000)
    Sediment.query!(pool, "insert into t values (7)", [], sync: true)
    GenServer.stop(pool)
  end

  # A commit waiting for room in the upload queue (S3 down) is interrupted
  # by the caller's timeout, and the connection closes; whatever it left
  # behind, the local database and S3 agree once S3 is back: no commit
  # visible here that never gets there.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a commit stuck on backpressure times out; local and S3 agree afterwards", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.merge(s3(prefix),
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        max_lag_ms: 200,
        close_timeout_ms: 2_000
      )

    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3, pool_size: 1)
    Sediment.query!(pool, "create table t (x integer)", [], sync: true)
    :ok = Sediment.TcpProxy.cut(proxy)
    Sediment.query!(pool, "insert into t values (1)")
    Process.sleep(300)

    {micros, result} =
      :timer.tc(fn ->
        ExUnit.CaptureLog.capture_log(fn ->
          send(self(), Sediment.query(pool, "insert into t values (2)", [], timeout: 1_000))
        end)

        receive do: (result -> result)
      end)

    # the timeout closes the connection, whose drain of the pending uploads
    # the caller waits out: at most twice close_timeout_ms
    assert {:error, _} = result
    assert micros < (1_000 + 2 * 2_000 + 1_500) * 1_000

    :ok = Sediment.TcpProxy.resume(proxy)

    # the pool recovers (the close may have given up on the queued insert:
    # a loss to acknowledge), and what it shows is what S3 has
    local =
      Enum.find_value(1..100, fn _ ->
        with :ok <- flush_acknowledging_loss(pool),
             {:ok, %{rows: [[n]]}} <- Sediment.query(pool, "select count(*) from t") do
          {:ok, info} = S3.info(pool)
          assert info.durable_offset == info.committed_offset
          n
        else
          _ -> Process.sleep(100) && nil
        end
      end)

    # the cancelled insert is durable everywhere or nowhere: a cancelled commit's frame
    # used to stay in the local log only, replayed on reconnect but never uploaded
    assert local in [0, 1]
    direct = Keyword.put(s3, :endpoint, s3(prefix)[:endpoint])
    assert restored_count(dir, direct, "r.db") == local
    GenServer.stop(pool)
  end

  # The same without a pool: the cancelled commit is gone locally too, also
  # from what a reopen replays, and later commits reach S3 around it.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a commit cancelled on backpressure is not in the local log either", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.merge(s3(prefix),
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        max_lag_ms: 200
      )

    {:ok, db} = Engine.open(Path.join(dir, "w.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer)")
    :ok = S3.flush(db, 10_000)
    :ok = Sediment.TcpProxy.cut(proxy)
    :ok = Engine.execute(db, "insert into t values (1)")
    Process.sleep(300)

    spawn(fn ->
      Process.sleep(500)
      Engine.cancel(db)
    end)

    assert {:error, "s3 commit cancelled" <> _} = Engine.execute(db, "insert into t values (2)")
    :ok = Sediment.TcpProxy.resume(proxy)
    :ok = Engine.execute(db, "insert into t values (3)")
    :ok = S3.flush(db, 10_000)

    {:ok, stmt} = Engine.prepare(db, "select x from t order by x")
    {:ok, local} = Engine.fetch_all(db, stmt)
    assert local == [[1], [3]]

    direct = Keyword.put(s3, :endpoint, s3(prefix)[:endpoint])
    assert restored_count(dir, direct, "r.db") == 2
    :ok = Engine.close(db)

    # nor in the local log a reopen replays
    {:ok, db} = Engine.open(Path.join(dir, "w.db"))
    {:ok, stmt} = Engine.prepare(db, "select x from t order by x")
    assert Engine.fetch_all(db, stmt) == {:ok, [[1], [3]]}
    :ok = Engine.close(db)
  end

  # sync: true waits for what the statement or transaction itself
  # committed: a read commits nothing and never waits for S3, even with a
  # backlog and S3 unreachable, where it used to wait for (and fail with)
  # the whole backlog. A write still waits.
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "sync: true on reads returns at once; on writes it still waits for S3", %{
    dir: dir,
    prefix: prefix
  } do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(s3(prefix)[:endpoint]).port)

    s3 =
      Keyword.put(s3(prefix), :endpoint, "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}")

    {:ok, pool} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3, pool_size: 2)
    Sediment.query!(pool, "create table t (x integer)", [], sync: true)
    :ok = Sediment.TcpProxy.cut(proxy)
    Sediment.query!(pool, "insert into t values (1)")

    read = fn -> Sediment.query(pool, "select count(*) from t", [], sync: true) end
    {micros, result} = :timer.tc(read)
    assert {:ok, %{rows: [[1]]}} = result
    assert micros < 500_000

    {micros, result} =
      :timer.tc(fn ->
        Sediment.transaction(
          pool,
          fn conn -> Sediment.query!(conn, "select count(*) from t").rows end,
          sync: true
        )
      end)

    assert result == {:ok, [[1]]}
    assert micros < 500_000

    assert {:error, %Sediment.Error{message: message}} =
             Sediment.query(pool, "insert into t values (2)", [],
               sync: true,
               sync_timeout: 1_000
             )

    assert message =~ "not known to be durable"

    :ok = Sediment.TcpProxy.resume(proxy)
    assert :ok = S3.flush(pool, 10_000)
    GenServer.stop(pool)
  end

  defp flush_acknowledging_loss(pool) do
    case S3.flush(pool, 5_000) do
      {:error, "s3 commits were lost" <> _} ->
        {:ok, _} = S3.acknowledge_loss(pool)
        S3.flush(pool, 5_000)

      other ->
        other
    end
  end

  # Files of `dir` this OS process still has open (Linux).
  defp open_files(dir) do
    for fd <- File.ls!("/proc/self/fd"),
        {:ok, target} <- [File.read_link("/proc/self/fd/#{fd}")],
        String.starts_with?(target, dir),
        do: target
  end
end
