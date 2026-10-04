defmodule Sediment.S3CancelFaultTest do
  # Async S3 durability under client timeouts (which cancel the native call
  # and disconnect), a lossy, slow S3 connection and repeated outages, with
  # several writers. The crash torture kills writers; this cancels them.
  # Afterwards the local database and a restore from S3 must hold the same
  # rows, and every commit acknowledged with sync: true must be in S3.
  #
  # S3_CANCEL_SECONDS sets the duration (default 20).
  use ExUnit.Case, async: false

  alias Sediment.{Engine, S3}

  @moduletag :s3
  # Faults through TcpProxy: local S3 servers only.
  @moduletag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"

  @moduletag timeout: 600_000

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  test "timeouts, drops and outages never leave local and S3 apart" do
    seconds = String.to_integer(System.get_env("S3_CANCEL_SECONDS", "20"))
    dir = Path.join(System.tmp_dir!(), "s3cancel-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    endpoint = System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")

    {:ok, proxy} =
      Sediment.TcpProxy.start_link(URI.parse(endpoint).port, max_delay_ms: 30, drop: 0.05)

    s3 = [
      bucket: Sediment.S3Bucket.name(),
      prefix: "elixir/cancel/#{System.unique_integer([:positive])}-#{System.os_time()}",
      endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
      region: Sediment.S3Bucket.region(),
      access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
      encryption: false,
      secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
      owner: "cancel-writer",
      durability: :async,
      max_lag_ms: 150,
      max_pending_bytes: 4_096,
      request_timeout_ms: 1_000,
      max_retries: 1,
      close_timeout_ms: 1_000
    ]

    {:ok, pool} =
      Sediment.start_link(
        database: Path.join(dir, "w.db"),
        s3: s3,
        pool_size: 2,
        idle_interval: 100,
        backoff_min: 50,
        backoff_max: 200
      )

    Sediment.query!(pool, "create table t (id integer primary key, w integer)", [], sync: true)

    ids = :atomics.new(1, [])
    deadline = System.monotonic_time(:millisecond) + seconds * 1_000

    chaos = Task.async(fn -> chaos(proxy, deadline) end)

    writers =
      for w <- 1..3 do
        Task.async(fn -> write(pool, w, ids, deadline, []) end)
      end

    synced = writers |> Enum.flat_map(&Task.await(&1, :infinity)) |> MapSet.new()
    Task.await(chaos, :infinity)

    :ok = Sediment.TcpProxy.set_faults(proxy, [])
    :ok = Sediment.TcpProxy.resume(proxy)

    local = recovered_rows(pool)
    GenServer.stop(pool)

    restored = restored_rows(dir, Keyword.put(s3, :endpoint, endpoint))
    assert local == restored
    assert MapSet.subset?(synced, MapSet.new(restored))
    # the run did write, and some sync: true commits made it through
    assert :atomics.get(ids, 1) > 50
    assert MapSet.size(synced) > 0
  end

  defp write(pool, w, ids, deadline, synced) do
    if System.monotonic_time(:millisecond) > deadline do
      synced
    else
      write(pool, w, ids, deadline, write_one(pool, w, ids, synced))
    end
  end

  # One random commit; returns `synced` plus its id if it was a sync: true
  # commit that succeeded.
  defp write_one(pool, w, ids, synced) do
    id = :atomics.add_get(ids, 1, 1)
    sql = "insert into t values (?, ?)"

    case :rand.uniform(10) do
      n when n <= 7 ->
        Sediment.query(pool, sql, [id, w], timeout: 50 + :rand.uniform(750))
        synced

      n when n <= 9 ->
        case Sediment.query(pool, sql, [id, w], sync: true, timeout: 3_000) do
          {:ok, _} -> [id | synced]
          {:error, _} -> synced
        end

      _ ->
        id2 = :atomics.add_get(ids, 1, 1)

        Sediment.transaction(
          pool,
          fn conn ->
            Sediment.query!(conn, sql, [id, w])
            Sediment.query!(conn, sql, [id2, w])
          end,
          timeout: 50 + :rand.uniform(750)
        )

        synced
    end
  rescue
    # query!/transaction on a dropped checkout or a disconnect
    _ -> synced
  catch
    # a client timeout may exit the caller (DBConnection checkout)
    :exit, _ -> synced
  end

  defp chaos(proxy, deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(1_000 + :rand.uniform(2_000))
      :ok = Sediment.TcpProxy.cut(proxy)
      Process.sleep(300 + :rand.uniform(1_200))
      :ok = Sediment.TcpProxy.resume(proxy)
      chaos(proxy, deadline)
    end
  end

  # Once S3 is back: the pool reconnects (acknowledging any loss the
  # outages caused), and after a successful flush shows its rows.
  defp recovered_rows(pool) do
    Enum.find_value(1..300, fn _ ->
      with :ok <- flush(pool),
           {:ok, %{rows: rows}} <- Sediment.query(pool, "select id from t order by id"),
           {:ok, %{durable_offset: d, committed_offset: d}} <- S3.info(pool) do
        List.flatten(rows)
      else
        _ -> Process.sleep(100) && nil
      end
    end) || flunk("the pool did not recover")
  end

  defp flush(pool) do
    case S3.flush(pool, 5_000) do
      {:error, "s3 commits were lost" <> _} ->
        _ = S3.acknowledge_loss(pool)
        S3.flush(pool, 5_000)

      other ->
        other
    end
  end

  defp restored_rows(dir, s3) do
    path = Path.join(dir, "restored.db")
    {:ok, _} = S3.restore(path, s3)
    {:ok, db} = Engine.open(path)
    {:ok, stmt} = Engine.prepare(db, "select id from t order by id")
    {:ok, rows} = Engine.fetch_all(db, stmt)
    :ok = Engine.close(db)
    List.flatten(rows)
  end
end
