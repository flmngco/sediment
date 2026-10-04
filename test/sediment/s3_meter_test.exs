defmodule Sediment.S3MeterTest do
  # The driver's S3 request meter (Sediment.S3.request_counts/0), which
  # guards the budget of soaks against paid S3, must count every request it
  # sends. An independent counting proxy sees the requests on the
  # wire; for a workload using every S3 path (sync and async commits,
  # flush, lease renewal, checkpoints with full, incremental and multipart
  # snapshots, GC, restore, point-in-time restore, a replica and its
  # refresh, close), both must agree per operation.
  #
  # The meter counts the whole OS process, so the workload runs in a VM of
  # its own and reports its counts once everything is closed.
  use ExUnit.Case, async: false

  alias Sediment.CountingProxy

  @moduletag :s3
  @moduletag timeout: 300_000
  # Proxies in front of the endpoint: local S3 servers only.
  @moduletag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"

  setup_all do
    # Before the proxy, so its CreateBucket (not a driver request) isn't seen.
    :ok = Sediment.S3Bucket.ensure()
  end

  test "the request meter counts exactly what reaches S3, per operation" do
    compare(URI.parse(Sediment.S3Bucket.endpoint()).port, [], [])
  end

  # Retries count too: a fault proxy behind the counting proxy delays
  # answers, and the second GET reaches S3 but its answer is lost and the
  # connection reset, so the driver's S3 client retries it. Deterministic:
  # random resets could miss every request, or hit a create-only PUT, which
  # fails its commit instead of being retried.
  test "requests that fail and are retried are counted like the others" do
    {:ok, faults} =
      Sediment.TcpProxy.start_link(URI.parse(Sediment.S3Bucket.endpoint()).port,
        max_delay_ms: 20,
        lose_answer: {"GET ", 2}
      )

    log_dir = Path.join(System.tmp_dir!(), "s3meter-log-#{System.unique_integer([:positive])}")
    File.mkdir_p!(log_dir)
    on_exit(fn -> File.rm_rf!(log_dir) end)
    compare(Sediment.TcpProxy.port(faults), [{"SEDIMENT_S3_METER_DIR", log_dir}], max_retries: 10)

    # The meter's own log shows the failed attempts that were retried.
    failed =
      for file <- File.ls!(log_dir),
          line <- File.read!(Path.join(log_dir, file)) |> String.split("\n"),
          String.starts_with?(line, "E "),
          do: line

    assert failed != []
  end

  defp compare(upstream_port, env, s3_extra) do
    dir = Path.join(System.tmp_dir!(), "s3meter-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, proxy} = CountingProxy.start_link(upstream_port)
    base = "elixir/meter/#{System.unique_integer([:positive])}-#{System.os_time()}"

    s3 =
      [
        bucket: Sediment.S3Bucket.name(),
        endpoint: "http://127.0.0.1:#{CountingProxy.port(proxy)}",
        region: "us-east-1",
        access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
        encryption: false,
        secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
        owner: "meter"
      ] ++ s3_extra

    script = Path.join(dir, "workload.exs")
    File.write!(script, workload(dir, base, s3))

    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    {output, status} =
      System.cmd(System.find_executable("elixir"), paths ++ [script],
        stderr_to_stdout: true,
        env: env
      )

    assert status == 0, output
    [_, encoded] = Regex.run(~r/METER (\S+)/, output)
    meter = encoded |> Base.decode64!() |> :erlang.binary_to_term()
    seen = CountingProxy.counts(proxy)

    # The workload reached every path it is meant to.
    for op <- ~w(PutObject GetObject HeadObject ListObjectsV2 DeleteObject
                 CreateMultipartUpload UploadPart CompleteMultipartUpload) do
      assert Map.get(seen, op, 0) + Map.get(seen, op <> "s", 0) > 0,
             "#{op} not exercised: #{inspect(seen)}"
    end

    if meter == nil do
      flunk("Sediment.S3.request_counts/0 is not available; proxy saw #{inspect(seen)}")
    else
      counted = for {op, %{count: n}} <- meter, n > 0, into: %{}, do: {op, n}
      assert counted == seen

      for {op, %{class: class}} <- meter, Map.has_key?(seen, op) do
        assert class == op |> CountingProxy.class() |> class_name(), op
      end
    end
  end

  defp class_name(:a), do: "A"
  defp class_name(:b), do: "B"
  defp class_name(:free), do: "free"

  # Every S3 path of the driver, then close everything and report the
  # meter's counts once the last requests (lease releases) are done.
  defp workload(dir, base, s3) do
    """
    alias Sediment.{Engine, S3}
    s3 = #{inspect(s3)}
    dir = #{inspect(dir)}
    opts = fn prefix, extra -> Keyword.merge(s3, [prefix: #{inspect(base)} <> "/" <> prefix] ++ extra) end

    # sync commits, lease renewals, full and incremental snapshots, GC
    sync = opts.("sync", durability: :sync, lease_ttl_ms: 2_000, retain_epochs: 1)
    {:ok, db} = Engine.open(Path.join(dir, "sync.db"), s3: sync)
    :ok = Engine.execute(db, "create table t (x integer, b blob)")
    for i <- 1..20, do: :ok = Engine.execute(db, "insert into t values (\#{i}, randomblob(2000))")
    :ok = S3.snapshot(db)
    for i <- 21..30, do: :ok = Engine.execute(db, "insert into t values (\#{i}, randomblob(2000))")
    :ok = S3.snapshot(db)
    for i <- 31..40, do: :ok = Engine.execute(db, "insert into t values (\#{i}, randomblob(2000))")
    :ok = S3.snapshot(db)
    Process.sleep(3_000)
    {:ok, %{epoch: epoch}} = S3.info(db)

    # async commits, flush
    async = opts.("async", durability: :async)
    {:ok, adb} = Engine.open(Path.join(dir, "async.db"), s3: async)
    :ok = Engine.execute(adb, "create table t (x integer)")
    for i <- 1..50, do: :ok = Engine.execute(adb, "insert into t values (\#{i})")
    :ok = S3.flush(adb, 30_000)
    :ok = Engine.close(adb)

    # a multipart snapshot: over 8 MiB of incompressible pages
    big = opts.("big", durability: :sync, incremental_snapshots: false)
    {:ok, bdb} = Engine.open(Path.join(dir, "big.db"), s3: big)
    :ok = Engine.execute(bdb, "create table t (b blob)")
    for _ <- 1..12, do: :ok = Engine.execute(bdb, "insert into t values (randomblob(1000000))")
    :ok = S3.snapshot(bdb)
    :ok = Engine.close(bdb)

    # a replica and its refresh
    {:ok, replica} = Engine.open(Path.join(dir, "replica.db"), s3: Keyword.put(sync, :mode, :replica))
    :ok = Engine.execute(db, "insert into t values (100, randomblob(10))")
    {:ok, _} = S3.refresh(replica)
    :ok = Engine.close(replica)
    :ok = Engine.close(db)

    # restore, point-in-time restore
    {:ok, _} = S3.restore(Path.join(dir, "r1.db"), sync)
    [seq | _] = String.split(epoch, "-")
    {:ok, _} = S3.restore(Path.join(dir, "r2.db"), sync, epoch: String.to_integer(seq))

    Process.sleep(1_500)
    counts = if function_exported?(S3, :request_counts, 0), do: S3.request_counts(), else: nil
    IO.puts("METER " <> Base.encode64(:erlang.term_to_binary(counts)))
    """
  end
end
