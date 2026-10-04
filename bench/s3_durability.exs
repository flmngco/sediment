# S3 commit latency and throughput, durability: :sync vs :async, through a
# Sediment pool (what applications see). Needs an S3 server:
#
#   cd bench && MIX_ENV=prod mix run s3_durability.exs
#   S3_TEST_ENDPOINT=http://127.0.0.1:9000 S3_TEST_BUCKET=... \
#     S3_TEST_ACCESS_KEY_ID=... S3_TEST_SECRET_ACCESS_KEY=... MIX_ENV=prod mix run s3_durability.exs

commits = String.to_integer(System.get_env("COMMITS", "300"))
dir = Path.join(System.tmp_dir!(), "s3_durability_bench_#{System.os_time()}")
File.mkdir_p!(dir)

s3 = fn name, extra ->
  [
    bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"),
    prefix: "bench/durability/#{name}-#{System.os_time()}",
    endpoint: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333"),
    region: System.get_env("S3_TEST_REGION", "us-east-1"),
    access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
    encryption: false,
    secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
    owner: "bench"
  ] ++ extra
end

percentile = fn sorted, p -> Enum.at(sorted, round((length(sorted) - 1) * p)) end

report = fn label, micros, wall_us ->
  sorted = Enum.sort(micros)
  n = length(sorted)

  IO.puts(
    String.pad_trailing(label, 44) <>
      "p50 #{Float.round(percentile.(sorted, 0.5) / 1000, 2)} ms  " <>
      "p99 #{Float.round(percentile.(sorted, 0.99) / 1000, 2)} ms  " <>
      "#{round(n / (wall_us / 1_000_000))} commits/s"
  )
end

pool = fn name, extra, size ->
  {:ok, pool} =
    Sediment.start_link(
      database: Path.join(dir, "#{name}.db"),
      s3: s3.(name, extra),
      pool_size: size,
      journal_mode: :mvcc
    )

  Sediment.query!(pool, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
  pool
end

single = fn label, name, extra, opts ->
  p = pool.(name, extra, 1)

  {wall, micros} =
    :timer.tc(fn ->
      for i <- 1..commits do
        {us, _} =
          :timer.tc(fn ->
            Sediment.query!(p, "INSERT INTO t VALUES (?, 'hello')", [i], opts)
          end)

        us
      end
    end)

  report.(label, micros, wall)
  p
end

IO.puts(
  "# #{commits} single-row commits, endpoint #{System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")}\n"
)

single.("durability: :sync", "sync", [durability: :sync], [])
async = single.("durability: :async", "async", [durability: :async], [])
{flush_us, :ok} = :timer.tc(fn -> Sediment.S3.flush(async, 30_000) end)

IO.puts(
  String.pad_trailing("  then flush/2 of what's pending", 44) <>
    "#{Float.round(flush_us / 1000, 2)} ms"
)

single.("durability: :async, sync: true", "async-sync", [durability: :async], sync: true)

for {label, extra} <- [
      {"4 writers, :sync", [durability: :sync]},
      {"4 writers, :async", [durability: :async]}
    ] do
  p = pool.(String.replace(label, ~r/\W+/, "-"), extra, 4)

  {wall, micros} =
    :timer.tc(fn ->
      1..4
      |> Task.async_stream(
        fn w ->
          for i <- 1..div(commits, 4) do
            {us, _} =
              :timer.tc(fn ->
                Sediment.query!(p, "INSERT INTO t VALUES (?, 'w')", [w * 100_000 + i])
              end)

            us
          end
        end,
        timeout: :infinity
      )
      |> Enum.flat_map(fn {:ok, us} -> us end)
    end)

  report.(label, micros, wall)
end

File.rm_rf!(dir)
