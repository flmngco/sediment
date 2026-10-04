# Writer process for the S3 crash-torture test (test/sediment/s3_torture_test.exs),
# run as a separate BEAM that the test kill -9's. Reports on stdout, one line
# per event:
#
#   OPENED           (TORTURE_PAUSE_BEFORE_SCHEMA) the database is open, the
#                    table not created yet; the writer then waits to be killed
#   READY            the pool is open (restored from S3) and the table exists
#   B <ids> <pad>    about to commit rows <ids> (comma separated) as one
#                    transaction, with <pad> bytes of padding each; the row
#                    values are Sediment.TortureOracle.expected_row/2
#   A <ids>          the commit of <ids> was acknowledged
#   E <ids> <why>    the commit of <ids> failed (it may still be durable)
#   S <ids>          after A: <ids> were flushed (a sync: true commit)
#   F0 <k> / F1 <k>  flush <k> of everything committed so far starts / succeeded
#   S0 / S1          a snapshot (Sediment.S3.snapshot/1) starts / ends
#   FENCED           a connection was dropped because the writer was fenced (or
#                    its uploader gave up); the pool reconnects from what is
#                    durable, so earlier non-durable commits may be gone
env = &System.fetch_env!/1
base = String.to_integer(env.("TORTURE_BASE"))

s3 = [
  bucket: env.("TORTURE_BUCKET"),
  prefix: env.("TORTURE_PREFIX"),
  endpoint: env.("TORTURE_ENDPOINT"),
  region: System.get_env("TORTURE_REGION", "us-east-1"),
  access_key_id: env.("TORTURE_ACCESS_KEY_ID"),
  encryption: false,
  secret_access_key: env.("TORTURE_SECRET_ACCESS_KEY"),
  owner: env.("TORTURE_OWNER"),
  lease_ttl_ms: 3_000,
  checkpoint_threshold: 262_144,
  request_timeout_ms: 2_000,
  max_retries: 4,
  group_commit: env.("TORTURE_GROUP_COMMIT") == "true",
  durability: env.("TORTURE_DURABILITY"),
  upload_interval_ms: String.to_integer(System.get_env("TORTURE_UPLOAD_INTERVAL_MS", "0"))
]

say = fn line -> IO.puts(line) end

{:ok, _} = Application.ensure_all_started(:sediment)

# A fenced writer's connections reconnect and continue from what is durable
:telemetry.attach(
  "torture-fenced",
  [:sediment, :connection, :disconnect],
  fn
    _event, _measurements, %{reason: :fenced}, _ -> say.("FENCED")
    _event, _measurements, _meta, _ -> :ok
  end,
  nil
)

{:ok, pool} =
  Sediment.start_link(
    database: env.("TORTURE_DB"),
    s3: s3,
    pool_size: 4,
    journal_mode: :mvcc,
    backoff_min: 100,
    backoff_max: 500
  )

if System.get_env("TORTURE_PAUSE_BEFORE_SCHEMA") == "true" do
  opened = fn opened ->
    case Sediment.query(pool, "SELECT 1", []) do
      {:ok, _} ->
        :ok

      {:error, _} ->
        Process.sleep(100)
        opened.(opened)
    end
  end

  opened.(opened)
  say.("OPENED")
  Process.sleep(:infinity)
end

create = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, w INTEGER, v TEXT, pad BLOB)"

ready = fn ready ->
  case Sediment.query(pool, create, []) do
    {:ok, _} ->
      :ok

    {:error, _} ->
      Process.sleep(100)
      ready.(ready)
  end
end

ready.(ready)
# Flushes go through a connection of its own: it shares the pool's storage.
{:ok, flusher} = Sediment.Engine.open(env.("TORTURE_DB"), s3: s3, journal_mode: :mvcc)
flush = fn -> Sediment.Native.s3_flush(flusher, 10_000) end
say.("READY")

commit = fn ids, pad ->
  insert = "INSERT INTO t VALUES (?1, ?2, ?3, ?4)"

  params = fn id ->
    {w, v, padding} = Sediment.TortureOracle.expected_row(id, pad)
    [id, w, v, padding && {:blob, padding}]
  end

  case ids do
    [id] when rem(id, 2) == 0 ->
      Sediment.query(pool, insert, params.(id))

    _ ->
      mode = Enum.random([:deferred, :concurrent])

      try do
        Sediment.transaction(
          pool,
          fn conn -> Enum.each(ids, &Sediment.query!(conn, insert, params.(&1))) end,
          mode: mode
        )
      rescue
        error -> {:error, error}
      end
  end
end

writer = fn w ->
  Enum.each(Stream.iterate(0, &(&1 + 1)), fn i ->
    first = base + w * 1_000_000 + i * 4
    ids = Enum.to_list(first..(first + Enum.random(0..2)))
    line = Enum.join(ids, ",")
    pad = if :rand.uniform(64) == 1, do: 16_384, else: 0
    say.("B #{line} #{pad}")

    case commit.(ids, pad) do
      {:ok, _} ->
        say.("A " <> line)

        # A sync: true commit: acknowledged as durable once flushed.
        if :rand.uniform(5) == 1 do
          case flush.() do
            {:ok, _} -> say.("S " <> line)
            {:error, reason} -> say.("X #{line} #{reason}")
          end
        end

      {:error, error} ->
        say.("E #{line} #{inspect(error) |> String.slice(0, 120)}")
    end
  end)
end

for w <- 0..2, do: spawn_link(fn -> writer.(w) end)

spawn_link(fn ->
  Enum.each(Stream.iterate(1, &(&1 + 1)), fn k ->
    Process.sleep(500 + :rand.uniform(2_000))
    say.("F0 #{k}")
    if match?({:ok, _}, flush.()), do: say.("F1 #{k}")
  end)
end)

spawn_link(fn ->
  Enum.each(Stream.cycle([:ok]), fn _ ->
    Process.sleep(200 + :rand.uniform(1_500))
    say.("S0")
    _ = Sediment.S3.snapshot(pool)
    say.("S1")
  end)
end)

Process.sleep(:infinity)
