# One writer, sync durability, through a fault proxy resetting connections at random, with
# the S3 client's own retries on (object_store retries an attempt that got no answer; a
# landed first attempt makes the retry a 412). Prints the prefix. Run by formal/trace/run.
alias Sediment.{Engine, S3}
endpoint = System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
{:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint).port, max_delay_ms: 5, drop: 0.6)
prefix = "trace/retries-#{System.os_time()}"
s3 = [bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"), prefix: prefix,
      endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}", region: "us-east-1",
      access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w", durability: :sync,
      max_retries: 2, request_timeout_ms: 2_000, checkpoint_threshold: -1]
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}")
File.mkdir_p!(dir)
open = fn name ->
  Enum.find_value(1..20, fn _ ->
    case Engine.open(Path.join(dir, name), s3: s3) do
      {:ok, db} -> db
      {:error, _} -> Process.sleep(100) && nil
    end
  end)
end
db = open.("a.db")
_ = Engine.execute(db, "create table t (x integer)")
for i <- 1..30 do
  _ = Engine.execute(db, "insert into t values (#{i})")
  if rem(i, 10) == 0, do: S3.snapshot(db)
end
Engine.close(db)
db = open.("b.db")
for i <- 31..40, do: Engine.execute(db, "insert into t values (#{i})")
Engine.close(db)
IO.puts(prefix)
