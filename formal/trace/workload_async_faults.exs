# One writer, async durability, no client retries (errors reach the uploader, which retries
# the same segment), with targeted faults through TcpProxy: uploads failing while S3 is cut
# off, a segment whose answer is lost after it landed, a publication whose manifest answer
# is lost, then a close and a reopen. Prints the prefix. Run by formal/trace/run ... TRUE.
alias Sediment.{Engine, S3, TcpProxy}
endpoint = System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
{:ok, proxy} = TcpProxy.start_link(URI.parse(endpoint).port)
prefix = "trace/async-faults-#{System.os_time()}"
s3 = [bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"), prefix: prefix,
      endpoint: "http://127.0.0.1:#{TcpProxy.port(proxy)}", region: "us-east-1",
      access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w", durability: :async,
      max_retries: 0, request_timeout_ms: 2_000, checkpoint_threshold: -1]
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}")
File.mkdir_p!(dir)
# Faults apply to new connections: drop the kept-alive ones.
faults = fn f -> TcpProxy.set_faults(proxy, f); TcpProxy.cut(proxy); TcpProxy.resume(proxy) end
insert = fn db, i -> :ok = Engine.execute(db, "insert into t values (#{i})") end

{:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
:ok = Engine.execute(db, "create table t (x integer)")
insert.(db, 1)
:ok = S3.flush(db)
# S3 cut off: the commit returns, its upload fails and is retried after S3 is back.
TcpProxy.cut(proxy)
insert.(db, 2)
Process.sleep(300)
TcpProxy.resume(proxy)
:ok = S3.flush(db, 10_000)
# A segment that lands but whose answer is lost: the retry finds it (412, then a read).
faults.(lose_answer: {"/log/", 1})
insert.(db, 3)
_ = S3.flush(db, 10_000)
faults.([])
insert.(db, 4)
:ok = S3.flush(db, 10_000)
# A publication whose manifest answer is lost: the next upload re-seals, then adopts it.
faults.(lose_answer: {"/manifest.json", 1})
_ = S3.snapshot(db)
faults.([])
insert.(db, 5)
:ok = S3.flush(db, 10_000)
Engine.close(db)
{:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: s3)
insert.(db, 6)
:ok = S3.flush(db, 10_000)
Engine.close(db)
IO.puts(prefix)
