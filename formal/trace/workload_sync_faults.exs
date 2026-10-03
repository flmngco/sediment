# One writer, sync durability, no client retries (errors reach the code), with targeted
# faults through TcpProxy: a commit whose PUT never reaches S3, a commit whose answer is
# lost after it landed, a publication whose manifest answer is lost, then more commits,
# a close and a reopen. Prints the prefix. Run by formal/trace/run.
alias Sediment.{Engine, S3, TcpProxy}
endpoint = System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
{:ok, proxy} = TcpProxy.start_link(URI.parse(endpoint).port)
prefix = "trace/faults-#{System.os_time()}"
s3 = [bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"), prefix: prefix,
      endpoint: "http://127.0.0.1:#{TcpProxy.port(proxy)}", region: "us-east-1",
      access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w", durability: :sync,
      max_retries: 0, request_timeout_ms: 2_000, checkpoint_threshold: -1]
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}")
File.mkdir_p!(dir)
# Faults apply to new connections: drop the kept-alive ones.
faults = fn f -> TcpProxy.set_faults(proxy, f); TcpProxy.cut(proxy); TcpProxy.resume(proxy) end
insert = fn db, i -> Engine.execute(db, "insert into t values (#{i})") end

{:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
:ok = Engine.execute(db, "create table t (x integer)")
:ok = insert.(db, 1)
# A commit whose PUT never reaches S3: it fails and rolls back; the next takes its offset.
TcpProxy.cut(proxy)
{:error, _} = insert.(db, 2)
TcpProxy.resume(proxy)
:ok = insert.(db, 3)
# A commit that lands but whose answer is lost: the code reads the key back.
faults.(lose_answer: {"/log/", 1})
_ = insert.(db, 4)
faults.([])
:ok = insert.(db, 5)
# A publication whose manifest answer is lost: the next commit's ownership check adopts it.
faults.(lose_answer: {"/manifest.json", 1})
_ = S3.snapshot(db)
faults.([])
_ = insert.(db, 6)
_ = insert.(db, 7)
Engine.close(db)
{:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: s3)
:ok = insert.(db, 8)
Engine.close(db)
IO.puts(prefix)
