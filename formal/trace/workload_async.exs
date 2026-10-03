# One writer, async durability: bootstrap, commits, a checkpoint, close, reopen (takeover),
# a commit. Prints the prefix. Run by formal/trace/run.
alias Sediment.{Engine, S3}
s3 = [bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"), prefix: "trace/async-sync-#{System.os_time()}", endpoint: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333"),
      region: "us-east-1", access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w", durability: :async]
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}"); File.mkdir_p!(dir)
{:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
:ok = Engine.execute(db, "create table t (x integer)")
for i <- 1..3, do: :ok = Engine.execute(db, "insert into t values (#{i})")
:ok = S3.flush(db)
:ok = S3.snapshot(db)
:ok = Engine.execute(db, "insert into t values (4)")
:ok = Engine.close(db)
{:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: s3)
:ok = Engine.execute(db, "insert into t values (5)")
:ok = Engine.close(db)
IO.puts(s3[:prefix])
