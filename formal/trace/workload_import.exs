# Import of an existing database file (an AUTOINCREMENT table, as Ecto makes them) into an empty
# prefix (Sediment.S3.import/3, verified by a restore), then an open that restores it, commits,
# checkpoints and closes. Prints the prefix.
# Run by formal/trace/run with Async=FALSE (sync durability).
alias Sediment.{Engine, S3}
s3 = [bucket: System.get_env("S3_TEST_BUCKET", "sediment-tests"), prefix: "trace/import-#{System.os_time()}", endpoint: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333"),
      region: "us-east-1", access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w", durability: :sync]
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}"); File.mkdir_p!(dir)
source = Path.join(dir, "existing.db")
{:ok, plain} = Engine.open(source)
:ok = Engine.execute(plain, "create table t (id integer primary key autoincrement, x integer)")
for i <- 1..3, do: :ok = Engine.execute(plain, "insert into t (x) values (#{i})")
:ok = Engine.close(plain)
{:ok, _} = S3.import(source, s3, verify: :restore)
{:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
:ok = Engine.execute(db, "insert into t (x) values (4)")
:ok = S3.snapshot(db)
:ok = Engine.execute(db, "insert into t (x) values (5)")
:ok = Engine.close(db)
File.rm_rf!(dir)
IO.puts(s3[:prefix])
