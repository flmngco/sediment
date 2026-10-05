defmodule Sediment.S3Test do
  # Runs against a local S3 server: mix test --only s3
  # SeaweedFS on 127.0.0.1:8333 by default; another backend with
  # S3_TEST_ENDPOINT, S3_TEST_ACCESS_KEY_ID, S3_TEST_SECRET_ACCESS_KEY and
  # S3_TEST_BUCKET: any S3-compatible server with conditional writes (the
  # bucket must exist if the server requires signed requests).
  use ExUnit.Case, async: false

  alias Sediment.Engine
  alias Sediment.S3

  @moduletag :s3

  defp bucket, do: Sediment.S3Bucket.name()
  defp endpoint, do: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  # Under System.tmp_dir!(), which test_helper points at a directory of this
  # run: ExUnit's :tmp_dir is relative to the checkout and wiped at test start,
  # so two runs in one checkout would share (and delete) each other's files.
  setup do
    prefix =
      "elixir/#{System.unique_integer([:positive])}-#{System.os_time()}"

    dir = Path.join(System.tmp_dir!(), "s3-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    %{s3: s3_opts(prefix, "writer-a"), prefix: prefix, dir: dir}
  end

  defp s3_opts(prefix, owner) do
    [
      bucket: bucket(),
      prefix: prefix,
      endpoint: endpoint(),
      region: Sediment.S3Bucket.region(),
      access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
      encryption: false,
      secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
      owner: owner,
      lease_ttl_ms: 5_000,
      # These tests check what is in S3 right after a commit returns.
      durability: :sync
    ]
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(100) && eventually(fun, attempts - 1)
    end
  end

  defp count(db, table) do
    {:ok, stmt} = Engine.prepare(db, "SELECT count(*) FROM #{table}")
    {:ok, [[n]]} = Engine.fetch_all(db, stmt)
    :ok = Engine.release(db, stmt)
    n
  end

  test "commits survive losing the local files", %{s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

    for i <- 1..10 do
      :ok = Engine.execute(db, "INSERT INTO t VALUES (#{i}, 'v#{i}')")
    end

    :ok = Engine.close(db)
    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    {:ok, restored} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    assert count(restored, "t") == 10
    :ok = Engine.close(restored)
  end

  test "destroy/2 deletes the database: a later open starts empty", %{
    prefix: prefix,
    s3: s3,
    dir: dir
  } do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

    assert {:error, "s3 config: the database at " <> open} = S3.destroy(s3)
    assert open =~ "is open in this VM"
    :ok = Engine.close(db)

    # A writer that was closed released its lease.
    {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "writer-b"))
    :ok = Engine.close(b)
    assert {:ok, %{objects: objects}} = S3.destroy(s3)
    assert objects > 0

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    assert {:error, "s3 config: no database" <> _} = S3.restore(Path.join(dir, "r.db"), s3)
    {:ok, fresh} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    {:ok, stmt} = Engine.prepare(fresh, "SELECT count(*) FROM sqlite_schema WHERE name = 't'")
    assert {:ok, [[0]]} = Engine.fetch_all(fresh, stmt)
    :ok = Engine.release(fresh, stmt)
    :ok = Engine.execute(fresh, "CREATE TABLE u (id INTEGER PRIMARY KEY)")
    :ok = Engine.close(fresh)
    assert {:ok, _} = S3.destroy(s3, force: true)
  end

  test "exists?/1 and must_exist: true", %{s3: s3, dir: dir} do
    refute S3.exists?(s3)
    strict = Keyword.put(s3, :must_exist, true)
    assert {:error, message} = Engine.open(Path.join(dir, "a.db"), s3: strict)
    assert message =~ "no database at s3://"
    assert message =~ "(must_exist: true)"
    refute S3.exists?(s3)

    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.close(db)
    assert S3.exists?(s3)
    {:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: strict)
    :ok = Engine.close(db)

    assert {:ok, _} = S3.destroy(s3)
    refute S3.exists?(s3)

    assert_raise Sediment.Error, ~r/s3/, fn ->
      S3.exists?(Keyword.put(s3, :endpoint, "http://127.0.0.1:1"))
    end
  end

  test "destroy/2 refuses a writer's unexpired lease unless forced", %{
    prefix: prefix,
    dir: dir
  } do
    # The same server under another name: as far as this destroy can tell,
    # the writer runs in another process.
    alias_endpoint = String.replace(endpoint(), "127.0.0.1", "localhost")
    if alias_endpoint == endpoint(), do: flunk("needs an endpoint on 127.0.0.1")
    {:ok, a} = Engine.open(Path.join(dir, "a.db"), s3: s3_opts(prefix, "writer-a"))
    other = Keyword.put(s3_opts(prefix, "writer-a"), :endpoint, alias_endpoint)
    assert {:error, "s3 lease held by writer-a" <> _} = S3.destroy(other)
    assert {:ok, _} = S3.destroy(other, force: true)
    assert {:error, reason} = Engine.execute(a, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    assert reason =~ "fenced"
    :ok = Engine.close(a)
  end

  test "S3 databases are encrypted unless encryption: false says otherwise", %{
    s3: s3,
    prefix: prefix,
    dir: dir
  } do
    # The documented form: encryption is a top-level option, not an :s3 one
    s3 = Keyword.delete(s3, :encryption)
    path = &Path.join(dir, &1)

    assert {:error, "s3 config: S3 databases are encrypted by default" <> message} =
             Engine.open(path.("a.db"), s3: s3)

    assert message =~ "Sediment.S3.generate_key()"
    assert message =~ "encryption: false"
    refute File.exists?(path.("a.db"))

    {:ok, db} = Engine.open(path.("a.db"), s3: s3, encryption: false)
    :ok = Engine.execute(db, "CREATE TABLE t (v)")
    :ok = Engine.close(db)

    assert {:error, "s3 config: the database at this S3 prefix is unencrypted" <> _} =
             Engine.open(path.("b.db"), s3: s3)

    # A key doesn't open it, and the attempt changes nothing
    key = [cipher: "aegis256", key: S3.generate_key()]

    assert {:error, "s3 config: the database at this S3 prefix is unencrypted" <> _} =
             Engine.open(path.("k.db"), s3: s3, encryption: key)

    {:ok, db} = Engine.open(path.("k2.db"), s3: s3, encryption: false)
    assert count(db, "t") == 0
    :ok = Engine.close(db)

    replica = Keyword.merge(s3, owner: "reader", mode: :replica)

    assert {:error, "s3 config: the database at this S3 prefix is unencrypted" <> _} =
             Engine.open(path.("r.db"), s3: replica)

    {:ok, r} = Engine.open(path.("r.db"), s3: replica, encryption: false)
    :ok = Engine.close(r)

    assert {:error, "s3 config: the database at this S3 prefix is unencrypted" <> _} =
             S3.restore(path.("copy.db"), s3)

    assert {:ok, _} = S3.restore(path.("copy.db"), s3, encryption: false)

    # A key from generate_key/0
    key = S3.generate_key()
    assert key =~ ~r/\A[0-9a-f]{64}\z/
    refute key == S3.generate_key()
    encrypted = Keyword.put(s3, :prefix, prefix <> "-enc")
    enc = [cipher: "aegis256", key: key]
    {:ok, db} = Engine.open(path.("e.db"), s3: encrypted, encryption: enc)
    :ok = Engine.close(db)

    for opts <- [[], [encryption: false]] do
      assert {:error, "s3 config: the database at this S3 prefix is encrypted:" <> _} =
               Engine.open(path.("f.db"), [s3: encrypted] ++ opts)
    end

    assert {:error, message} = Engine.open(path.("g.db"), s3: Keyword.put(s3, :encryption, true))
    assert message =~ "not inside :s3"
  end

  test "an open sharing a database already open in this VM needs the same encryption", %{
    s3: s3,
    dir: dir
  } do
    s3 = Keyword.delete(s3, :encryption)
    enc = [cipher: "aegis256", key: String.duplicate("ab", 32)]
    wrong = [cipher: "aegis256", key: String.duplicate("cd", 32)]
    path = &Path.join(dir, &1)
    {:ok, writer} = Engine.open(path.("w.db"), s3: s3, encryption: enc)
    :ok = Engine.execute(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY, secret TEXT)")
    :ok = Engine.execute(writer, "INSERT INTO t VALUES (1, 'IN-THE-LOG')")

    # A second writer open of the same file (the cached storage)
    for opts <- [[], [encryption: false], [encryption: wrong]] do
      assert {:error, message} = Engine.open(path.("w.db"), [s3: s3] ++ opts)
      assert message =~ "encrypted" or message =~ "another :encryption", message
    end

    # A plain open of the S3 database's file
    for opts <- [[], [encryption: wrong]] do
      assert {:error, message} = Engine.open(path.("w.db"), opts)
      assert message =~ "another :encryption choice", message
    end

    replica = Keyword.merge(s3, owner: "reader", mode: :replica)
    {:ok, keyed} = Engine.open(path.("r.db"), s3: replica, encryption: enc)

    refused = fn ->
      for opts <- [[], [encryption: false], [encryption: wrong]] do
        assert {:error, message} = Engine.open(path.("r.db"), [s3: replica] ++ opts)
        assert message =~ "is encrypted" or message =~ "another :encryption key", message
      end
    end

    refused.()
    # Rows in the snapshot, and a newer generation through refresh
    :ok = Engine.execute(writer, "INSERT INTO t VALUES (2, 'IN-THE-SNAPSHOT')")
    :ok = S3.snapshot(writer)
    {:ok, _} = S3.refresh(keyed)
    refused.()

    {:ok, again} = Engine.open(path.("r.db"), s3: replica, encryption: enc)
    assert count(again, "t") == 2
    :ok = Engine.close(again)
    :ok = Engine.close(keyed)
    :ok = Engine.close(writer)

    # An unencrypted database: the same choice again
    plain_s3 = Keyword.put(s3, :prefix, s3[:prefix] <> "-plain")
    {:ok, writer} = Engine.open(path.("p.db"), s3: plain_s3, encryption: false)

    for opts <- [[], [encryption: enc]] do
      assert {:error, message} = Engine.open(path.("p.db"), [s3: plain_s3] ++ opts)
      assert message =~ "is unencrypted", message
    end

    {:ok, second} = Engine.open(path.("p.db"), s3: plain_s3, encryption: false)
    :ok = Engine.close(second)
    :ok = Engine.close(writer)
  end

  test "export_sqlite/3 from S3 needs the key, reads the prefix only, and refuses existing files",
       %{s3: s3, dir: dir} do
    s3 = Keyword.delete(s3, :encryption)
    enc = [cipher: "aegis256", key: String.duplicate("ab", 32)]
    {:ok, db} = Engine.open(Path.join(dir, "w.db"), s3: s3, encryption: enc)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    :ok = Engine.execute(db, "INSERT INTO t (v) VALUES ('a'), ('b'), ('c')")
    :ok = Engine.execute(db, "DELETE FROM t WHERE id = 3")
    :ok = Engine.close(db)
    dest = Path.join(dir, "export.db")

    assert {:error, "export: s3 config: the database at this S3 prefix is encrypted" <> _} =
             Sediment.export_sqlite(nil, dest, from_s3: s3)

    assert {:error, _} =
             Sediment.export_sqlite(nil, dest,
               from_s3: s3,
               encryption: [cipher: "aegis256", key: String.duplicate("cd", 32)]
             )

    refute File.exists?(dest)

    assert {:ok, %{rows: 2, sequences: [{"t", 3}]}} =
             Sediment.export_sqlite(nil, dest, from_s3: s3, encryption: enc)

    assert <<"SQLite format 3", 0, _::binary>> = File.read!(dest)

    assert {:error, "export target exists: " <> _} =
             Sediment.export_sqlite(nil, dest, from_s3: s3, encryption: enc)

    # the writer still opens the prefix as it was
    {:ok, db} = Engine.open(Path.join(dir, "w2.db"), s3: s3, encryption: enc)
    assert count(db, "t") == 2
    :ok = Engine.close(db)
  end

  test "an empty prefix never replaces an existing local database", %{s3: s3, dir: dir} do
    path = Path.join(dir, "existing.db")
    {:ok, plain} = Engine.open(path)
    :ok = Engine.execute(plain, "CREATE TABLE t (v)")
    :ok = Engine.execute(plain, "INSERT INTO t VALUES (1)")
    :ok = Engine.close(plain)

    assert {:error, message} = Engine.open(path, s3: s3)
    assert message =~ "refusing to replace the local file"
    assert message =~ "move or delete it"

    {:ok, plain} = Engine.open(path)
    assert count(plain, "t") == 1
    :ok = Engine.close(plain)

    # Moved away, the prefix starts a new, empty database.
    File.rename!(path, path <> ".bak")
    {:ok, db} = Engine.open(path, s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (v)")
    :ok = Engine.close(db)
  end

  test "import/3 uploads an existing database that later opens restore", %{
    s3: s3,
    prefix: prefix,
    dir: dir
  } do
    source = Path.join(dir, "existing.db")
    {:ok, plain} = Engine.open(source)
    :ok = Engine.execute(plain, "CREATE TABLE t (v)")
    :ok = Engine.execute(plain, "INSERT INTO t VALUES (1), (2)")
    :ok = Engine.close(plain)
    bytes = File.read!(source)

    assert {:ok, %{objects: 1, rows: 2, epoch: "00000000000000000000-" <> _} = info} =
             S3.import(source, s3, verify: :restore)

    assert info.size > 0 and info.stored_size > 0
    assert File.read!(source) == bytes
    assert Path.wildcard(Path.join(dir, ".*import*")) == []

    {:ok, db} = Engine.open(Path.join(dir, "restored.db"), s3: s3)
    assert count(db, "t") == 2
    :ok = Engine.close(db)

    assert {:error, "s3 config: the S3 prefix " <> message} = S3.import(source, s3)
    assert message =~ "already holds a database"

    # Into an encrypted database: opens need the key
    enc = [cipher: "aegis256", key: String.duplicate("ab", 32)]
    wrong = [cipher: "aegis256", key: String.duplicate("cd", 32)]
    other = s3_opts(prefix <> "-enc", "writer-a")
    assert {:ok, %{rows: 2}} = S3.import(source, other, encryption: enc)

    assert {:error, _} =
             Engine.open(Path.join(dir, "wrong.db"), s3: other, encryption: wrong)

    {:ok, db} = Engine.open(Path.join(dir, "enc.db"), s3: other, encryption: enc)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (3)")
    assert count(db, "t") == 3
    :ok = Engine.close(db)
  end

  test "import/3 of a database larger than one upload part", %{s3: s3, dir: dir} do
    source = Path.join(dir, "big.db")
    {:ok, plain} = Engine.open(source)
    :ok = Engine.execute(plain, "CREATE TABLE b (v BLOB)")

    for _ <- 1..12 do
      :ok = Engine.execute(plain, "INSERT INTO b VALUES (randomblob(1024 * 1024))")
    end

    :ok = Engine.close(plain)

    assert {:ok, %{size: size, stored_size: stored}} = S3.import(source, s3, verify: :restore)
    assert size > 12 * 1024 * 1024 and stored > 8 * 1024 * 1024

    {:ok, db} = Engine.open(Path.join(dir, "restored.db"), s3: s3)
    assert count(db, "b") == 12
    :ok = Engine.close(db)
  end

  test "restore refuses to replace an existing file, a running writer's database included",
       %{s3: s3, dir: dir} do
    live = Path.join(dir, "live.db")
    {:ok, db} = Engine.open(live, s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (v)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

    other = Path.join(dir, "other.db")
    File.write!(other, "not a database")
    assert {:error, "restore target exists: " <> ^other} = S3.restore(other, s3)
    assert File.read!(other) == "not a database"

    assert {:error, "restore target exists: " <> ^live} = S3.restore(live, s3)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (2)")
    assert count(db, "t") == 2
    :ok = Engine.close(db)

    # A leftover WAL or MVCC log of an old copy would be applied to the
    # restored file
    stale = Path.join(dir, "stale.db")
    File.write!(Path.join(dir, "stale.db-log"), "old")
    assert {:error, "restore target exists: " <> _} = S3.restore(stale, s3)
    refute File.exists?(stale)

    File.rm!(Path.join(dir, "stale.db-log"))
    assert {:ok, _} = S3.restore(stale, s3)
  end

  test "snapshots after small changes are deltas, and restore rebuilds the chain",
       %{s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")
    for i <- 1..40, do: :ok = Engine.execute(db, "INSERT INTO t VALUES (#{i}, randomblob(20000))")
    :ok = S3.snapshot(db)
    :ok = Engine.execute(db, "UPDATE t SET v = zeroblob(1) WHERE id = 3")
    :ok = S3.snapshot(db)
    {:ok, info} = S3.info(db)
    assert info.snapshot =~ ~r/\.delta$/
    :ok = Engine.close(db)

    {:ok, _} = S3.restore(Path.join(dir, "r.db"), s3)
    {:ok, restored} = Engine.open(Path.join(dir, "r.db"))
    assert count(restored, "t") == 40
    :ok = Engine.close(restored)
  end

  test "snapshot/1 starts a new epoch and info/1 reports it", %{s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

    {:ok, before} = S3.info(db)
    assert before.uploaded_frames == 2
    assert before.owner == "writer-a"
    assert before.poisoned == nil

    :ok = S3.snapshot(db)
    {:ok, after_snapshot} = S3.info(db)
    assert after_snapshot.epoch != before.epoch
    assert after_snapshot.snapshot =~ after_snapshot.epoch
    assert after_snapshot.snapshot_pending == false

    :ok = Engine.execute(db, "INSERT INTO t VALUES (2)")
    :ok = Engine.close(db)

    {:ok, restored} = Engine.open(Path.join(dir, "b.db"), s3: s3)
    assert count(restored, "t") == 2
    :ok = Engine.close(restored)
  end

  test "a second writer is refused while the lease is held", %{prefix: prefix, s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)

    assert {:error, reason} =
             Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "writer-b"))

    assert reason =~ "lease held by writer-a"

    :ok = Engine.close(db)
    {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "writer-b"))
    :ok = Engine.close(b)
  end

  test "close releases the lease while statements are still referenced", %{
    prefix: prefix,
    s3: s3,
    dir: dir
  } do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer)")
    {:ok, stmt} = Engine.prepare(db, "select x from t")
    :ok = Engine.close(db)

    {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "writer-b"))
    :ok = Engine.close(b)
    assert {:error, :connection_closed} = Engine.step(db, stmt)
  end

  describe "replica mode" do
    test "reads the writer's commits without taking the lease", %{
      prefix: prefix,
      s3: s3,
      dir: dir
    } do
      {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3)
      :ok = Engine.execute(writer, "create table t (x integer); insert into t values (1)")

      replica_opts = Keyword.put(s3_opts(prefix, "reader"), :mode, :replica)
      {:ok, replica} = Engine.open(Path.join(dir, "r.db"), s3: replica_opts)
      assert count(replica, "t") == 1
      assert {:ok, %{mode: :replica, writer: "writer-a"}} = S3.info(replica)

      assert {:error, "attempt to write a readonly database"} =
               Engine.execute(replica, "insert into t values (2)")

      # the writer still owns the lease and keeps committing
      :ok = Engine.execute(writer, "insert into t values (2)")
      assert count(replica, "t") == 1

      {:ok, stmt} = Engine.prepare(replica, "select 1")
      assert {:ok, %{log_bytes: bytes}} = S3.refresh(replica)
      assert bytes > 0
      assert count(replica, "t") == 2
      assert {:error, :invalid_statement} = Engine.bind(stmt, [])

      :ok = S3.snapshot(writer)
      :ok = Engine.execute(writer, "insert into t values (3)")
      {:ok, before} = S3.info(replica)
      {:ok, refreshed} = S3.refresh(replica)
      assert refreshed.epoch != before.epoch
      assert count(replica, "t") == 3

      :ok = Engine.close(replica)
      :ok = Engine.close(writer)
    end

    test "refresh without changes keeps the local copy", %{prefix: prefix, s3: s3, dir: dir} do
      {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3)
      :ok = Engine.execute(writer, "create table t (x integer)")

      replica_opts = Keyword.put(s3_opts(prefix, "reader"), :mode, :replica)
      {:ok, replica} = Engine.open(Path.join(dir, "r.db"), s3: replica_opts)
      {:ok, stmt} = Engine.prepare(replica, "select count(*) from t")
      {:ok, info} = S3.info(replica)
      assert {:ok, ^info} = S3.refresh(replica)
      assert {:row, [0]} = Engine.step(replica, stmt)
    end

    test "works through DBConnection", %{prefix: prefix, s3: s3, dir: dir} do
      {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3)
      :ok = Engine.execute(writer, "create table t (x integer); insert into t values (7)")

      {:ok, pool} =
        Sediment.start_link(
          database: Path.join(dir, "r.db"),
          s3: Keyword.put(s3_opts(prefix, "reader"), :mode, :replica)
        )

      assert %{rows: [[7]]} = Sediment.query!(pool, "select x from t")
    end

    test "errors", %{prefix: prefix, s3: s3, dir: dir} do
      {:ok, plain} = Engine.open(Path.join(dir, "plain.db"))
      assert {:error, "not an s3 replica"} = S3.refresh(plain)

      assert {:error, "s3 config: no database at " <> _} =
               Engine.open(Path.join(dir, "r.db"),
                 s3: Keyword.put(s3_opts(prefix <> "-missing", "reader"), :mode, :replica)
               )

      assert {:error, "s3 config: mode must be writer or replica" <> _} =
               Engine.open(Path.join(dir, "x.db"), s3: Keyword.put(s3, :mode, :bogus))

      assert {:error, "s3 durability does not support read-only opens" <> _} =
               Engine.open(Path.join(dir, "y.db"), s3: s3, mode: :readonly)
    end
  end

  describe "group commit" do
    test "every commit is uploaded even when the connection asks for synchronous = NORMAL",
         %{s3: s3, dir: dir} do
      path = Path.join(dir, "g.db")

      {:ok, pool} =
        Sediment.start_link(
          database: path,
          s3: Keyword.put(s3, :group_commit, true),
          pool_size: 4,
          default_transaction_mode: :concurrent,
          synchronous: :normal
        )

      Sediment.query!(pool, "create table t (id integer primary key, n integer)")

      1..40
      |> Task.async_stream(
        fn n ->
          Sediment.transaction(pool, fn conn ->
            Sediment.query!(conn, "insert into t (n) values (?)", [n])
          end)
        end,
        max_concurrency: 8
      )
      |> Enum.each(fn {:ok, {:ok, _}} -> :ok end)

      assert %{rows: [[40]]} = Sediment.query!(pool, "select count(*) from t")
      assert %{rows: [[2]]} = Sediment.query!(pool, "PRAGMA synchronous")
      GenServer.stop(pool)

      # nothing acknowledged was left behind locally: a fresh restore has it all
      File.rm_rf!(path)
      {:ok, restored} = Engine.open(Path.join(dir, "g2.db"), s3: s3)
      assert count(restored, "t") == 40
      :ok = Engine.close(restored)
    end
  end

  test "close releases the lease while other processes drop statements", %{
    prefix: prefix,
    s3: s3,
    dir: dir
  } do
    for _ <- 1..30 do
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
      parent = self()

      holders =
        for _ <- 1..20 do
          spawn(fn ->
            {:ok, stmt} = Engine.prepare(db, "select 1")
            send(parent, :ready)
            receive do: (:exit -> stmt)
          end)
        end

      for _ <- holders, do: assert_receive(:ready, 5_000)
      Enum.each(holders, &send(&1, :exit))
      :ok = Engine.close(db)
      Process.sleep(50)

      assert {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "writer-b"))
      :ok = Engine.close(b)
    end
  end

  describe "S3 helpers with a DBConnection pool" do
    test "refresh/1 brings every connection of a replica pool up to date", %{
      prefix: prefix,
      s3: s3,
      dir: dir
    } do
      {:ok, writer} = Sediment.start_link(database: Path.join(dir, "w.db"), s3: s3)
      Sediment.query!(writer, "create table t (x integer)")
      Sediment.query!(writer, "insert into t values (1)")
      assert {:ok, %{owner: "writer-a"}} = S3.info(writer)
      assert :ok = S3.snapshot(writer)

      {:ok, replica} =
        Sediment.start_link(
          database: Path.join(dir, "r.db"),
          pool_size: 3,
          s3: Keyword.put(s3_opts(prefix, "reader"), :mode, :replica)
        )

      counts = fn ->
        parent = self()

        1..3
        |> Enum.map(fn _ ->
          Task.async(fn ->
            DBConnection.run(replica, fn conn ->
              send(parent, :checked_out)
              receive do: (:go -> :ok)
              %{rows: [[n]]} = Sediment.query!(conn, "select count(*) from t")
              n
            end)
          end)
        end)
        |> tap(fn tasks ->
          for _ <- tasks, do: assert_receive(:checked_out, 5_000)
          Enum.each(tasks, &send(&1.pid, :go))
        end)
        |> Enum.map(&Task.await/1)
      end

      # The pool's connections share one working copy
      copies = fn ->
        dir |> File.ls!() |> Enum.filter(&(&1 =~ ~r/^\.r\.replica-.*\.db$/))
      end

      assert counts.() == [1, 1, 1]
      assert [_] = copies.()
      Sediment.query!(writer, "insert into t values (2)")
      assert counts.() == [1, 1, 1]

      assert {:ok, %{mode: :replica}} = S3.refresh(replica)
      assert counts.() == [2, 2, 2]
      assert {:ok, %{mode: :replica}} = S3.info(replica)
      # all moved to the new copy, and the old one is gone
      assert [_] = copies.()

      assert {:ok, {:error, "cannot refresh a replica inside a transaction"}} =
               Sediment.transaction(replica, fn conn -> S3.refresh(conn) end)
    end
  end

  test "an unreachable endpoint fails the open within the request timeout", %{s3: s3, dir: dir} do
    s3 =
      Keyword.merge(s3, endpoint: "http://127.0.0.1:1", request_timeout_ms: 500, max_retries: 0)

    {micros, result} = :timer.tc(fn -> Engine.open(Path.join(dir, "down.db"), s3: s3) end)
    assert {:error, "s3 store: " <> _} = result
    assert micros < 10_000_000
    refute File.exists?(Path.join(dir, "down.db"))
  end

  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "commits fail and roll back while S3 is unreachable, then resume", %{s3: s3, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port)
    endpoint = "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}"
    # One retry: a server may close a connection after refusing a
    # conditional PUT (the open's probe), and through the proxy the client can
    # reuse it before seeing the close.
    s3 = Keyword.merge(s3, endpoint: endpoint, request_timeout_ms: 1_000, max_retries: 1)

    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer)")
    :ok = Engine.execute(db, "insert into t values (1)")

    :ok = Sediment.TcpProxy.cut(proxy)
    assert {:error, "s3 store: " <> _} = Engine.execute(db, "insert into t values (2)")
    assert count(db, "t") == 1

    :ok = Sediment.TcpProxy.resume(proxy)
    :ok = Engine.execute(db, "insert into t values (3)")
    assert count(db, "t") == 2
    :ok = Engine.close(db)

    {:ok, restored} = Engine.open(Path.join(dir, "b.db"), s3: s3)

    {:ok, stmt} = Engine.prepare(restored, "select x from t order by x")
    assert {:ok, [[1], [3]]} = Engine.fetch_all(restored, stmt)
    :ok = Engine.close(restored)
  end

  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "closing an async database uploads its pending commits first", %{s3: s3, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port, max_delay_ms: 300)

    slow =
      Keyword.merge(s3,
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        durability: :async
      )

    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: slow)
    :ok = Engine.execute(db, "create table t (x integer)")
    for i <- 1..5, do: :ok = Engine.execute(db, "insert into t values (#{i})")
    assert {:ok, %{durability: "async", pending_frames: pending}} = S3.info(db)
    assert pending > 0
    :ok = Engine.close(db)

    {:ok, _} = S3.restore(Path.join(dir, "r.db"), s3)
    {:ok, restored} = Engine.open(Path.join(dir, "r.db"))
    assert count(restored, "t") == 5
    :ok = Engine.close(restored)
  end

  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a script's async commits are uploaded when it exits", %{s3: s3, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port, max_delay_ms: 300)

    slow =
      Keyword.merge(s3,
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        durability: :async
      )

    script = Path.join(dir, "script.exs")

    File.write!(script, """
    alias Sediment.Engine
    {:ok, db} = Engine.open(#{inspect(Path.join(dir, "a.db"))}, s3: #{inspect(slow)})
    :ok = Engine.execute(db, "create table t (x integer)")
    for i <- 1..20, do: :ok = Engine.execute(db, "insert into t values (\#{i})")
    {:ok, %{pending_frames: pending}} = Sediment.S3.info(db)
    IO.puts("pending " <> to_string(pending))
    """)

    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    {output, 0} =
      System.cmd(System.find_executable("elixir"), paths ++ [script], stderr_to_stdout: true)

    # The script ended with commits still queued, and exited without closing.
    assert [_, pending] = Regex.run(~r/pending (\d+)/, output)
    assert String.to_integer(pending) > 0

    {:ok, _} = S3.restore(Path.join(dir, "r.db"), s3)
    {:ok, restored} = Engine.open(Path.join(dir, "r.db"))
    assert count(restored, "t") == 20
    :ok = Engine.close(restored)
  end

  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a failed group commit fences the writer and hides its rows", %{s3: s3, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(8333)
    endpoint = "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}"

    s3 =
      Keyword.merge(s3,
        endpoint: endpoint,
        request_timeout_ms: 1_000,
        max_retries: 0,
        group_commit: true
      )

    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer)")
    :ok = Engine.execute(db, "insert into t values (1)")

    :ok = Sediment.TcpProxy.cut(proxy)
    assert {:error, "s3 writer fenced: " <> _} = Engine.execute(db, "insert into t values (2)")
    :ok = Sediment.TcpProxy.resume(proxy)

    # the failed batch is in turso's memory but not in S3: nothing reads it
    assert {:ok, stmt} = Engine.prepare(db, "select count(*) from t")
    assert {:error, "s3 writer fenced: " <> _} = Engine.step(db, stmt)
    :ok = Engine.close(db)

    {:ok, restored} = Engine.open(Path.join(dir, "b.db"), s3: s3)
    assert count(restored, "t") == 1
    :ok = Engine.close(restored)
  end

  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "a pool recovers by itself after a failed group commit", %{s3: s3, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(8333)

    s3 =
      Keyword.merge(s3,
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        request_timeout_ms: 1_000,
        max_retries: 0,
        group_commit: true
      )

    {:ok, pool} =
      Sediment.start_link(
        database: Path.join(dir, "a.db"),
        s3: s3,
        pool_size: 2,
        backoff_min: 50,
        backoff_max: 200
      )

    Sediment.query!(pool, "create table t (x integer)")
    Sediment.query!(pool, "insert into t values (1)")

    :ok = Sediment.TcpProxy.cut(proxy)

    assert {:error, %Sediment.Error{message: "s3 writer fenced: " <> _}} =
             Sediment.query(pool, "insert into t values (2)")

    :ok = Sediment.TcpProxy.resume(proxy)

    # every connection reopens, restoring what reached S3, without a restart
    assert eventually(fn ->
             match?({:ok, %{rows: [[1]]}}, Sediment.query(pool, "select count(*) from t"))
           end)

    Sediment.query!(pool, "insert into t values (3)")
    assert %{rows: [[1], [3]]} = Sediment.query!(pool, "select x from t order by x")
    GenServer.stop(pool)
  end

  test "snapshot, refresh and restore emit telemetry spans", %{s3: s3, dir: dir} do
    id = make_ref()
    parent = self()

    :telemetry.attach_many(
      id,
      for(op <- [:snapshot, :refresh, :restore], do: [:sediment, :s3, op, :stop]),
      fn event, measurements, metadata, _ -> send(parent, {event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x integer)")
    :ok = S3.snapshot(db)
    assert_receive {[:sediment, :s3, :snapshot, :stop], %{duration: _}, %{result: :ok}}

    {:ok, replica} = Engine.open(Path.join(dir, "r.db"), s3: Keyword.put(s3, :mode, :replica))
    {:ok, _} = S3.refresh(replica)
    assert_receive {[:sediment, :s3, :refresh, :stop], _, %{result: {:ok, _}}}
    :ok = Engine.close(replica)

    restored = Path.join(dir, "restored.db")
    {:ok, _} = S3.restore(restored, s3)

    assert_receive {[:sediment, :s3, :restore, :stop], _, %{path: ^restored, result: {:ok, _}}}

    :ok = Engine.close(db)
  end

  # /dev/full fails every write with ENOSPC: the disk fills during a restore.
  @tag skip: not File.exists?("/dev/full") && "needs /dev/full"
  test "a restore onto a full disk fails cleanly and leaves no database behind",
       %{s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "src.db"), s3: s3)
    :ok = Engine.execute(db, "create table t (x); insert into t values (1), (2)")
    :ok = S3.snapshot(db)
    :ok = Engine.execute(db, "insert into t values (3)")
    :ok = Engine.close(db)

    dest = Path.join(dir, "restored.db")
    File.ln_s!("/dev/full", dest <> ".s3-restore")
    assert {:error, "s3 local io: " <> message} = S3.restore(dest, s3)
    assert message =~ "#{dest}: No space left"
    refute File.exists?(dest)

    File.rm(dest <> ".s3-restore")
    assert {:ok, _} = S3.restore(dest, s3)
    {:ok, restored} = Engine.open(dest)
    assert count(restored, "t") == 3
    :ok = Engine.close(restored)
  end

  test "connections to the same path share the S3 storage", %{s3: s3, dir: dir} do
    path = Path.join(dir, "a.db")
    {:ok, first} = Engine.open(path, s3: s3)
    {:ok, second} = Engine.open(path, s3: s3)
    :ok = Engine.execute(first, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(second, "INSERT INTO t VALUES (1)")
    assert count(first, "t") == 1
    {:ok, info} = S3.info(first)
    assert info.uploaded_frames == 2
    :ok = Engine.close(first)
    :ok = Engine.close(second)
  end

  test "works through DBConnection", %{s3: s3, dir: dir} do
    {:ok, conn} =
      DBConnection.start_link(Sediment.Connection,
        database: Path.join(dir, "pool.db"),
        s3: s3,
        pool_size: 2
      )

    {:ok, _} = Sediment.query(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
    {:ok, _} = Sediment.query(conn, "INSERT INTO t VALUES (?1)", [1])
    {:ok, %{rows: [[1]]}} = Sediment.query(conn, "SELECT count(*) FROM t", [])
    GenServer.stop(conn)

    {:ok, restored} = Engine.open(Path.join(dir, "fresh.db"), s3: s3)
    assert count(restored, "t") == 1
    :ok = Engine.close(restored)
  end

  test "rejects invalid option combinations", %{s3: s3, dir: dir} do
    assert {:error, msg} = Engine.open(":memory:", s3: s3)
    assert msg =~ "file path"

    assert {:error, msg} = Engine.open(Path.join(dir, "x.db"), s3: s3, journal_mode: :wal)
    assert msg =~ "mvcc"

    assert {:error, msg} = Engine.open(Path.join(dir, "r.db"), s3: s3, mode: :readonly)
    assert msg =~ "replica"

    assert {:error, msg} = Engine.open(Path.join(dir, "y.db"), s3: [prefix: "x"])
    assert msg =~ "bucket"

    assert {:error, msg} = Engine.open(Path.join(dir, "z.db"), s3: [bucket: "b", bogus: 1])
    assert msg =~ "unknown option"
  end

  test "info/1 on a plain database is an error", %{dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "plain.db"))
    assert {:error, "not an s3 database"} = S3.info(db)
    :ok = Engine.close(db)
  end

  test "restore/3 rebuilds a past state into a local file", %{s3: s3, dir: dir} do
    s3 = Keyword.put(s3, :retain_epochs, 2)
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")
    Process.sleep(1100)
    before = DateTime.utc_now()
    Process.sleep(1100)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (2)")
    :ok = S3.snapshot(db)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (3)")

    {:ok, info} = S3.restore(Path.join(dir, "past.db"), s3, at: before)
    assert info.frames == 2
    {:ok, past} = Engine.open(Path.join(dir, "past.db"))
    assert count(past, "t") == 1
    :ok = Engine.close(past)

    {:ok, _} = S3.restore(Path.join(dir, "latest.db"), s3)
    {:ok, latest} = Engine.open(Path.join(dir, "latest.db"))
    assert count(latest, "t") == 3
    :ok = Engine.close(latest)
    :ok = Engine.close(db)
  end

  describe "leaving MVCC" do
    defp ids(db) do
      {:ok, stmt} = Engine.prepare(db, "SELECT id FROM t ORDER BY id")
      {:ok, rows} = Engine.fetch_all(db, stmt)
      :ok = Engine.release(db, stmt)
      List.flatten(rows)
    end

    defp restored_ids(s3, dir) do
      {:ok, db} =
        Engine.open(Path.join(dir, "restored-#{System.unique_integer([:positive])}.db"), s3: s3)

      ids = ids(db)
      :ok = Engine.close(db)
      ids
    end

    test "switching the journal mode fences the writer", %{s3: s3, dir: dir} do
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (2)")

      assert {:error, msg} = Engine.execute(db, "PRAGMA journal_mode = wal")
      assert msg =~ "left MVCC"
      assert {:error, _} = Engine.execute(db, "INSERT INTO t VALUES (3)")
      {:ok, info} = S3.info(db)
      assert info.poisoned =~ "MVCC"
      :ok = Engine.close(db)

      assert restored_ids(s3, dir) == [1, 2]
    end

    test "a multi-statement switch stops before the next write", %{s3: s3, dir: dir} do
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

      assert {:error, _} =
               Engine.execute(db, "PRAGMA journal_mode = 'wal'; INSERT INTO t VALUES (2)")

      :ok = Engine.close(db)
      assert restored_ids(s3, dir) == [1]
    end

    test "through DBConnection", %{s3: s3, dir: dir} do
      {:ok, conn} =
        DBConnection.start_link(Sediment.Connection,
          database: Path.join(dir, "pool.db"),
          s3: s3,
          pool_size: 2
        )

      {:ok, _} = Sediment.query(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
      {:ok, _} = Sediment.query(conn, "INSERT INTO t VALUES (1)", [])
      assert {:error, _} = Sediment.query(conn, "PRAGMA journal_mode = wal", [])
      assert {:error, _} = Sediment.query(conn, "INSERT INTO t VALUES (2)", [])
      GenServer.stop(conn)

      assert restored_ids(s3, dir) == [1]
    end
  end

  test "a killed pool releases the lease though clients hold its statements",
       %{prefix: prefix, s3: s3, dir: dir} do
    Process.flag(:trap_exit, true)

    {:ok, pool} =
      DBConnection.start_link(Sediment.Connection,
        database: Path.join(dir, "pool.db"),
        s3: s3,
        pool_size: 2
      )

    {:ok, _} = Sediment.query(pool, "CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
    query = %Sediment.Query{statement: "SELECT count(*) FROM t"}
    {:ok, prepared} = DBConnection.prepare(pool, query)

    # The pool's processes die without disconnect/2; `prepared` stays alive here.
    Process.exit(pool, :kill)
    assert_receive {:EXIT, ^pool, :killed}

    other = s3_opts(prefix, "writer-b")

    opened =
      Enum.find_value(1..50, fn _ ->
        case Engine.open(Path.join(dir, "b.db"), s3: other) do
          {:ok, db} -> db
          {:error, _} -> Process.sleep(100) && nil
        end
      end)

    assert opened, "the killed pool's lease was never released"
    assert count(opened, "t") == 0
    :ok = Engine.close(opened)
    assert prepared.ref
  end

  describe "plain connections to an open S3 database" do
    test "share its group-commit durability", %{s3: s3, dir: dir} do
      s3 = Keyword.put(s3, :group_commit, true)
      path = Path.join(dir, "g.db")
      {:ok, db} = Engine.open(path, s3: s3)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

      {:ok, plain} = Engine.open(path)
      :ok = Engine.execute(plain, "INSERT INTO t VALUES (2)")
      # The pool runs at synchronous = NORMAL unless the storage forces FULL.
      {:ok, pool} = Sediment.start_link(database: path, pool_size: 2)
      {:ok, _} = Sediment.query(pool, "INSERT INTO t VALUES (3)", [])

      # Restore while everything is still open: nothing pending locally.
      {:ok, _} = S3.restore(Path.join(dir, "now.db"), s3)
      {:ok, now} = Engine.open(Path.join(dir, "now.db"))
      assert count(now, "t") == 3
      :ok = Engine.close(now)
      assert {:ok, %{uploaded_frames: 4}} = S3.info(plain)

      GenServer.stop(pool)
      :ok = Engine.close(plain)
      :ok = Engine.close(db)
    end

    test "can't switch it out of MVCC", %{s3: s3, dir: dir} do
      path = Path.join(dir, "a.db")
      {:ok, db} = Engine.open(path, s3: s3)
      assert {:error, msg} = Engine.open(path, journal_mode: :wal)
      assert msg =~ "journal_mode must stay mvcc"
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.close(db)
    end
  end

  test "replica connections to one path refresh concurrently", %{s3: s3, dir: dir} do
    {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3)
    :ok = Engine.execute(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

    replica_opts = Keyword.put(s3, :mode, :replica)
    path = Path.join(dir, "r.db")
    {:ok, a} = Engine.open(path, s3: replica_opts)
    {:ok, b} = Engine.open(path, s3: replica_opts)

    for round <- 1..5 do
      :ok = Engine.execute(writer, "INSERT INTO t VALUES (#{round})")
      results = [a, b] |> Enum.map(&Task.async(fn -> S3.refresh(&1) end)) |> Task.await_many()
      assert Enum.all?(results, &match?({:ok, _}, &1)), inspect(results)
      assert count(a, "t") == round
      assert count(b, "t") == round
    end

    :ok = Engine.close(a)
    :ok = Engine.close(b)
    :ok = Engine.close(writer)
    # Working copies and staging files are gone with their connections.
    assert Path.wildcard(Path.join(dir, ".r.*")) == []
  end

  test "nil options are unset and a bad endpoint is an error, not a crash",
       %{s3: s3, dir: dir} do
    nils = [session_token: nil, owner: nil, checkpoint_threshold: nil, retain_epochs: nil]
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: Keyword.merge(s3, nils))
    :ok = Engine.close(db)

    for endpoint <- ["localhost:9000", "http://", "ftp://x", "http://bad host"] do
      assert {:error, msg} =
               Engine.open(Path.join(dir, "b.db"), s3: Keyword.put(s3, :endpoint, endpoint))

      assert msg =~ "invalid endpoint"
    end
  end

  test "a fenced pool reconnects and restores once the other writer is gone",
       %{prefix: prefix, dir: dir} do
    s3 = s3_opts(prefix, "node")

    {:ok, pool} =
      Sediment.start_link(
        database: Path.join(dir, "a.db"),
        s3: s3,
        pool_size: 2,
        idle_interval: 100,
        backoff_min: 50,
        backoff_max: 200
      )

    {:ok, _} = Sediment.query(pool, "CREATE TABLE t (id TEXT PRIMARY KEY)", [])
    {:ok, _} = Sediment.query(pool, "INSERT INTO t VALUES ('a1')", [])

    # Another process with the same owner takes over, writes, and leaves.
    {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3)
    :ok = Engine.execute(b, "INSERT INTO t VALUES ('b1')")
    :ok = Engine.close(b)

    # The pool is fenced: its next statement fails (reads too) ...
    assert {:error, %Sediment.Error{message: message}} =
             Sediment.query(pool, "INSERT INTO t VALUES ('a2')", [])

    assert message =~ "fenced"

    # ... and it recovers by itself: reconnect, restore from S3, go on.
    rows =
      Enum.find_value(1..50, fn _ ->
        case Sediment.query(pool, "SELECT id FROM t ORDER BY id", []) do
          {:ok, %{rows: rows}} -> rows
          {:error, _} -> Process.sleep(100) && nil
        end
      end)

    assert rows == [["a1"], ["b1"]]
    {:ok, _} = Sediment.query(pool, "INSERT INTO t VALUES ('a2')", [])
    GenServer.stop(pool)

    {:ok, restored} = Engine.open(Path.join(dir, "c.db"), s3: s3)
    {:ok, stmt} = Engine.prepare(restored, "SELECT id FROM t ORDER BY id")
    assert {:ok, [["a1"], ["a2"], ["b1"]]} = Engine.fetch_all(restored, stmt)
    :ok = Engine.close(restored)
  end

  describe "one storage per file" do
    test "path aliases share the writer", %{s3: s3, dir: dir} do
      File.mkdir_p!(Path.join(dir, "sub"))
      File.ln_s!(dir, Path.join(dir, "link"))
      {:ok, a} = Engine.open(Path.join(dir, "a.db"), s3: s3)
      :ok = Engine.execute(a, "CREATE TABLE t (x INTEGER)")
      {:ok, b} = Engine.open(Path.join([dir, "sub", "..", "a.db"]), s3: s3)
      {:ok, c} = Engine.open(Path.join([dir, "link", "a.db"]), s3: s3)
      :ok = Engine.execute(b, "INSERT INTO t VALUES (1)")
      :ok = Engine.execute(c, "INSERT INTO t VALUES (2)")
      :ok = Engine.execute(a, "INSERT INTO t VALUES (3)")
      assert {:ok, %{poisoned: nil, uploaded_frames: 4}} = S3.info(a)
      for db <- [a, b, c], do: :ok = Engine.close(db)
    end

    test "an S3 open refuses a file open without S3", %{s3: s3, dir: dir} do
      path = Path.join(dir, "a.db")
      {:ok, a} = Engine.open(path, s3: s3)
      :ok = Engine.execute(a, "CREATE TABLE t (x INTEGER)")
      :ok = Engine.close(a)

      {:ok, plain} = Engine.open(path)
      assert {:error, msg} = Engine.open(path, s3: s3)
      assert msg =~ "open without :s3"
      :ok = Engine.close(plain)

      {:ok, b} = Engine.open(path, s3: s3)
      :ok = Engine.close(b)
    end

    test "a second open with other settings is refused", %{s3: s3, dir: dir} do
      path = Path.join(dir, "a.db")
      {:ok, a} = Engine.open(path, s3: s3)

      for change <- [group_commit: true, lease_ttl_ms: 9_000, owner: "someone-else"] do
        assert {:error, msg} = Engine.open(path, s3: Keyword.merge(s3, [change]))
        assert msg =~ "other S3 settings"
      end

      # Credentials and timeouts may differ.
      {:ok, b} = Engine.open(path, s3: Keyword.put(s3, :request_timeout_ms, 5_000))
      :ok = Engine.close(b)
      :ok = Engine.close(a)
    end
  end

  @tag timeout: 180_000
  @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
  test "random network cuts never lose an acknowledged commit", %{prefix: prefix, dir: dir} do
    {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port)

    s3 =
      Keyword.merge(s3_opts(prefix, "chaos"),
        endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
        request_timeout_ms: 1_000,
        max_retries: 1,
        checkpoint_threshold: 16_384
      )

    seed = System.unique_integer([:positive])

    open = fn n ->
      Enum.find_value(1..300, fn _ ->
        case Engine.open(Path.join(dir, "c#{n}-#{System.unique_integer([:positive])}.db"), s3: s3) do
          {:ok, db} -> db
          {:error, _} -> Process.sleep(50) && nil
        end
      end) || flunk("could not reopen")
    end

    db = open.(0)
    :ok = Engine.execute(db, "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, pad TEXT)")

    chaos =
      spawn_link(fn ->
        :rand.seed(:exsss, {seed, 11, 13})

        Stream.repeatedly(fn ->
          Process.sleep(:rand.uniform(300))
          :ok = Sediment.TcpProxy.cut(proxy)
          Process.sleep(:rand.uniform(150))
          :ok = Sediment.TcpProxy.resume(proxy)
        end)
        |> Stream.run()
      end)

    {db, acked} =
      Enum.reduce(1..150, {db, []}, fn id, {db, acked} ->
        case Engine.execute(db, "INSERT INTO t VALUES (#{id}, hex(randomblob(200)))") do
          :ok ->
            {db, [id | acked]}

          {:error, message} ->
            if message =~ "fenced" do
              Engine.close(db)
              {open.(id), acked}
            else
              {db, acked}
            end
        end
      end)

    Process.unlink(chaos)
    Process.exit(chaos, :kill)
    :ok = Sediment.TcpProxy.resume(proxy)
    Engine.close(db)

    direct = s3_opts(prefix, "chaos")
    {:ok, _} = S3.restore(Path.join(dir, "final.db"), direct)
    {:ok, final} = Engine.open(Path.join(dir, "final.db"))
    {:ok, stmt} = Engine.prepare(final, "SELECT id FROM t")
    {:ok, rows} = Engine.fetch_all(final, stmt)
    ids = MapSet.new(List.flatten(rows))
    :ok = Engine.close(final)

    assert Enum.count_until(acked, 21) > 20, "seed #{seed}: too few commits got through"
    lost = Enum.reject(acked, &MapSet.member?(ids, &1))
    assert lost == [], "seed #{seed}: acknowledged but lost: #{inspect(lost)}"
    assert Enum.all?(ids, &(&1 in 1..150))
  end

  describe "review round 4" do
    test "plain pools through a file symlink or hard link inherit S3 durability",
         %{s3: s3, dir: dir} do
      s3 = Keyword.put(s3, :group_commit, true)
      path = Path.join(dir, "a.db")
      {:ok, db} = Engine.open(path, s3: s3)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

      File.ln_s!(path, Path.join(dir, "a.symlink.db"))
      File.ln!(path, Path.join(dir, "a.hardlink.db"))

      for {alias_path, id} <- [{"a.symlink.db", 2}, {"a.hardlink.db", 3}] do
        {:ok, pool} = Sediment.start_link(database: Path.join(dir, alias_path), pool_size: 1)
        {:ok, _} = Sediment.query(pool, "INSERT INTO t VALUES (#{id})", [])
        GenServer.stop(pool)
      end

      {:ok, _} = S3.restore(Path.join(dir, "now.db"), s3)
      {:ok, now} = Engine.open(Path.join(dir, "now.db"))
      assert count(now, "t") == 3
      :ok = Engine.close(now)
      :ok = Engine.close(db)
    end

    @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
    test "an idle pool notices a takeover through reads alone", %{prefix: prefix, dir: dir} do
      {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port)

      a =
        Keyword.merge(s3_opts(prefix, "owner-a"),
          endpoint: "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}",
          lease_ttl_ms: 1_000,
          request_timeout_ms: 500,
          max_retries: 0
        )

      {:ok, pool} =
        Sediment.start_link(
          database: Path.join(dir, "a.db"),
          s3: a,
          pool_size: 1,
          idle_interval: 100,
          backoff_min: 50,
          backoff_max: 200
        )

      {:ok, _} = Sediment.query(pool, "CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
      {:ok, _} = Sediment.query(pool, "INSERT INTO t VALUES (1)", [])

      :ok = Sediment.TcpProxy.cut(proxy)
      Process.sleep(1_400)
      {:ok, b} = Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "owner-b"))
      :ok = Engine.execute(b, "INSERT INTO t VALUES (2)")
      :ok = Engine.close(b)
      :ok = Sediment.TcpProxy.resume(proxy)

      rows =
        Enum.find_value(1..100, fn _ ->
          case Sediment.query(pool, "SELECT id FROM t ORDER BY id", []) do
            {:ok, %{rows: [[1], [2]] = rows}} -> rows
            _ -> Process.sleep(100) && nil
          end
        end)

      assert rows == [[1], [2]], "the pool kept serving its stale copy"
      GenServer.stop(pool)
    end

    test "an owner dying during an operation frees the lease without another call",
         %{prefix: prefix, dir: dir} do
      test = self()

      owner =
        spawn(fn ->
          {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3_opts(prefix, "owner-a"))
          :ok = Sediment.Native.monitor_owner(db)
          {:ok, stmt} = Engine.prepare(db, "SELECT 1")
          send(test, {:db, db, stmt})
          Process.sleep(:infinity)
        end)

      assert_receive {:db, db, stmt}, 10_000

      slow =
        Task.async(fn ->
          Engine.execute(
            db,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000000) SELECT count(*) FROM c"
          )
        end)

      Process.sleep(200)
      Process.exit(owner, :kill)
      Task.await(slow, 60_000)

      opened =
        Enum.find_value(1..40, fn _ ->
          case Engine.open(Path.join(dir, "b.db"), s3: s3_opts(prefix, "owner-b")) do
            {:ok, b} -> b
            {:error, _} -> Process.sleep(100) && nil
          end
        end)

      assert opened, "the dead owner's lease was never released"
      :ok = Engine.close(opened)
      # The statement is still referenced here.
      assert stmt
    end

    @tag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"
    test "a failed lazy replica refresh is retried, not skipped", %{s3: s3, dir: dir} do
      {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3)
      :ok = Engine.execute(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(writer, "INSERT INTO t VALUES (1)")

      {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port)
      proxied = "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}"
      replica = Keyword.merge(s3, mode: :replica, request_timeout_ms: 500, max_retries: 0)
      path = Path.join(dir, "r.db")
      {:ok, pool_a} = Sediment.start_link(database: path, s3: replica, pool_size: 1)

      {:ok, pool_b} =
        Sediment.start_link(
          database: path,
          s3: Keyword.put(replica, :endpoint, proxied),
          pool_size: 1
        )

      {:ok, %{rows: [[1]]}} = Sediment.query(pool_b, "SELECT id FROM t", [])
      :ok = Engine.execute(writer, "INSERT INTO t VALUES (2)")
      {:ok, _} = S3.refresh(pool_a)

      :ok = Sediment.TcpProxy.cut(proxy)
      assert {:error, _} = Sediment.query(pool_b, "SELECT id FROM t ORDER BY id", [])
      :ok = Sediment.TcpProxy.resume(proxy)

      rows =
        Enum.find_value(1..50, fn _ ->
          case Sediment.query(pool_b, "SELECT id FROM t ORDER BY id", []) do
            {:ok, %{rows: rows}} -> rows
            _ -> Process.sleep(100) && nil
          end
        end)

      assert rows == [[1], [2]]
      GenServer.stop(pool_a)
      GenServer.stop(pool_b)
      :ok = Engine.close(writer)
    end
  end

  test "a large pool connecting at once shares one storage and one lease", %{s3: s3, dir: dir} do
    {:ok, pool} =
      Sediment.start_link(database: Path.join(dir, "big.db"), s3: s3, pool_size: 8)

    {:ok, _} = Sediment.query(pool, "CREATE TABLE t (id INTEGER PRIMARY KEY)", [])

    1..64
    |> Task.async_stream(&Sediment.query(pool, "INSERT INTO t VALUES (?1)", [&1]),
      max_concurrency: 16,
      timeout: 60_000
    )
    |> Enum.each(fn {:ok, result} -> assert {:ok, _} = result end)

    {:ok, %{rows: [[64]]}} = Sediment.query(pool, "SELECT count(*) FROM t", [])
    {:ok, info} = S3.info(pool)
    assert info.generation == 1
    assert info.uploaded_frames == 65
    GenServer.stop(pool)

    {:ok, restored} = Engine.open(Path.join(dir, "check.db"), s3: s3)
    assert count(restored, "t") == 64
    :ok = Engine.close(restored)
  end

  test "restore/3 edge cases", %{s3: s3, dir: dir} do
    {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")

    future = DateTime.add(DateTime.utc_now(), 3600)
    {:ok, _} = S3.restore(Path.join(dir, "future.db"), s3, at: future)
    {:ok, copy} = Engine.open(Path.join(dir, "future.db"))
    assert count(copy, "t") == 1
    :ok = Engine.close(copy)

    assert {:error, msg} = S3.restore(Path.join(dir, "e.db"), s3, epoch: 999)
    assert msg =~ "not retained"
    refute File.exists?(Path.join(dir, "e.db"))

    assert {:error, msg} =
             S3.restore(Path.join(dir, "none.db"), Keyword.put(s3, :prefix, "nothing/here"))

    assert msg =~ "no database"
    :ok = Engine.close(db)
  end

  describe "review round 5" do
    test "an owner dying at any point frees the lease", %{dir: dir} do
      for round <- 1..8 do
        prefix =
          "elixir/owner-death/#{System.os_time()}-#{System.unique_integer([:positive])}"

        test = self()

        owner =
          spawn(fn ->
            {:ok, db} =
              Engine.open(Path.join(dir, "o#{round}.db"), s3: s3_opts(prefix, "owner-a"))

            :ok = Sediment.Native.monitor_owner(db)
            :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
            {:ok, stmt} = Engine.prepare(db, "SELECT count(*) FROM t")
            send(test, {:db, db, stmt})
            Process.sleep(:infinity)
          end)

        assert_receive {:db, db, stmt}, 10_000

        client =
          spawn(fn ->
            Stream.repeatedly(fn -> Engine.execute(db, "SELECT count(*) FROM t") end)
            |> Enum.take_while(&(&1 == :ok))
          end)

        Process.sleep(:rand.uniform(50))
        Process.exit(owner, :kill)

        opened =
          Enum.find_value(1..40, fn _ ->
            case Engine.open(Path.join(dir, "b#{round}.db"), s3: s3_opts(prefix, "owner-b")) do
              {:ok, b} -> b
              {:error, _} -> Process.sleep(100) && nil
            end
          end)

        assert opened, "round #{round}: the dead owner's lease was never released"
        :ok = Engine.close(opened)
        Process.exit(client, :kill)
        assert stmt
      end
    end

    test "simultaneous S3 and plain opens through a file symlink never lose a write",
         %{s3: s3, dir: dir} do
      for round <- 1..6 do
        s3 =
          Keyword.merge(s3,
            prefix: "elixir/alias-race/#{System.os_time()}-#{System.unique_integer([:positive])}"
          )

        path = Path.join(dir, "a#{round}.db")
        {:ok, seed} = Engine.open(path, s3: s3)
        :ok = Engine.execute(seed, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        :ok = Engine.execute(seed, "INSERT INTO t VALUES (1)")
        :ok = Engine.close(seed)
        alias_path = Path.join(dir, "alias#{round}.db")
        File.ln_s!(path, alias_path)

        s3_open = Task.async(fn -> Engine.open(path, s3: s3) end)
        plain_open = Task.async(fn -> Engine.open(alias_path) end)
        writer = Task.await(s3_open, 30_000)
        plain = Task.await(plain_open, 30_000)

        with {:ok, w} <- writer, {:ok, p} <- plain do
          case Engine.execute(p, "INSERT INTO t VALUES (2)") do
            :ok ->
              {:ok, _} = S3.restore(Path.join(dir, "check#{round}.db"), s3)
              {:ok, check} = Engine.open(Path.join(dir, "check#{round}.db"))
              assert count(check, "t") == 2, "round #{round}: acknowledged plain write not in S3"
              :ok = Engine.close(check)

            {:error, _} ->
              :ok
          end

          :ok = Engine.close(p)
          :ok = Engine.close(w)
        else
          # One side was refused (a plain handle blocks an S3 open): fine.
          _ -> for {:ok, db} <- [writer, plain], do: Engine.close(db)
        end
      end
    end
  end

  test "an S3 open stuck on the network doesn't hold up opens of other files",
       %{s3: s3, dir: dir} do
    # A server that accepts connections and never answers.
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    stuck =
      Keyword.merge(s3,
        endpoint: "http://127.0.0.1:#{port}",
        request_timeout_ms: 5_000,
        max_retries: 0
      )

    s3_open = Task.async(fn -> Engine.open(Path.join(dir, "stuck.db"), s3: stuck) end)
    Process.sleep(300)
    {micros, {:ok, other}} = :timer.tc(fn -> Engine.open(Path.join(dir, "other.db")) end)
    assert micros < 1_000_000, "a plain open waited #{div(micros, 1000)} ms for an S3 open"
    :ok = Engine.close(other)
    {micros, {:ok, other}} = :timer.tc(fn -> Engine.open(Path.join(dir, "s3.db"), s3: s3) end)
    assert micros < 2_500_000, "an S3 open waited #{div(micros, 1000)} ms for another"
    :ok = Engine.close(other)
    assert {:error, _} = Task.await(s3_open, 30_000)
    :gen_tcp.close(listen)

    # The failed open left nothing behind: the file opens with working S3.
    {:ok, db} = Engine.open(Path.join(dir, "stuck.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.close(db)
  end

  test "S3 requests are counted, logged per process, and refused once the budget's stop file exists",
       %{s3: s3, dir: dir} do
    meter = Path.join(dir, "meter")
    script = Path.join(dir, "meter.exs")

    File.write!(script, """
    alias Sediment.Engine
    {:ok, db} = Engine.open(#{inspect(Path.join(dir, "a.db"))}, s3: #{inspect(s3)})
    :ok = Engine.execute(db, "create table t (x integer)")
    counts = Sediment.S3.request_counts()
    IO.puts("puts " <> to_string(counts["PutObject"].count))
    File.write!(#{inspect(Path.join(meter, "STOP"))}, "")
    {:error, error} = Engine.execute(db, "insert into t values (1)")
    IO.puts("refused " <> error)
    IO.puts("after " <> to_string(Sediment.S3.request_counts()["PutObject"].count))
    """)

    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    {output, _} =
      System.cmd(System.find_executable("elixir"), paths ++ [script],
        stderr_to_stdout: true,
        env: [{"SEDIMENT_S3_METER_DIR", meter}]
      )

    [_, puts] = Regex.run(~r/puts (\d+)/, output)
    assert String.to_integer(puts) > 0
    assert output =~ ~r/refused .*budget exhausted/
    assert [_, ^puts] = Regex.run(~r/after (\d+)/, output)

    [log] = Path.wildcard(Path.join(meter, "requests-*.log"))
    logged = log |> File.read!() |> String.split("\n", trim: true)
    assert Enum.count(logged, &(&1 =~ ~r/^R \d+ PutObject A$/)) == String.to_integer(puts)
    assert Enum.all?(logged, &(&1 =~ ~r/^[RE] \d+ \w+ \w+$/))
  end

  test "with SEDIMENT_S3_TRACE=1 every request is traced with its key, condition and answer",
       %{s3: s3, dir: dir} do
    meter = Path.join(dir, "trace")
    script = Path.join(dir, "trace.exs")

    File.write!(script, """
    alias Sediment.Engine
    {:ok, db} = Engine.open(#{inspect(Path.join(dir, "a.db"))}, s3: #{inspect(s3)})
    :ok = Engine.execute(db, "create table t (x integer)")
    :ok = Engine.close(db)
    """)

    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    {_, 0} =
      System.cmd(System.find_executable("elixir"), paths ++ [script],
        stderr_to_stdout: true,
        env: [{"SEDIMENT_S3_METER_DIR", meter}, {"SEDIMENT_S3_TRACE", "1"}]
      )

    [log] = Path.wildcard(Path.join(meter, "requests-*.log"))
    lines = log |> File.read!() |> String.split("\n", trim: true)
    traced = for "T " <> rest <- lines, do: String.split(rest, " ")

    answered =
      for "A " <> rest <- lines,
          into: %{},
          do: rest |> String.split(" ") |> then(fn [_, id | a] -> {id, a} end)

    assert length(traced) == Enum.count(lines, &String.starts_with?(&1, "R "))

    assert Enum.all?(traced, fn [_ms, id, _op, _path, _cond, _tag] ->
             Map.has_key?(answered, id)
           end)

    # The bootstrap creates the manifest create-only, and it lands.
    assert Enum.any?(traced, fn
             [_, id, "PutObject", path, "if-none-match", _tag] ->
               String.ends_with?(path, "/manifest.json") and hd(answered[id]) == "200"

             _ ->
               false
           end)
  end

  test "S3 connections wait longer for the write lock by default", %{s3: s3} do
    assert Sediment.Pragma.busy_timeout(s3: s3) == 15_000
    assert Sediment.Pragma.busy_timeout(s3: s3, busy_timeout: 100) == 100
    assert Sediment.Pragma.busy_timeout([]) == 2000
  end

  # Some servers (SeaweedFS) store an EMPTY object when a PUT's
  # connection breaks right after its headers. Every upload carries its
  # SHA-256, so the server rejects the truncated body instead.
  describe "uploads cut off after their headers" do
    @describetag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"

    defp truncating(s3, faults \\ []) do
      {:ok, proxy} = Sediment.TcpProxy.start_link(URI.parse(endpoint()).port, faults)
      url = "http://127.0.0.1:#{Sediment.TcpProxy.port(proxy)}"
      {proxy, Keyword.merge(s3, endpoint: url, max_retries: 1, request_timeout_ms: 2_000)}
    end

    # Faults apply to new connections: drop the kept-alive ones.
    defp faults(proxy, faults) do
      :ok = Sediment.TcpProxy.set_faults(proxy, faults)
      :ok = Sediment.TcpProxy.cut(proxy)
      :ok = Sediment.TcpProxy.resume(proxy)
    end

    defp restored_count(s3, dir) do
      {:ok, db} =
        Engine.open(Path.join(dir, "restored-#{System.unique_integer([:positive])}.db"), s3: s3)

      n = count(db, "t")
      :ok = Engine.close(db)
      n
    end

    test "leave no empty manifest behind", %{s3: s3, dir: dir} do
      {_proxy, cut} = truncating(s3, truncate_puts: 1.0, truncate_puts_matching: "manifest.json")
      assert {:error, _} = Engine.open(Path.join(dir, "a.db"), s3: cut)

      # The database can still be created.
      {:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: s3)
      :ok = Engine.execute(db, "create table t (x integer)")
      :ok = Engine.execute(db, "insert into t values (1)")
      :ok = Engine.close(db)
      assert restored_count(s3, dir) == 1
    end

    test "leave no empty lease behind", %{s3: s3, dir: dir} do
      {_proxy, cut} = truncating(s3, truncate_puts: 1.0, truncate_puts_matching: "lease.json")
      assert {:error, _} = Engine.open(Path.join(dir, "a.db"), s3: cut)

      {:ok, db} = Engine.open(Path.join(dir, "b.db"), s3: s3)
      :ok = Engine.execute(db, "create table t (x integer)")
      :ok = Engine.close(db)
      assert restored_count(s3, dir) == 0
    end

    test "leave no empty log segment behind", %{s3: s3, dir: dir} do
      {proxy, via} = truncating(s3)
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: via)
      :ok = Engine.execute(db, "create table t (x integer)")

      faults(proxy, truncate_puts: 1.0, truncate_puts_matching: "/log/")
      assert {:error, _} = Engine.execute(db, "insert into t values (1)")
      faults(proxy, [])

      # The next commit takes the same offset.
      :ok = Engine.execute(db, "insert into t values (2)")
      :ok = Engine.close(db)
      assert restored_count(s3, dir) == 1
    end

    test "leave no empty snapshot behind", %{s3: s3, dir: dir} do
      {proxy, via} = truncating(s3)
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: via)
      :ok = Engine.execute(db, "create table t (x integer)")
      :ok = Engine.execute(db, "insert into t values (1)")

      faults(proxy, truncate_puts: 1.0, truncate_puts_matching: "/snapshots/")
      assert {:error, _} = S3.snapshot(db)
      faults(proxy, [])

      :ok = Engine.execute(db, "insert into t values (2)")
      :ok = S3.snapshot(db)
      :ok = Engine.close(db)
      assert restored_count(s3, dir) == 2
    end
  end

  describe "encryption" do
    @enc [cipher: "aegis256", key: String.duplicate("ab", 32)]
    @wrong [cipher: "aegis256", key: String.duplicate("cd", 32)]

    test "an encrypted S3 database round-trips and needs its key", %{s3: s3, dir: dir} do
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3, encryption: @enc)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1, 'secret')")
      :ok = S3.snapshot(db)
      :ok = Engine.execute(db, "INSERT INTO t VALUES (2, 'secret')")
      :ok = Engine.close(db)

      assert {:error, msg} = Engine.open(Path.join(dir, "b.db"), s3: s3, encryption: @wrong)
      assert msg =~ "wrong key"
      assert {:error, _} = Engine.open(Path.join(dir, "c.db"), s3: s3)

      {:ok, restored} = Engine.open(Path.join(dir, "d.db"), s3: s3, encryption: @enc)
      assert count(restored, "t") == 2
      :ok = Engine.close(restored)

      {:ok, _} = S3.restore(Path.join(dir, "copy.db"), s3, encryption: @enc)
      {:ok, copy} = Engine.open(Path.join(dir, "copy.db"), encryption: @enc)
      assert count(copy, "t") == 2
      :ok = Engine.close(copy)
      assert {:error, _} = S3.restore(Path.join(dir, "bad.db"), s3, encryption: @wrong)
    end

    test "opening or restoring without the key says the database is encrypted",
         %{s3: s3, dir: dir} do
      {:ok, db} = Engine.open(Path.join(dir, "a.db"), s3: s3, encryption: @enc)
      :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = S3.snapshot(db)
      # log frames after the snapshot: without a key they don't verify
      :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")
      :ok = Engine.close(db)

      encrypted = "the database at this S3 prefix is encrypted"
      assert {:error, msg} = Engine.open(Path.join(dir, "b.db"), s3: s3)
      assert msg =~ encrypted
      assert {:error, msg} = S3.restore(Path.join(dir, "c.db"), s3)
      assert msg =~ encrypted
      replica = Keyword.put(s3, :mode, :replica)
      assert {:error, msg} = Engine.open(Path.join(dir, "d.db"), s3: replica)
      assert msg =~ encrypted

      {:ok, db} = Engine.open(Path.join(dir, "e.db"), s3: s3, encryption: @enc)
      assert count(db, "t") == 1
      :ok = Engine.close(db)
    end

    test "encrypted replicas", %{s3: s3, dir: dir} do
      {:ok, writer} = Engine.open(Path.join(dir, "w.db"), s3: s3, encryption: @enc)
      :ok = Engine.execute(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
      :ok = Engine.execute(writer, "INSERT INTO t VALUES (1)")
      replica = Keyword.put(s3, :mode, :replica)

      {:ok, r} = Engine.open(Path.join(dir, "r.db"), s3: replica, encryption: @enc)
      assert count(r, "t") == 1
      :ok = Engine.execute(writer, "INSERT INTO t VALUES (2)")
      {:ok, _} = S3.refresh(r)
      assert count(r, "t") == 2
      :ok = Engine.close(r)
      assert {:error, _} = Engine.open(Path.join(dir, "r2.db"), s3: replica, encryption: @wrong)
      :ok = Engine.close(writer)
    end
  end

  # turso names a database's MVCC log after the file without its extension:
  # app.1 and app.2 both use app.db-log. Nothing may write, replay or delete
  # the log of another database file.
  describe "databases whose MVCC log names collide" do
    setup %{dir: dir} do
      {:ok, plain} = Engine.open(Path.join(dir, "app.1"), journal_mode: :mvcc)
      :ok = Engine.execute(plain, "CREATE TABLE t (v); INSERT INTO t VALUES (1)")
      :ok = Engine.close(plain)
      %{log: Path.join(dir, "app.db-log")}
    end

    defp assert_first_intact(dir) do
      {:ok, db} = Engine.open(Path.join(dir, "app.1"))
      assert count(db, "t") == 1
      :ok = Engine.close(db)
    end

    test "a writer open is refused before it touches the other's log", %{
      s3: s3,
      dir: dir,
      log: log
    } do
      bytes = File.read!(log)

      assert {:error, message} =
               Engine.open(Path.join(dir, "app.2"), s3: s3, encryption: false)

      assert message =~ "would share its MVCC log #{log} with #{Path.join(dir, "app.1")}"
      assert File.read!(log) == bytes
      refute File.exists?(Path.join(dir, "app.2"))
      assert_first_intact(dir)
    end

    test "a second writer in this VM can't take an open writer's log", %{
      s3: s3,
      prefix: prefix,
      dir: dir
    } do
      {:ok, a} = Engine.open(Path.join(dir, "w.1"), s3: s3, encryption: false)
      :ok = Engine.execute(a, "CREATE TABLE t (v); INSERT INTO t VALUES (1)")
      other = s3_opts(prefix <> "-b", "writer-b")

      assert {:error, message} =
               Engine.open(Path.join(dir, "w.2"), s3: other, encryption: false)

      assert message =~ "would share its MVCC log #{Path.join(dir, "w.db-log")}"
      :ok = Engine.execute(a, "INSERT INTO t VALUES (2)")
      assert count(a, "t") == 2
      :ok = Engine.close(a)
    end

    test "restore is refused next to a database with the same stem", %{s3: s3, dir: dir} do
      {:ok, db} = Engine.open(Path.join(dir, "src.db"), s3: s3, encryption: false)
      :ok = Engine.execute(db, "CREATE TABLE t (v)")
      :ok = Engine.close(db)
      # A WAL database has no log, so only the shared name gives it away.
      {:ok, wal} = Engine.open(Path.join(dir, "rest.1"), journal_mode: :wal)
      :ok = Engine.execute(wal, "CREATE TABLE t (v)")
      :ok = Engine.close(wal)

      assert {:error, "s3 config: " <> message} =
               S3.restore(Path.join(dir, "rest.2"), s3, encryption: false)

      assert message =~ "would share its MVCC log #{Path.join(dir, "rest.db-log")}"
      refute File.exists?(Path.join(dir, "rest.2"))
      refute File.exists?(Path.join(dir, "rest.db-log"))
    end

    test "a writer restarts with an export of its database next to it", %{s3: s3, dir: dir} do
      path = Path.join(dir, "exported.db")
      {:ok, db} = Engine.open(path, s3: s3, encryption: false)
      :ok = Engine.execute(db, "CREATE TABLE t (v); INSERT INTO t VALUES (1)")
      :ok = Engine.close(db)
      {:ok, _} = Sediment.export_sqlite(path, Path.join(dir, "exported.sqlite"))

      {:ok, db} = Engine.open(path, s3: s3, encryption: false)
      assert count(db, "t") == 1
      :ok = Engine.close(db)
    end

    test "import refuses a source whose log belongs to another file", %{s3: s3, dir: dir} do
      File.cp!(Path.join(dir, "app.1"), Path.join(dir, "app.2"))

      assert {:error, "s3 config: " <> message} = S3.import(Path.join(dir, "app.2"), s3)
      assert message =~ "would share its MVCC log"
      File.rm!(Path.join(dir, "app.2"))
      assert_first_intact(dir)
    end
  end
end
