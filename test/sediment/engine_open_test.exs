defmodule Sediment.EngineOpenTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  defp query(conn, sql) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    :ok = Engine.release(conn, stmt)
    rows
  end

  describe "journal_mode option" do
    test "opens in wal mode" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :wal)
      assert [["wal"]] = query(conn, "PRAGMA journal_mode")
      Engine.close(conn)
    end

    test "opens in mvcc mode and supports BEGIN CONCURRENT" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      assert [["mvcc"]] = query(conn, "PRAGMA journal_mode")

      :ok = Engine.execute(conn, "create table t (id integer primary key, v text)")
      :ok = Engine.execute(conn, "BEGIN CONCURRENT")
      :ok = Engine.execute(conn, "insert into t (v) values ('a')")
      :ok = Engine.execute(conn, "COMMIT")
      assert [[1, "a"]] = query(conn, "select id, v from t")
      Engine.close(conn)

      {:ok, conn} = Engine.open(path)
      assert [[1, "a"]] = query(conn, "select id, v from t")
      Engine.close(conn)
    end

    test "rejects an unknown journal mode" do
      assert {:error, _} = Engine.open(":memory:", journal_mode: :bogus)
    end
  end

  describe "encryption option" do
    @key "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"

    test "round trips an encrypted database" do
      path = Temp.path!()
      opts = [encryption: [cipher: "aegis256", key: @key]]

      {:ok, conn} = Engine.open(path, opts)
      :ok = Engine.execute(conn, "create table secrets (v text)")
      :ok = Engine.execute(conn, "insert into secrets values ('hidden')")
      Engine.close(conn)

      refute File.read!(path) =~ "hidden"

      {:ok, conn} = Engine.open(path, opts)
      assert [["hidden"]] = query(conn, "select v from secrets")
      Engine.close(conn)
    end

    test "reports a wrong key, a missing key and an unknown cipher" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, encryption: [cipher: "aegis256", key: @key])
      :ok = Engine.execute(conn, "create table secrets (v text)")
      Engine.close(conn)

      wrong_key = String.duplicate("00", 32)

      assert {:error, "Decryption failed for page=1"} =
               Engine.open(path, encryption: [cipher: "aegis256", key: wrong_key])

      assert {:error, "File is not a database"} = Engine.open(path)

      assert {:error, "Unknown cipher name: nope"} =
               Engine.open(Temp.path!(), encryption: [cipher: "nope", key: @key])
    end
  end

  describe "mvcc_checkpoint_threshold option" do
    test "defaults to 256 KiB for MVCC databases" do
      {:ok, conn} = Engine.open(Temp.path!(), journal_mode: :mvcc)
      assert [[262_144]] = query(conn, "PRAGMA mvcc_checkpoint_threshold")
      Engine.close(conn)
    end

    test "applies to MVCC databases reopened without :journal_mode" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      Engine.close(conn)

      {:ok, conn} = Engine.open(path)
      assert [["mvcc"]] = query(conn, "PRAGMA journal_mode")
      assert [[262_144]] = query(conn, "PRAGMA mvcc_checkpoint_threshold")
      Engine.close(conn)
    end

    test "can be set, or left at turso's default with nil" do
      {:ok, conn} =
        Engine.open(Temp.path!(), journal_mode: :mvcc, mvcc_checkpoint_threshold: 1_000)

      assert [[1_000]] = query(conn, "PRAGMA mvcc_checkpoint_threshold")
      Engine.close(conn)

      {:ok, conn} = Engine.open(Temp.path!(), journal_mode: :mvcc, mvcc_checkpoint_threshold: nil)
      assert [[default]] = query(conn, "PRAGMA mvcc_checkpoint_threshold")
      assert default > 262_144
      Engine.close(conn)
    end

    test "is ignored in WAL mode" do
      {:ok, conn} = Engine.open(Temp.path!(), journal_mode: :wal)
      assert [["wal"]] = query(conn, "PRAGMA journal_mode")
      Engine.close(conn)
    end
  end

  describe "experimental option" do
    test "rejects unknown features" do
      assert {:error, "unknown experimental feature: nope"} =
               Engine.open(":memory:", experimental: [:nope])
    end

    test "enables attach" do
      {:ok, conn} = Engine.open(":memory:", experimental: [:attach])
      assert :ok = Engine.execute(conn, "ATTACH DATABASE ':memory:' AS other")
    end
  end

  describe "readonly mode" do
    test "rejects writes even when the file is already open read-write" do
      path = Temp.path!()
      {:ok, rw} = Engine.open(path)
      :ok = Engine.execute(rw, "create table t (x integer)")

      {:ok, ro} = Engine.open(path, mode: :readonly)
      assert [] = query(ro, "select x from t")

      assert {:error, "attempt to write a readonly database"} =
               Engine.execute(ro, "insert into t values (1)")

      :ok = Engine.execute(rw, "insert into t values (1)")
    end
  end

  describe "serialize/deserialize" do
    test "round trips an in-memory database" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table t (x integer); insert into t values (1), (2)")
      {:ok, image} = Engine.serialize(conn)
      assert byte_size(image) > 0

      {:ok, copy} = Engine.open(":memory:")
      :ok = Engine.deserialize(copy, image)
      assert [[1], [2]] = query(copy, "select x from t order by x")
    end

    test "rejects garbage" do
      {:ok, conn} = Engine.open(":memory:")
      assert {:error, "file is not a database"} = Engine.deserialize(conn, "garbage")
    end
  end

  test "an invalid encryption key's error doesn't quote the key" do
    for key <- ["SECRET-VALUE", "abc"] do
      assert {:error, message} =
               Engine.open(Temp.path!(), encryption: [cipher: "aegis256", key: key])

      assert message =~ "must be hex encoded"
      refute message =~ "S"
    end
  end

  describe "s3 option errors" do
    # Rejected while decoding the options, before any network access.
    test "name the option without echoing a credential's value" do
      s3 = [bucket: "b", region: "us-east-1", access_key_id: "AKIA"]

      for secret <- [~c"SECRET-VALUE", {:system, "SECRET-VALUE"}] do
        assert {:error, message} =
                 Engine.open(Temp.path!(), s3: Keyword.put(s3, :secret_access_key, secret))

        assert message =~ "secret_access_key"
        refute message =~ "SECRET-VALUE"
      end
    end
  end
end
