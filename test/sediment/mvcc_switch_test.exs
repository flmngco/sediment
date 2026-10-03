defmodule Sediment.MvccSwitchTest do
  # turso_core 0.8.1 reuses AUTOINCREMENT ids after an existing database
  # switches to MVCC, silently overwriting rows: the switch is refused.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  defp query(conn, sql) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    :ok = Engine.release(conn, stmt)
    rows
  end

  # A WAL database with rows 1:a and 2:b in an AUTOINCREMENT table.
  defp wal_db(table_sql \\ "create table t (id integer primary key autoincrement, v text)") do
    path = Temp.path!()
    {:ok, conn} = Engine.open(path, journal_mode: :wal)
    :ok = Engine.execute(conn, table_sql)
    :ok = Engine.execute(conn, "insert into t (v) values ('a'), ('b')")
    :ok = Engine.close(conn)
    path
  end

  # The database is still WAL and its next id is 3.
  defp assert_intact(path) do
    {:ok, conn} = Engine.open(path)
    assert [["wal"]] = query(conn, "PRAGMA journal_mode")
    :ok = Engine.execute(conn, "insert into t (v) values ('c')")
    assert [[1, "a"], [2, "b"], [3, "c"]] = query(conn, "select id, v from t order by id")
    :ok = Engine.close(conn)
  end

  describe "with AUTOINCREMENT tables" do
    for mode <- [:mvcc, "mvcc", "experimental_mvcc"] do
      test "opening with journal_mode #{inspect(mode)} is refused" do
        path = wal_db()

        assert {:error,
                "refusing to switch to MVCC: the database has AUTOINCREMENT tables (t)" <> _} =
                 Engine.open(path, journal_mode: unquote(mode))

        assert_intact(path)
      end
    end

    test "every pool connection refuses, and the data is intact" do
      path = wal_db()

      for _ <- 1..3 do
        assert {:error, %Sediment.Error{message: "refusing to switch to MVCC:" <> _}} =
                 Sediment.Connection.connect(database: path, journal_mode: :mvcc)
      end

      assert_intact(path)
    end

    for sql <- [
          "PRAGMA journal_mode = 'mvcc'",
          "pragma main.journal_mode=MVCC",
          "PRAGMA journal_mode(experimental_mvcc)"
        ] do
      test "#{sql} is refused by execute and prepare" do
        path = wal_db()
        {:ok, conn} = Engine.open(path)

        assert {:error, "refusing to switch to MVCC:" <> _} = Engine.execute(conn, unquote(sql))
        assert {:error, "refusing to switch to MVCC:" <> _} = Engine.prepare(conn, unquote(sql))

        assert {:error, "refusing to switch to MVCC:" <> _} =
                 Engine.execute(conn, "select 1; " <> unquote(sql))

        :ok = Engine.close(conn)
        assert_intact(path)
      end
    end

    # The table doesn't exist yet when the script starts, so the
    # check must run before the pragma itself, after the earlier statements.
    test "a script that creates the table, inserts and then switches is refused" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :wal)

      assert {:error,
              "refusing to switch to MVCC: the database has AUTOINCREMENT tables (t)" <> _} =
               Engine.execute(conn, """
               CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
               INSERT INTO t (v) VALUES ('a'), ('b');
               PRAGMA journal_mode = 'mvcc';
               """)

      :ok = Engine.close(conn)
      assert_intact(path)
    end

    # The pragma is prepared before the AUTOINCREMENT table
    # exists and run after it was created and filled.
    test "a pragma prepared before the table exists is refused when it runs" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :wal)
      {:ok, switch} = Engine.prepare(conn, "PRAGMA journal_mode = 'mvcc'")
      {:ok, switch_all} = Engine.prepare(conn, "PRAGMA journal_mode = 'mvcc'")

      :ok =
        Engine.execute(conn, """
        CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
        INSERT INTO t (v) VALUES ('a'), ('b');
        """)

      assert {:error, "refusing to switch to MVCC:" <> _} = Engine.step(conn, switch)
      assert {:error, "refusing to switch to MVCC:" <> _} = Engine.fetch_all(conn, switch_all)
      :ok = Engine.release(conn, switch)
      :ok = Engine.release(conn, switch_all)
      :ok = Engine.close(conn)
      assert_intact(path)
    end

    test "a prepared query run after the table was created is refused through DBConnection" do
      path = Temp.path!()
      {:ok, pool} = Sediment.start_link(database: path, journal_mode: :wal, pool_size: 1)
      {:ok, switch} = Sediment.prepare(pool, "switch", "PRAGMA journal_mode = 'mvcc'")
      Sediment.query!(pool, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
      Sediment.query!(pool, "INSERT INTO t (v) VALUES ('a'), ('b')")

      assert {:error, %Sediment.Error{message: "refusing to switch to MVCC:" <> _}} =
               Sediment.execute(pool, switch, [])

      GenServer.stop(pool)
      assert_intact(path)
    end

    # The switch waits out another connection's lock, and that
    # connection commits an AUTOINCREMENT table meanwhile.
    test "a switch resumed after a busy wait checks again" do
      path = Temp.path!()
      {:ok, b} = Engine.open(path, journal_mode: :wal)
      :ok = Engine.execute(b, "CREATE TABLE x (a)")
      {:ok, a} = Engine.open(path)
      :ok = Engine.set_busy_timeout(a, 5_000)

      :ok = Engine.execute(b, "BEGIN IMMEDIATE")
      :ok = Engine.execute(b, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
      :ok = Engine.execute(b, "INSERT INTO t (v) VALUES ('a'), ('b')")

      switch = Task.async(fn -> Engine.execute(a, "PRAGMA journal_mode = 'mvcc'") end)
      Process.sleep(300)
      :ok = Engine.execute(b, "COMMIT")

      assert {:error,
              "refusing to switch to MVCC: the database has AUTOINCREMENT tables (t)" <> _} =
               Task.await(switch)

      :ok = Engine.close(a)
      :ok = Engine.close(b)
      assert_intact(path)
    end

    test "the same script through DBConnection never switches" do
      # A query runs only its first statement (prepare semantics, like exqlite).
      path = Temp.path!()
      {:ok, pool} = Sediment.start_link(database: path, journal_mode: :wal)

      Sediment.query(pool, """
      CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
      INSERT INTO t (v) VALUES ('a'), ('b');
      PRAGMA journal_mode = 'mvcc';
      """)

      assert %{rows: [["wal"]]} = Sediment.query!(pool, "PRAGMA journal_mode")
      GenServer.stop(pool)
    end

    test "through DBConnection" do
      path = wal_db()
      {:ok, pool} = Sediment.start_link(database: path)

      assert {:error, %Sediment.Error{message: "refusing to switch to MVCC:" <> _}} =
               Sediment.query(pool, "PRAGMA journal_mode = 'mvcc'")

      GenServer.stop(pool)
      assert_intact(path)
    end

    test "names every AUTOINCREMENT table, even an empty one" do
      path = wal_db()
      {:ok, conn} = Engine.open(path)
      :ok = Engine.execute(conn, "create table e (id integer primary key autoincrement)")
      :ok = Engine.close(conn)

      assert {:error,
              "refusing to switch to MVCC: the database has AUTOINCREMENT tables (t, e)" <> _} =
               Engine.open(path, journal_mode: :mvcc)
    end
  end

  describe "allowed" do
    test "a database without AUTOINCREMENT tables switches" do
      path = wal_db("create table t (id integer primary key, v text)")
      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      assert [["mvcc"]] = query(conn, "PRAGMA journal_mode")
      :ok = Engine.close(conn)

      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      :ok = Engine.execute(conn, "insert into t (v) values ('c')")
      assert [[1, "a"], [2, "b"], [3, "c"]] = query(conn, "select id, v from t order by id")
      :ok = Engine.close(conn)
    end

    test "a database created in MVCC mode keeps its AUTOINCREMENT tables" do
      path = Temp.path!()
      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      :ok = Engine.execute(conn, "create table t (id integer primary key autoincrement, v text)")
      :ok = Engine.execute(conn, "insert into t (v) values ('a'), ('b')")
      :ok = Engine.close(conn)

      {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
      :ok = Engine.execute(conn, "PRAGMA journal_mode = 'mvcc'")
      :ok = Engine.execute(conn, "insert into t (v) values ('c')")
      assert [[1, "a"], [2, "b"], [3, "c"]] = query(conn, "select id, v from t order by id")
      :ok = Engine.close(conn)
    end

    test "other pragmas and reading the journal mode" do
      path = wal_db()
      {:ok, conn} = Engine.open(path)
      assert [["wal"]] = query(conn, "PRAGMA journal_mode")
      :ok = Engine.execute(conn, "PRAGMA journal_mode = wal")
      :ok = Engine.close(conn)
    end
  end
end
