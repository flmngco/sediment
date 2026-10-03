defmodule Sediment.EngineTest do
  use ExUnit.Case

  alias Sediment.Engine
  doctest Sediment.Engine

  describe ".open/1" do
    test "opens a database in memory" do
      {:ok, conn} = Engine.open(":memory:")

      assert conn
    end

    test "opens a database on disk" do
      {:ok, path} = Temp.path()
      {:ok, conn} = Engine.open(path)

      assert conn

      File.rm(path)
    end

    test "creates database path on disk when non-existent" do
      {:ok, path} = Temp.mkdir()
      {:ok, conn} = Engine.open(path <> "/non_exist.db")

      assert conn

      File.rm(path)
    end

    test "opens a database in readonly mode" do
      # Create database with readwrite connection
      {:ok, path} = Temp.path()
      {:ok, rw_conn} = Engine.open(path)

      create_table_query = "create table test (id integer primary key, stuff text)"
      :ok = Engine.execute(rw_conn, create_table_query)

      insert_value_query = "insert into test (stuff) values ('This is a test')"
      :ok = Engine.execute(rw_conn, insert_value_query)

      # Read from database with a readonly connection
      {:ok, ro_conn} = Engine.open(path, mode: :readonly)

      select_query = "select id, stuff from test order by id asc"
      {:ok, statement} = Engine.prepare(ro_conn, select_query)
      {:row, columns} = Engine.step(ro_conn, statement)

      assert [1, "This is a test"] == columns

      # Readonly connection cannot insert
      assert {:error, "attempt to write a readonly database"} ==
               Engine.execute(ro_conn, insert_value_query)
    end

    test "opens a database in a list of mode" do
      # Create database with readwrite connection
      {:ok, path} = Temp.path()
      {:ok, rw_conn} = Engine.open(path)

      create_table_query = "create table test (id integer primary key, stuff text)"
      :ok = Engine.execute(rw_conn, create_table_query)

      insert_value_query = "insert into test (stuff) values ('This is a test')"
      :ok = Engine.execute(rw_conn, insert_value_query)

      # Read from database with a readonly connection
      {:ok, ro_conn} = Engine.open(path, mode: [:readonly, :nomutex])

      select_query = "select id, stuff from test order by id asc"
      {:ok, statement} = Engine.prepare(ro_conn, select_query)
      {:row, columns} = Engine.step(ro_conn, statement)

      assert [1, "This is a test"] == columns
    end

    test "opens a database with invalid mode" do
      {:ok, path} = Temp.path()

      msg =
        "expected mode to be `:readwrite`, `:readonly` or list of modes, but received :notarealmode"

      assert_raise ArgumentError, msg, fn ->
        Engine.open(path, mode: :notarealmode)
      end
    end

    test "opens a database with invalid single nomutex mode" do
      {:ok, path} = Temp.path()

      msg =
        "expected mode to be `:readwrite` or `:readonly`, can't use a single :nomutex mode"

      assert_raise ArgumentError, msg, fn ->
        Engine.open(path, mode: :nomutex)
      end
    end

    test "opens a database with invalid list of mode" do
      {:ok, path} = Temp.path()

      msg =
        "expected mode to be `:readwrite`, `:readonly`, `:nomutex` or `:create`, but received :notarealmode"

      assert_raise ArgumentError, msg, fn ->
        Engine.open(path, mode: [:notarealmode])
      end
    end

    test "opens with default create but explicit readwrite does not create" do
      {:ok, path} = Temp.path()

      # Pure readwrite modes should not create the file.
      assert {:error, _reason} = Engine.open(path, mode: :readwrite)
      assert {:error, _reason} = Engine.open(path, mode: [:readwrite])

      # The default (and [:readwrite, :create]) still creates.
      {:ok, conn} = Engine.open(path)
      :ok = Engine.close(conn)
      assert File.exists?(path)
      File.rm!(path)

      # Explicit list with create also works
      {:ok, path2} = Temp.path()
      {:ok, conn2} = Engine.open(path2, mode: [:readwrite, :create])
      :ok = Engine.close(conn2)
      assert File.exists?(path2)
      File.rm!(path2)
    end
  end

  describe ".close/2" do
    test "closes a database in memory" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.close(conn)
    end

    test "closing a database multiple times works properly" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.close(conn)
      :ok = Engine.close(conn)
    end
  end

  describe ".execute/2" do
    test "creates a table" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      :ok = Engine.execute(conn, "insert into test (stuff) values ('This is a test')")
      {:ok, 1} = Engine.last_insert_rowid(conn)
      {:ok, 1} = Engine.changes(conn)
      :ok = Engine.close(conn)
    end

    test "handles incorrect syntax" do
      {:ok, conn} = Engine.open(":memory:")

      {:error, "unexpected token: a"} =
        Engine.execute(
          conn,
          "create a dumb table test (id integer primary key, stuff text)"
        )

      {:ok, 0} = Engine.changes(conn)
      :ok = Engine.close(conn)
    end

    test "sqlite fts virtual tables are not available" do
      {:ok, conn} = Engine.open(":memory:")

      assert {:error, "no such module: fts5"} =
               Engine.execute(conn, "create virtual table things using fts5(content)")
    end

    test "handles unicode characters" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Sediment.Engine.execute(
          conn,
          "create table test (id integer primary key, stuff text)"
        )

      :ok = Sediment.Engine.execute(conn, "insert into test (stuff) values ('😝')")
    end
  end

  describe ".prepare/3" do
    test "preparing a valid sql statement" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")

      assert statement
    end

    test "supports utf8 in error messages" do
      {:ok, conn} = Engine.open(":memory:")
      assert {:error, "no such table: 🌍"} = Engine.prepare(conn, "select * from 🌍")
    end
  end

  describe ".release/2" do
    test "double releasing a statement" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.release(conn, statement)
      :ok = Engine.release(conn, statement)
    end

    test "releasing a statement" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.release(conn, statement)
    end

    test "releasing a statement through another connection raises" do
      {:ok, conn_a} = Engine.open(":memory:")
      {:ok, conn_b} = Engine.open(":memory:")
      {:ok, statement} = Engine.prepare(conn_a, "select 123")

      assert_raise ArgumentError,
                   "Statement was prepared for a different connection, which is illegal",
                   fn -> Engine.release(conn_b, statement) end

      assert {:row, [123]} = Engine.step(conn_a, statement)
    end

    test "releasing a nil statement" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.release(conn, nil)
    end
  end

  describe ".bind" do
    test "binding values to a valid sql statement" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.bind(statement, ["testing"])
    end

    test "trying to bind with incorrect amount of arguments" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")

      assert_raise ArgumentError, "expected 1 arguments, got 0", fn ->
        Engine.bind(statement, [])
      end
    end

    test "binds datetime value as string" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.bind(statement, [DateTime.utc_now()])
    end

    test "binds date value as string" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.bind(statement, [Date.utc_today()])
    end

    test "raises an error when binding non UTC datetimes" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")

      msg = "#DateTime<2021-08-25 13:23:25+00:00 UTC Europe/Berlin> is not in UTC"

      assert_raise ArgumentError, msg, fn ->
        {:ok, dt} = DateTime.from_naive(~N[2021-08-25 13:23:25], "Etc/UTC")
        # Sneak in other timezone without a tz database
        other_tz = struct(dt, time_zone: "Europe/Berlin")

        Engine.bind(statement, [other_tz])
      end
    end

    test "accepts arguments for parameters turso folded away" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table t (id integer, x integer)")
      :ok = Engine.execute(conn, "insert into t values (1, 10)")

      {:ok, statement} = Engine.prepare(conn, "select id from t where 0 and x = ?")
      assert :ok = Engine.bind(statement, [10])
      assert :done = Engine.step(conn, statement)

      {:ok, statement} = Engine.prepare(conn, "select id from t where (0) and (x in (?, ?))")
      assert :ok = Engine.bind(statement, [10, 11])
      assert :done = Engine.step(conn, statement)

      {:ok, statement} = Engine.prepare(conn, "select id, ? from t where 0 and x = ?")
      assert :ok = Engine.bind(statement, ["a", 10])
      assert :done = Engine.step(conn, statement)

      # a genuine mismatch still raises
      {:ok, folded} = Engine.prepare(conn, "select id from t where 0 and x = ?")

      assert_raise ArgumentError, "expected 0 arguments, got 2", fn ->
        Engine.bind(folded, [1, 2])
      end
    end

    test "binds named parameters" do
      {:ok, conn} = Engine.open(":memory:")

      {:ok, statement} =
        Engine.prepare(conn, "select :answer, @pi, :name, $👋, :blob, :null")

      :ok =
        Engine.bind(statement, %{
          ":answer" => 42,
          "@pi" => 3.14,
          :":name" => "Alice",
          "$👋" => "👋",
          ":blob" => {:blob, <<0, 1, 2>>},
          ~c":null" => nil
        })

      assert {:row, [42, 3.14, "Alice", "👋", <<0, 1, 2>>, nil]} =
               Engine.step(conn, statement)
    end

    test "handles repeating named parameters" do
      {:ok, conn} = Engine.open(":memory:")

      {:ok, statement} =
        Engine.prepare(conn, "select :name, :name, :name")

      :ok =
        Engine.bind(statement, %{
          ":name" => "Alice"
        })

      assert {:row, ["Alice", "Alice", "Alice"]} = Engine.step(conn, statement)
    end

    test "raises an error when too few or too many named parameters" do
      {:ok, conn} = Engine.open(":memory:")

      {:ok, statement} =
        Engine.prepare(conn, "select :name, :age")

      assert_raise ArgumentError, ~r"expected 2 named arguments, got 1", fn ->
        Engine.bind(statement, %{":name" => "Alice"})
      end

      assert_raise ArgumentError, ~r"expected 2 named arguments, got 3", fn ->
        Engine.bind(statement, %{":name" => "Alice", ":age" => 30, ":extra" => "value"})
      end
    end
  end

  describe ".bind_text/3" do
    setup do
      {:ok, conn} = Engine.open(":memory:", [:readonly])
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      {:ok, conn: conn, stmt: stmt}
    end

    test "binds text value", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_text(stmt, 1, "hello")
      assert {:row, ["hello"]} = Engine.step(conn, stmt)
    end

    test "binds emojis", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_text(stmt, 1, "hello 👋 world 🌏")
      assert {:row, ["hello 👋 world 🌏"]} = Engine.step(conn, stmt)
    end

    test "errors on invalid statement" do
      assert_raise ArgumentError, "argument error: nil", fn ->
        Engine.bind_text(_not_stmt = nil, 1, "hello")
      end
    end

    test "errors on invalid index", %{stmt: stmt} do
      assert_raise Sediment.Error, "column index out of range", fn ->
        Engine.bind_text(stmt, _out_of_range = 2, "hello")
      end
    end

    test "errors on invalid text argument", %{stmt: stmt} do
      assert_raise ArgumentError, "argument error: 1", fn ->
        Engine.bind_text(stmt, 1, _not_text = 1)
      end
    end
  end

  describe ".bind_blob/3" do
    setup do
      {:ok, conn} = Engine.open(":memory:", [:readonly])
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      {:ok, conn: conn, stmt: stmt}
    end

    test "binds binary value", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_blob(stmt, 1, <<0, 0, 0>>)
      assert {:row, [<<0, 0, 0>>]} = Engine.step(conn, stmt)
    end

    test "errors on invalid statement" do
      assert_raise ArgumentError, "argument error: nil", fn ->
        Engine.bind_blob(_not_stmt = nil, 1, "hello")
      end
    end

    test "errors on invalid index", %{stmt: stmt} do
      assert_raise Sediment.Error, "column index out of range", fn ->
        Engine.bind_blob(stmt, _out_of_range = 2, "hello")
      end
    end

    test "errors on invalid blob argument", %{stmt: stmt} do
      assert_raise ArgumentError, "argument error: 1", fn ->
        Engine.bind_blob(stmt, 1, _not_binary = 1)
      end
    end
  end

  describe ".bind_integer/3" do
    setup do
      {:ok, conn} = Engine.open(":memory:", [:readonly])
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      {:ok, conn: conn, stmt: stmt}
    end

    test "binds integer value", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_integer(stmt, 1, 42)
      assert {:row, [42]} = Engine.step(conn, stmt)
    end

    test "binds the full 64-bit integer range", %{conn: conn, stmt: stmt} do
      for i <- [9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        assert :ok = Engine.bind_integer(stmt, 1, i)
        assert {:row, [^i]} = Engine.step(conn, stmt)
      end
    end

    test "rejects integers outside the 64-bit range", %{stmt: stmt} do
      assert_raise Sediment.Error, "integer out of range", fn ->
        Engine.bind_integer(stmt, 1, 9_223_372_036_854_775_808)
      end
    end

    test "binds integers larger than INT32_MAX", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_integer(stmt, 1, 0xFFFFFFFF + 1)
      assert {:row, [0x100000000]} = Engine.step(conn, stmt)
    end

    test "errors on invalid statement" do
      assert_raise ArgumentError, "argument error: nil", fn ->
        Engine.bind_integer(_not_stmt = nil, 1, 42)
      end
    end

    test "errors on invalid index", %{stmt: stmt} do
      assert_raise Sediment.Error, "column index out of range", fn ->
        Engine.bind_integer(stmt, _out_of_range = 2, 42)
      end
    end

    test "errors on invalid blob argument", %{stmt: stmt} do
      assert_raise ArgumentError, "argument error: \"42\"", fn ->
        Engine.bind_integer(stmt, 1, _not_integer = "42")
      end
    end
  end

  describe ".bind_float/3" do
    setup do
      {:ok, conn} = Engine.open(":memory:", [:readonly])
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      {:ok, conn: conn, stmt: stmt}
    end

    test "binds float value", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_float(stmt, 1, 3.14)
      assert {:row, [3.14]} = Engine.step(conn, stmt)
    end

    test "errors on invalid statement" do
      assert_raise ArgumentError, "argument error: nil", fn ->
        Engine.bind_float(_not_stmt = nil, 1, 3.14)
      end
    end

    test "errors on invalid index", %{stmt: stmt} do
      assert_raise Sediment.Error, "column index out of range", fn ->
        Engine.bind_float(stmt, _out_of_range = 2, 3.14)
      end
    end

    test "errors on invalid blob argument", %{stmt: stmt} do
      assert_raise ArgumentError, "argument error: \"3.14\"", fn ->
        Engine.bind_float(stmt, 1, _not_float = "3.14")
      end
    end
  end

  describe "value round trips" do
    setup do
      {:ok, conn} = Engine.open(":memory:")
      [conn: conn]
    end

    defp select(conn, sql, args) do
      {:ok, stmt} = Engine.prepare(conn, sql)
      :ok = Engine.bind(stmt, args)
      Engine.step(conn, stmt)
    end

    test "empty text and blobs", %{conn: conn} do
      assert {:row, ["", "text"]} = select(conn, "select ?, typeof(?1)", [""])
      assert {:row, ["", "blob"]} = select(conn, "select ?, typeof(?1)", [{:blob, ""}])
    end

    test "binaries that are not valid UTF-8 are stored as blobs", %{conn: conn} do
      assert {:row, [<<255, 0, 1>>, "blob"]} =
               select(conn, "select ?, typeof(?1)", [<<255, 0, 1>>])
    end

    test "large blobs", %{conn: conn} do
      blob = :crypto.strong_rand_bytes(5_000_000)
      assert {:row, [^blob]} = select(conn, "select ?", [{:blob, blob}])
    end

    test "booleans bind as text like exqlite", %{conn: conn} do
      assert {:row, ["true", "false"]} = select(conn, "select ?, ?", [true, false])
    end

    test "infinite floats decode as :inf and :\"-inf\"", %{conn: conn} do
      assert {:row, [:inf, :"-inf"]} = select(conn, "select 1e999, -1e999", [])
    end
  end

  describe ".bind_null/2" do
    setup do
      {:ok, conn} = Engine.open(":memory:", [:readonly])
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      {:ok, conn: conn, stmt: stmt}
    end

    test "binds null value", %{conn: conn, stmt: stmt} do
      assert :ok = Engine.bind_null(stmt, 1)
      assert {:row, [nil]} = Engine.step(conn, stmt)
    end

    test "errors on invalid statement" do
      assert_raise ArgumentError, "argument error: nil", fn ->
        Engine.bind_null(_not_stmt = nil, 1)
      end
    end

    test "errors on invalid index", %{stmt: stmt} do
      assert_raise Sediment.Error, "column index out of range", fn ->
        Engine.bind_null(stmt, _out_of_range = 2)
      end
    end
  end

  describe ".columns/2" do
    test "returns the column definitions" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "select id, stuff from test")

      {:ok, columns} = Engine.columns(conn, statement)

      assert ["id", "stuff"] == columns
    end

    test "supports utf8 column names" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table test(👋 text, ✍️ text)")
      {:ok, statement} = Engine.prepare(conn, "select * from test")
      assert {:ok, ["👋", "✍️"]} = Engine.columns(conn, statement)
    end

    test "raises exception when statement was prepared for another connection" do
      {:ok, connection_a} = Engine.open(":memory:")
      {:ok, connection_b} = Engine.open(":memory:")

      {:ok, statement_b} = Engine.prepare(connection_b, "select 'connection b'")

      assert_raise(
        ArgumentError,
        "Statement was prepared for a different connection, which is illegal",
        fn ->
          Engine.columns(connection_a, statement_b)
        end
      )
    end
  end

  describe ".step/2" do
    test "returns results" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      :ok = Engine.execute(conn, "insert into test (stuff) values ('This is a test')")
      {:ok, 1} = Engine.last_insert_rowid(conn)
      :ok = Engine.execute(conn, "insert into test (stuff) values ('Another test')")
      {:ok, 2} = Engine.last_insert_rowid(conn)

      {:ok, statement} =
        Engine.prepare(conn, "select id, stuff from test order by id asc")

      {:row, columns} = Engine.step(conn, statement)
      assert [1, "This is a test"] == columns
      {:row, columns} = Engine.step(conn, statement)
      assert [2, "Another test"] == columns
      assert :done = Engine.step(conn, statement)

      {:row, columns} = Engine.step(conn, statement)
      assert [1, "This is a test"] == columns
      {:row, columns} = Engine.step(conn, statement)
      assert [2, "Another test"] == columns
      assert :done = Engine.step(conn, statement)
    end

    test "returns no results" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "select id, stuff from test")
      assert :done = Engine.step(conn, statement)
    end

    test "works with insert" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.bind(statement, ["this is a test"])
      assert :done == Engine.step(conn, statement)
    end

    test "bind raises an exception" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")

      assert_raise ArgumentError,
                   "unsupported type: %ArgumentError{message: \"argument error\"}",
                   fn -> Engine.bind(statement, [%ArgumentError{}]) end
    end

    test "raises exception when statement was prepared for another connection" do
      {:ok, connection_a} = Engine.open(":memory:")
      {:ok, connection_b} = Engine.open(":memory:")

      {:ok, statement_b} = Engine.prepare(connection_b, "select 'connection b'")

      assert_raise(
        ArgumentError,
        "Statement was prepared for a different connection, which is illegal",
        fn ->
          Engine.step(connection_a, statement_b)
        end
      )
    end
  end

  describe ".multi_step/3" do
    test "returns results" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      :ok = Engine.execute(conn, "insert into test (stuff) values ('one')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('two')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('three')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('four')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('five')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('six')")

      {:ok, statement} =
        Engine.prepare(conn, "select id, stuff from test order by id asc")

      {:rows, rows} = Engine.multi_step(conn, statement, 4)
      assert rows == [[1, "one"], [2, "two"], [3, "three"], [4, "four"]]

      {:done, rows} = Engine.multi_step(conn, statement, 4)
      assert rows == [[5, "five"], [6, "six"]]
    end

    test "raises exception when statement was prepared for another connection" do
      {:ok, connection_a} = Engine.open(":memory:")
      {:ok, connection_b} = Engine.open(":memory:")

      {:ok, statement_b} = Engine.prepare(connection_b, "select 'connection b'")

      assert_raise(
        ArgumentError,
        "Statement was prepared for a different connection, which is illegal",
        fn ->
          Engine.multi_step(connection_a, statement_b)
        end
      )
    end
  end

  describe ".multi_step/2" do
    test "returns results" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      :ok = Engine.execute(conn, "insert into test (stuff) values ('one')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('two')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('three')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('four')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('five')")
      :ok = Engine.execute(conn, "insert into test (stuff) values ('six')")

      {:ok, statement} =
        Engine.prepare(conn, "select id, stuff from test order by id asc")

      {:done, rows} = Engine.multi_step(conn, statement)

      assert rows == [
               [1, "one"],
               [2, "two"],
               [3, "three"],
               [4, "four"],
               [5, "five"],
               [6, "six"]
             ]
    end
  end

  describe "working with prepared statements after close" do
    test "returns proper error" do
      {:ok, conn} = Engine.open(":memory:")

      :ok =
        Engine.execute(conn, "create table test (id integer primary key, stuff text)")

      {:ok, statement} = Engine.prepare(conn, "insert into test (stuff) values (?1)")
      :ok = Engine.close(conn)

      # closing finalizes the connection's statements
      assert {:error, :invalid_statement} = Engine.bind(statement, ["this is a test"])

      assert {:error, :connection_closed} =
               Engine.execute(
                 conn,
                 "create table test (id integer primary key, stuff text)"
               )

      assert {:error, :connection_closed} == Engine.step(conn, statement)
    end
  end

  describe "serialize and deserialize" do
    test "serialize a database to binary and deserialize to new database" do
      {:ok, path} = Temp.path()
      {:ok, conn} = Engine.open(path)

      :ok =
        Engine.execute(conn, "create table test(id integer primary key, stuff text)")

      assert {:ok, binary} = Engine.serialize(conn, "main")
      assert is_binary(binary)
      Engine.close(conn)
      File.rm(path)

      {:ok, conn} = Engine.open(":memory:")
      assert :ok = Engine.deserialize(conn, "main", binary)

      assert :ok =
               Engine.execute(conn, "insert into test(id, stuff) values (1, 'hello')")

      assert {:ok, statement} = Engine.prepare(conn, "select id, stuff from test")
      assert {:row, [1, "hello"]} = Engine.step(conn, statement)
    end
  end

  describe "unsupported hooks" do
    setup do
      {:ok, conn} = Engine.open(":memory:")
      on_exit(fn -> Engine.close(conn) end)
      [conn: conn]
    end

    test "set_update_hook/2 is not supported", %{conn: conn} do
      assert {:error, :not_supported} = Engine.set_update_hook(conn, self())
    end

    test "set_authorizer/2 is not supported", %{conn: conn} do
      assert {:error, :not_supported} = Engine.set_authorizer(conn, [:attach])
    end

    test "set_log_hook/1 is not supported" do
      assert {:error, :not_supported} = Engine.set_log_hook(self())
    end

    test "enable_load_extension/2 is not supported", %{conn: conn} do
      assert {:error, :not_supported} = Engine.enable_load_extension(conn, true)
    end
  end

  describe ".interrupt/1" do
    test "double interrupting a connection" do
      {:ok, conn} = Engine.open(":memory:")

      :ok = Engine.interrupt(conn)
      :ok = Engine.interrupt(conn)
    end

    test "interrupting a nil connection" do
      :ok = Engine.interrupt(nil)
    end

    test "interrupting a long running query and able to close a connection" do
      {:ok, conn} = Engine.open(":memory:")

      spawn(fn ->
        :ok =
          Engine.execute(
            conn,
            "WITH RECURSIVE r(i) AS ( VALUES(0) UNION ALL SELECT i FROM r LIMIT 1000000000 ) SELECT i FROM r WHERE i = 1;"
          )
      end)

      Process.sleep(100)
      :ok = Engine.interrupt(conn)
      Process.sleep(100)
      :ok = Engine.close(conn)
    end

    test "concurrent interrupt and close does not segfault" do
      for _ <- 1..500 do
        {:ok, conn} = Engine.open(":memory:")
        task = Task.async(fn -> Engine.interrupt(conn) end)
        Engine.close(conn)
        Task.await(task, 1000)
      end
    end

    test "concurrent cancel and close does not segfault" do
      for _ <- 1..500 do
        {:ok, conn} = Engine.open(":memory:")
        task = Task.async(fn -> Engine.cancel(conn) end)
        Engine.close(conn)
        Task.await(task, 1000)
      end
    end

    test "concurrent double close does not segfault" do
      for _ <- 1..200 do
        {:ok, conn} = Engine.open(":memory:")
        parent = self()
        spawn(fn -> send(parent, {:a, Engine.close(conn)}) end)
        spawn(fn -> send(parent, {:b, Engine.close(conn)}) end)
        assert_receive {:a, :ok}, 1000
        assert_receive {:b, :ok}, 1000
      end
    end

    test "last_insert_rowid after close does not segfault" do
      for _ <- 1..100 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.execute(conn, "create table t (id integer primary key)")
        :ok = Engine.execute(conn, "insert into t values (1)")
        {:ok, 1} = Engine.last_insert_rowid(conn)
        :ok = Engine.close(conn)
        assert {:error, :connection_closed} = Engine.last_insert_rowid(conn)
      end
    end

    test "concurrent close and transaction_status does not segfault" do
      for _ <- 1..500 do
        {:ok, conn} = Engine.open(":memory:")
        parent = self()
        spawn(fn -> send(parent, {:a, Engine.close(conn)}) end)
        spawn(fn -> send(parent, {:b, Engine.transaction_status(conn)}) end)
        assert_receive {:a, :ok}, 1000
        assert_receive {:b, _}, 1000
      end
    end

    test "concurrent close and changes does not segfault" do
      for _ <- 1..500 do
        {:ok, conn} = Engine.open(":memory:")
        parent = self()
        spawn(fn -> send(parent, {:a, Engine.close(conn)}) end)
        spawn(fn -> send(parent, {:b, Engine.changes(conn)}) end)
        assert_receive {:a, :ok}, 1000
        assert_receive {:b, _}, 1000
      end
    end

    test "serialize after close does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.close(conn)
        assert {:error, _} = Engine.serialize(conn, "main")
      end
    end

    test "enable_load_extension after close does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.close(conn)
        assert {:error, _} = Engine.enable_load_extension(conn, false)
      end
    end

    test "deserialize after close does not segfault" do
      {:ok, src} = Engine.open(":memory:")
      {:ok, data} = Engine.serialize(src, "main")
      :ok = Engine.close(src)

      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.close(conn)
        assert {:error, _} = Engine.deserialize(conn, "main", data)
      end
    end

    test "deserialize malloc failure releases the connection lock" do
      {:ok, conn} = Engine.open(":memory:")

      assert {:error, _} = Engine.deserialize(conn, "main", <<>>)

      task = Task.async(fn -> Engine.close(conn) end)
      result = Task.yield(task, 500)
      Task.shutdown(task, :brutal_kill)

      assert {:ok, :ok} = result,
             "close deadlocked after failed deserialize (lock not released)"
    end

    test "set_update_hook after close does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.close(conn)
        assert {:error, _} = Engine.set_update_hook(conn, self())
      end
    end

    test "bind after release returns error" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        {:ok, stmt} = Engine.prepare(conn, "select ?")
        :ok = Engine.release(conn, stmt)
        assert {:error, :invalid_statement} = Engine.bind(stmt, [42])
        :ok = Engine.close(conn)
      end
    end
  end

  describe "a panic inside turso" do
    test "becomes an error and closes the connection" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table t (x)")
      {:ok, stmt} = Engine.prepare(conn, "select * from t")

      :ok = Sediment.Native.debug_panic_next_step(conn)
      assert {:error, "internal turso error: panic requested" <> _} = Engine.step(conn, stmt)
      assert {:error, :connection_closed} = Engine.execute(conn, "select 1")
      assert :ok = Engine.close(conn)
    end

    test "in prepare or bind is contained too" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Sediment.Native.debug_panic_next_step(conn)
      assert {:error, "internal turso error:" <> _} = Engine.prepare(conn, "select 1")
      assert {:error, :connection_closed} = Engine.prepare(conn, "select 1")

      {:ok, conn} = Engine.open(":memory:")
      {:ok, stmt} = Engine.prepare(conn, "select ?")
      :ok = Sediment.Native.debug_panic_next_step(conn)

      assert_raise Sediment.Error, ~r/internal turso error/, fn ->
        Engine.bind(stmt, [1])
      end

      assert {:error, :connection_closed} = Engine.step(conn, stmt)
      assert :ok = Engine.close(conn)
    end

    test "makes Sediment.Connection disconnect" do
      {:ok, state} = Sediment.Connection.connect(database: :memory)
      :ok = Sediment.Native.debug_panic_next_step(state.db)

      assert {:disconnect, %Sediment.Error{message: "internal turso error:" <> _}, _state} =
               Sediment.Connection.handle_execute(
                 %Sediment.Query{statement: "select 1"},
                 [],
                 [],
                 state
               )
    end
  end

  test "a panic mid-stream disconnects, and cleanup callbacks still succeed" do
    alias Sediment.Connection

    {:ok, state} = Connection.connect(database: :memory)
    {:ok, _, state} = Connection.handle_begin([], state)

    {:ok, query, cursor, state} =
      Connection.handle_declare(
        %Sediment.Query{statement: "select value from generate_series(1, 100)"},
        [],
        [],
        state
      )

    assert {:cont, %{num_rows: 10}, state} =
             Connection.handle_fetch(query, cursor, [max_rows: 10], state)

    :ok = Sediment.Native.debug_panic_next_step(state.db)

    assert {:disconnect, %Sediment.Error{message: "internal turso error:" <> _}, state} =
             Connection.handle_fetch(query, cursor, [max_rows: 10], state)

    assert {:ok, _, state} = Connection.handle_deallocate(query, cursor, [], state)
    assert :ok = Connection.disconnect(%Sediment.Error{message: "test"}, state)
  end

  describe "statement garbage collection" do
    test "does not block while another process runs a long query" do
      {:ok, conn} = Engine.open(":memory:")
      parent = self()

      holder =
        spawn(fn ->
          for _ <- 1..10, do: {:ok, _} = Engine.prepare(conn, "select 1")
          send(parent, :prepared)

          receive do
            :collect ->
              {micros, _} = :timer.tc(fn -> :erlang.garbage_collect() end)
              send(parent, {:collected, micros})
          end
        end)

      assert_receive :prepared

      spawn(fn ->
        result =
          Engine.execute(
            conn,
            "WITH RECURSIVE r(i) AS (VALUES(0) UNION ALL SELECT i FROM r LIMIT 1000000000) SELECT i FROM r WHERE i = 1"
          )

        send(parent, {:query_done, result})
      end)

      Process.sleep(100)
      send(holder, :collect)
      assert_receive {:collected, micros}, 2_000
      assert micros < 50_000

      :ok = Engine.interrupt(conn)
      assert_receive {:query_done, {:error, "interrupted"}}, 5_000
      assert :ok = Engine.execute(conn, "select 1")
    end

    test "connections that are never closed are closed after garbage collection" do
      path = Temp.path!()

      Task.await(
        Task.async(fn ->
          {:ok, conn} = Engine.open(path)
          :ok = Engine.execute(conn, "create table t (x integer); insert into t values (42)")
          {:ok, _stmt} = Engine.prepare(conn, "select x from t")
          :ok
        end)
      )

      :erlang.garbage_collect()
      Process.sleep(50)

      {:ok, conn} = Engine.open(path)
      {:ok, stmt} = Engine.prepare(conn, "select x from t")
      assert {:row, [42]} = Engine.step(conn, stmt)
    end

    test "finalizes statements dropped while the connection was busy" do
      {:ok, conn} = Engine.open(":memory:")

      for _ <- 1..200 do
        {:ok, _stmt} = Engine.prepare(conn, "select 1")
      end

      :erlang.garbage_collect()
      assert :ok = Engine.execute(conn, "select 1")
      assert :ok = Engine.close(conn)
    end
  end

  describe "prepare racing deserialize" do
    test "a statement is finalized or prepared on the new database, never stale" do
      {:ok, src} = Engine.open(":memory:")
      :ok = Engine.execute(src, "create table t(x); insert into t values (1),(2),(3)")
      {:ok, image} = Engine.serialize(src)

      for _ <- 1..300 do
        {:ok, db} = Engine.open(":memory:")
        :ok = Engine.deserialize(db, image)
        parent = self()

        task =
          Task.async(fn ->
            send(parent, :go)
            Engine.prepare(db, "select count(*) from t")
          end)

        receive do: (:go -> :ok)
        :ok = Engine.deserialize(db, image)
        {:ok, stmt} = Task.await(task)
        assert Engine.step(db, stmt) in [{:error, :invalid_statement}, {:row, [3]}]
        Engine.close(db)
      end
    end
  end

  describe ".step, .columns, .multi_step, .reset, .bind_* after release" do
    test "step after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.execute(conn, "create table t (x integer)")
        {:ok, stmt} = Engine.prepare(conn, "select * from t")
        :ok = Engine.release(conn, stmt)
        assert {:error, _} = Engine.step(conn, stmt)
        :ok = Engine.close(conn)
      end
    end

    test "columns after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.execute(conn, "create table t (x integer)")
        {:ok, stmt} = Engine.prepare(conn, "select * from t")
        :ok = Engine.release(conn, stmt)
        assert {:error, _} = Engine.columns(conn, stmt)
        :ok = Engine.close(conn)
      end
    end

    test "multi_step after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.execute(conn, "create table t (x integer)")
        {:ok, stmt} = Engine.prepare(conn, "select * from t")
        :ok = Engine.release(conn, stmt)
        assert {:error, _} = Engine.multi_step(conn, stmt, 10)
        :ok = Engine.close(conn)
      end
    end

    test "reset after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        :ok = Engine.execute(conn, "create table t (x integer)")
        {:ok, stmt} = Engine.prepare(conn, "select * from t")
        :ok = Engine.release(conn, stmt)
        assert {:error, _} = Engine.reset(stmt)
        :ok = Engine.close(conn)
      end
    end

    test "bind_parameter_count after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        {:ok, stmt} = Engine.prepare(conn, "select ?")
        :ok = Engine.release(conn, stmt)
        assert {:error, _} = Engine.bind_parameter_count(stmt)
        :ok = Engine.close(conn)
      end
    end

    test "bind_text after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        {:ok, stmt} = Engine.prepare(conn, "select ?")
        :ok = Engine.release(conn, stmt)

        try do
          Engine.bind_text(stmt, 1, "hello")
        rescue
          _ -> :ok
        end

        :ok = Engine.close(conn)
      end
    end

    test "bind_integer after release does not segfault" do
      for _ <- 1..50 do
        {:ok, conn} = Engine.open(":memory:")
        {:ok, stmt} = Engine.prepare(conn, "select ?")
        :ok = Engine.release(conn, stmt)

        try do
          Engine.bind_integer(stmt, 1, 42)
        rescue
          _ -> :ok
        end

        :ok = Engine.close(conn)
      end
    end
  end

  # -- Busy timeout baseline behavior ------------------------------------------

  describe "busy_timeout behavior" do
    defp with_file_db(fun) do
      path = Temp.path!()

      try do
        fun.(path)
      after
        File.rm(path)
        File.rm(path <> "-wal")
        File.rm(path <> "-shm")
      end
    end

    defp setup_write_conflict(path, opts) do
      busy_timeout = Keyword.get(opts, :busy_timeout, 2000)

      {:ok, db1} = Engine.open(path)
      {:ok, db2} = Engine.open(path)

      :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
      :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
      :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

      :ok = Engine.set_busy_timeout(db2, busy_timeout)

      # db1 grabs exclusive write lock
      :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
      :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

      {db1, db2}
    end

    test "second writer waits ~busy_timeout before returning error" do
      with_file_db(fn path ->
        {db1, db2} = setup_write_conflict(path, busy_timeout: 500)

        {elapsed_us, result} =
          :timer.tc(fn -> Engine.execute(db2, "INSERT INTO t VALUES(3)") end)

        elapsed_ms = div(elapsed_us, 1000)

        assert {:error, _msg} = result

        assert elapsed_ms >= 300,
               "returned too quickly (#{elapsed_ms}ms), expected ~500ms delay"

        assert elapsed_ms < 3000, "took too long (#{elapsed_ms}ms)"

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "busy_timeout=0 returns error immediately" do
      with_file_db(fn path ->
        {db1, db2} = setup_write_conflict(path, busy_timeout: 0)

        {elapsed_us, result} =
          :timer.tc(fn -> Engine.execute(db2, "INSERT INTO t VALUES(3)") end)

        elapsed_ms = div(elapsed_us, 1000)

        assert {:error, _msg} = result

        assert elapsed_ms < 100,
               "busy_timeout=0 should return immediately, took #{elapsed_ms}ms"

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "multi_step returns :busy on write conflict with timeout=0" do
      with_file_db(fn path ->
        {db1, db2} = setup_write_conflict(path, busy_timeout: 0)

        {:ok, stmt} = Engine.prepare(db2, "INSERT INTO t VALUES(3)")
        result = Engine.multi_step(db2, stmt, 1)

        assert result == :busy

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "step returns :busy on write conflict with timeout=0" do
      with_file_db(fn path ->
        {db1, db2} = setup_write_conflict(path, busy_timeout: 0)

        {:ok, stmt} = Engine.prepare(db2, "INSERT INTO t VALUES(3)")
        result = Engine.step(db2, stmt)

        assert result == :busy

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end
  end

  # -- New NIF: set_busy_timeout/2 ---------------------------------------------

  describe ".set_busy_timeout/2" do
    test "sets busy timeout and returns :ok" do
      {:ok, conn} = Engine.open(":memory:")

      assert :ok = Engine.set_busy_timeout(conn, 5000)

      Engine.close(conn)
    end

    test "closed connection returns connection_closed" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.close(conn)

      assert {:error, :connection_closed} = Engine.set_busy_timeout(conn, 5000)
    end

    test "concurrent close and set_busy_timeout does not segfault" do
      for timeout_ms <- [0, 1, 50, 5_000], _ <- 1..100 do
        {:ok, conn} = Engine.open(":memory:")
        parent = self()

        spawn(fn -> send(parent, {:close, Engine.close(conn)}) end)

        spawn(fn ->
          send(parent, {:busy_timeout, Engine.set_busy_timeout(conn, timeout_ms)})
        end)

        assert_receive {:close, :ok}, 1000

        assert_receive {:busy_timeout, result}, 1000
        assert result in [:ok, {:error, :connection_closed}]
      end
    end

    test "timeout of 0 causes immediate SQLITE_BUSY" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

        :ok = Engine.set_busy_timeout(db2, 0)

        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

        {elapsed_us, result} =
          :timer.tc(fn -> Engine.execute(db2, "INSERT INTO t VALUES(3)") end)

        elapsed_ms = div(elapsed_us, 1000)

        assert {:error, _} = result
        assert elapsed_ms < 100

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "custom timeout delays before SQLITE_BUSY" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

        :ok = Engine.set_busy_timeout(db2, 500)

        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

        {elapsed_us, result} =
          :timer.tc(fn -> Engine.execute(db2, "INSERT INTO t VALUES(3)") end)

        elapsed_ms = div(elapsed_us, 1000)

        assert {:error, _} = result
        assert elapsed_ms >= 300, "returned too quickly (#{elapsed_ms}ms)"
        assert elapsed_ms < 3000, "took too long (#{elapsed_ms}ms)"

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end
  end

  describe ".set_progress_handler_steps/2" do
    test "accepts disabling and re-enabling the progress handler" do
      {:ok, conn} = Engine.open(":memory:")

      assert :ok = Engine.set_progress_handler_steps(conn, -1)
      assert :ok = Engine.set_progress_handler_steps(conn, 5_000)

      Engine.close(conn)
    end

    test "closed connection returns connection_closed" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.close(conn)

      assert {:error, :connection_closed} =
               Engine.set_progress_handler_steps(conn, 5_000)
    end

    test "concurrent close and set_progress_handler_steps does not segfault" do
      for steps <- [-1, 1, 1_000, 50_000], _ <- 1..100 do
        {:ok, conn} = Engine.open(":memory:")
        parent = self()

        spawn(fn -> send(parent, {:close, Engine.close(conn)}) end)

        spawn(fn ->
          send(
            parent,
            {:progress_steps, Engine.set_progress_handler_steps(conn, steps)}
          )
        end)

        assert_receive {:close, :ok}, 1000

        assert_receive {:progress_steps, result}, 1000
        assert result in [:ok, {:error, :connection_closed}]
      end
    end
  end

  # -- New NIF: cancel/1 -------------------------------------------------------

  describe ".cancel/1" do
    test "returns :ok on an idle connection" do
      {:ok, conn} = Engine.open(":memory:")

      assert :ok = Engine.cancel(conn)

      Engine.close(conn)
    end

    test "breaks through busy handler sleep" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

        # db2 gets a long busy timeout — without cancel, it would wait 60s
        :ok = Engine.set_busy_timeout(db2, 60_000)
        :ok = Engine.set_progress_handler_steps(db2, -1)

        # db1 holds exclusive lock
        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

        parent = self()

        # db2 tries to write — enters busy handler sleep
        spawn(fn ->
          result = Engine.execute(db2, "INSERT INTO t VALUES(3)")
          send(parent, {:write_result, result})
        end)

        # Give it time to enter the busy handler
        Process.sleep(200)

        # Cancel should wake the busy handler immediately
        :ok = Engine.cancel(db2)

        result =
          receive do
            {:write_result, r} -> r
          after
            2_000 -> :timeout
          end

        assert result != :timeout,
               "cancel did not break through busy handler within 2s"

        assert {:error, _} = result

        :ok = Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "cancelled connection can be reused after reset" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

        :ok = Engine.set_busy_timeout(db2, 60_000)

        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

        parent = self()

        spawn(fn ->
          result = Engine.execute(db2, "INSERT INTO t VALUES(3)")
          send(parent, {:write_result, result})
        end)

        Process.sleep(200)
        :ok = Engine.cancel(db2)

        receive do
          {:write_result, _} -> :ok
        after
          2_000 -> flunk("cancel did not break through busy handler")
        end

        # Release the lock
        :ok = Engine.execute(db1, "ROLLBACK")

        # db2 should be usable again for reads and writes
        assert {:ok, _stmt} = Engine.prepare(db2, "SELECT * FROM t")
        assert :ok = Engine.execute(db2, "INSERT INTO t VALUES(99)")

        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "cancel on closed connection is a no-op" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.close(conn)

      # cancel on a closed connection is safe (same as interrupt)
      assert :ok = Engine.cancel(conn)
    end
  end
end
