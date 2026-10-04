defmodule Sediment.EdgeCasesTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  defp one(conn, sql, args \\ []) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    :ok = Engine.bind(stmt, args)
    Engine.step(conn, stmt)
  end

  setup do
    {:ok, conn} = Engine.open(":memory:")
    [conn: conn]
  end

  test "opening a directory or a missing parent fails cleanly" do
    assert {:error, _} = Engine.open(System.tmp_dir!())
    assert {:error, _} = Engine.open(Path.join([System.tmp_dir!(), "missing", "dir", "x.db"]))
    assert {:error, _} = Engine.open(Temp.path!(), mode: :readonly)
  end

  test "text with NUL bytes round trips", %{conn: conn} do
    assert {:row, ["a\0b"]} = one(conn, "select ?", ["a\0b"])
  end

  test "multi_step with odd chunk sizes", %{conn: conn} do
    :ok = Engine.execute(conn, "create table t (x); insert into t values (1), (2), (3)")
    {:ok, stmt} = Engine.prepare(conn, "select x from t order by x")
    assert {:rows, [[1]]} = Engine.multi_step(conn, stmt, 0)
    assert {:rows, [[2]]} = Engine.multi_step(conn, stmt, -5)
    assert {:done, [[3]]} = Engine.multi_step(conn, stmt, 1_000_000)
  end

  test "named parameters given as atoms, strings and charlists", %{conn: conn} do
    {:ok, stmt} = Engine.prepare(conn, "select :a, @b, $c")
    :ok = Engine.bind(stmt, %{:":a" => 1, "@b" => 2, ~c"$c" => 3})
    assert {:row, [1, 2, 3]} = Engine.step(conn, stmt)

    assert_raise ArgumentError, ~r/unknown named parameter/, fn ->
      Engine.bind(stmt, %{"a" => 1, "@b" => 2, "$c" => 3})
    end
  end

  test "empty, comment-only and whitespace scripts", %{conn: conn} do
    assert :ok = Engine.execute(conn, "")
    assert :ok = Engine.execute(conn, "   \n  ")
    assert :ok = Engine.execute(conn, "-- nothing here")
    assert :ok = Engine.execute(conn, "select 1; -- trailing comment")
  end

  test "SQL that isn't valid UTF-8 is an error", %{conn: conn} do
    assert {:error, "SQL is not valid UTF-8"} = Engine.prepare(conn, <<"select ", 0xFF>>)
    assert {:error, "SQL is not valid UTF-8"} = Engine.execute(conn, <<0xC3, 0x28>>)
  end

  test "preparing empty SQL is an error", %{conn: conn} do
    assert {:error, _} = Engine.prepare(conn, "")
  end

  test "prepare only accepts the first statement's result", %{conn: conn} do
    case Engine.prepare(conn, "select 1; select 2") do
      {:ok, stmt} -> assert {:row, [1]} = Engine.step(conn, stmt)
      {:error, _} -> :ok
    end
  end

  test "a failing statement in a script stops the script", %{conn: conn} do
    :ok = Engine.execute(conn, "create table t (x integer primary key)")

    assert {:error, _} =
             Engine.execute(
               conn,
               "insert into t values (1); insert into t values (1); insert into t values (2)"
             )

    assert {:row, [1]} = one(conn, "select count(*) from t")
  end

  test "huge and unicode identifiers", %{conn: conn} do
    name = String.duplicate("é", 300)
    :ok = Engine.execute(conn, ~s|create table "#{name}" ("🦀" text)|)
    :ok = Engine.execute(conn, ~s|insert into "#{name}" values ('crab')|)
    {:ok, stmt} = Engine.prepare(conn, ~s|select * from "#{name}"|)
    assert {:ok, ["🦀"]} = Engine.columns(conn, stmt)
    assert {:row, ["crab"]} = Engine.step(conn, stmt)
  end

  test "many parameters", %{conn: conn} do
    n = 2_000
    sql = "select " <> Enum.map_join(1..n, ", ", fn _ -> "?" end)
    assert {:row, values} = one(conn, sql, Enum.to_list(1..n))
    assert values == Enum.to_list(1..n)
  end

  test "reset in the middle of a scan restarts it", %{conn: conn} do
    :ok = Engine.execute(conn, "create table t (x); insert into t values (1), (2)")
    {:ok, stmt} = Engine.prepare(conn, "select x from t order by x")
    assert {:row, [1]} = Engine.step(conn, stmt)
    :ok = Engine.reset(stmt)
    assert {:row, [1]} = Engine.step(conn, stmt)
  end

  test "rebinding a statement mid-scan restarts it with new values", %{conn: conn} do
    :ok = Engine.execute(conn, "create table t (x); insert into t values (1), (2), (3)")
    {:ok, stmt} = Engine.prepare(conn, "select x from t where x >= ? order by x")
    :ok = Engine.bind(stmt, [1])
    assert {:row, [1]} = Engine.step(conn, stmt)
    :ok = Engine.bind(stmt, [3])
    assert {:row, [3]} = Engine.step(conn, stmt)
    assert :done = Engine.step(conn, stmt)
  end

  # turso_core 0.8.1; when this starts passing without the unary plus, drop
  # the README caveat (and ecto_sediment's).
  test "FULL OUTER JOIN on an indexed column needs a unary plus", %{conn: conn} do
    :ok =
      Engine.execute(conn, """
      create table p (a integer primary key);
      create table c (pa integer, name text);
      insert into p values (1), (3);
      insert into c values (1, 'x'), (7, 'z');
      """)

    assert {:error, "FULL OUTER JOIN requires an equality condition" <> _} =
             Engine.prepare(conn, "select count(*) from c full outer join p on p.a = c.pa")

    {:ok, stmt} =
      Engine.prepare(
        conn,
        "select c.name, p.a from c full outer join p on +p.a = c.pa order by 1, 2"
      )

    assert {:ok, [[nil, 3], ["x", 1], ["z", nil]]} = Engine.fetch_all(conn, stmt)
  end

  # turso_core 0.8.1; when the first drop starts working, drop the README
  # caveat (and ecto_sediment's).
  test "DROP COLUMN of a column with its own REFERENCES fails; a rebuild works", %{conn: conn} do
    :ok =
      Engine.execute(conn, """
      create table parent (id text primary key);
      create table child (id integer primary key, name text);
      alter table child add column parent_id text references parent(id) on delete set null;
      create table child2 (id integer primary key, parent_id text, foreign key (parent_id) references parent(id));
      insert into parent values ('p');
      insert into child values (1, 'a', 'p');
      """)

    assert {:error, "error in table child after drop column: unknown column \"parent_id\"" <> _} =
             Engine.execute(conn, "alter table child drop column parent_id")

    # Refused by SQLite too.
    assert {:error, "error in table child2 after drop column" <> _} =
             Engine.execute(conn, "alter table child2 drop column parent_id")

    :ok =
      Engine.execute(conn, """
      begin;
      create table child_new (id integer primary key, name text);
      insert into child_new (id, name) select id, name from child;
      drop table child;
      alter table child_new rename to child;
      commit;
      """)

    assert [["id"], ["name"]] = rows(conn, "select name from pragma_table_info('child')")

    assert [[1, "a"]] = rows(conn, "select * from child")
  end

  # turso compiles expressions recursively; on a dirty scheduler's own stack
  # a 50-term sum overflowed it and killed the VM. Depth 99 is the most turso
  # accepts.
  test "deep expressions compile without overflowing the scheduler stack", %{conn: conn} do
    sum = "select " <> Enum.map_join(1..99, " + ", fn _ -> "1" end)
    assert [[99]] = rows(conn, sum)

    assert [[1]] =
             rows(
               conn,
               "select " <> String.duplicate("abs(", 99) <> "1" <> String.duplicate(")", 99)
             )

    assert :ok = Engine.execute(conn, "create table deep (x); insert into deep " <> sum)
    assert [[99]] = rows(conn, "select x from deep")

    assert {:error, "Expression tree is too large" <> _} =
             Engine.prepare(conn, "select " <> Enum.map_join(1..150, " + ", fn _ -> "1" end))

    {:ok, pool} = Sediment.start_link(database: ":memory:", pool_size: 1)
    ors = "select 1 where " <> Enum.map_join(1..99, " or ", &"#{&1} = 99")
    assert %{rows: [[1]]} = Sediment.query!(pool, ors)
  end

  # Each trigger level runs a nested program: a chain of 100 overflowed the
  # scheduler stack while stepping.
  test "a long chain of triggers runs", %{conn: conn} do
    tables = for i <- 0..100, do: "create table c#{i} (x integer)"

    triggers =
      for i <- 0..99,
          do:
            "create trigger ct#{i} after insert on c#{i} begin insert into c#{i + 1} values (new.x + 1); end"

    :ok = Engine.execute(conn, Enum.join(tables ++ triggers, ";"))
    :ok = Engine.execute(conn, "insert into c0 values (0)")
    assert [[100]] = rows(conn, "select x from c100")
  end

  # turso_core 0.8.1 accepts the pragma but fires a trigger once, where
  # SQLite would recurse; pinned so an upgrade that changes it is noticed.
  test "recursive_triggers doesn't recurse", %{conn: conn} do
    :ok =
      Engine.execute(conn, """
      pragma recursive_triggers = on;
      create table r (x integer);
      create trigger rr after insert on r when new.x < 10 begin insert into r values (new.x + 1); end;
      insert into r values (0);
      """)

    assert [[2]] = rows(conn, "select count(*) from r")
  end

  # /dev/full fails every write with ENOSPC: a full disk under the WAL.
  @tag skip: not File.exists?("/dev/full") && "needs /dev/full"
  test "a commit on a full disk fails cleanly and the connection stays usable" do
    path = Temp.path!()
    {:ok, setup} = Engine.open(path, journal_mode: :wal)
    :ok = Engine.execute(setup, "create table t (x)")
    :ok = Engine.close(setup)
    File.rm(path <> "-wal")
    File.ln_s!("/dev/full", path <> "-wal")

    {:ok, conn} = Engine.open(path, journal_mode: :wal)
    assert {:error, "I/O error" <> _} = Engine.execute(conn, "insert into t values (1)")
    assert [[0]] = rows(conn, "select count(*) from t")
    assert :ok = Engine.close(conn)

    {:ok, pool} = Sediment.start_link(database: path, journal_mode: :wal, pool_size: 1)

    assert {:error, %Sediment.Error{message: "I/O error" <> _}} =
             Sediment.query(pool, "insert into t values (2)")

    assert %{rows: [[0]]} = Sediment.query!(pool, "select count(*) from t")
  end

  test "edge values round-trip exactly, with SQLite's types", %{conn: conn} do
    for {value, type} <- [
          {"a\0b", "text"},
          {<<0xFF, 0xFE, 0>>, "blob"},
          {"", "text"},
          {{:blob, ""}, "blob"},
          {nil, "null"},
          {-0.0, "real"},
          {5.0e-324, "real"},
          {(2 - :math.pow(2, -52)) * :math.pow(2, 1023), "real"}
        ] do
      expected = with {:blob, b} <- value, do: b
      assert [[^expected, ^type]] = rows(conn, "select ?1, typeof(?1)", [value])
    end
  end

  # SQLite's semantics, pinned so an upgrade that changes them is noticed.
  test "integer overflow, division by zero and casts behave like SQLite", %{conn: conn} do
    # 2^63: the exact result is out of range, so SQLite makes it a REAL
    two_63 = :math.pow(2, 63)
    assert [[^two_63]] = rows(conn, "select 9223372036854775807 + 1")
    assert [[^two_63]] = rows(conn, "select -9223372036854775808 / -1")
    assert [[nil]] = rows(conn, "select 1 / 0")
    assert [[nil]] = rows(conn, "select 1 % 0")

    assert [[9_223_372_036_854_775_807]] =
             rows(conn, "select cast('9223372036854775808' as integer)")

    {:ok, stmt} = Engine.prepare(conn, "select abs(-9223372036854775808)")
    assert {:error, "integer overflow"} = Engine.fetch_all(conn, stmt)
  end

  defp rows(conn, sql, args \\ []) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    :ok = Engine.bind(stmt, args)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    rows
  end
end
