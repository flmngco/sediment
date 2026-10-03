defmodule Sediment.ApiTest do
  use ExUnit.Case, async: true

  alias Sediment.Basic
  alias Sediment.Query

  describe "Sediment" do
    setup do
      {:ok, pool} = Sediment.start_link(database: Temp.path!())
      Sediment.query!(pool, "create table t (x integer)")
      [pool: pool]
    end

    test "prepare, execute and close a named query", %{pool: pool} do
      assert {:ok, query} = Sediment.prepare(pool, "ins", "insert into t values (?)")
      assert %Query{name: "ins"} = query
      assert {:ok, _, %{rows: []}} = Sediment.execute(pool, query, [1])
      assert %{num_rows: 1} = Sediment.execute!(pool, query, [2], command: :insert)
      assert :ok = Sediment.close(pool, query)

      query = Sediment.prepare!(pool, "sel", "select x from t order by x")
      assert %{rows: [[1], [2]]} = Sediment.execute!(pool, query, [])
      assert :ok = Sediment.close!(pool, query)
    end

    test "prepare_execute returns the query and the result", %{pool: pool} do
      assert {:ok, %Query{}, %{rows: [[3]]}} =
               Sediment.prepare_execute(pool, "q", "select ? + 1", [2])

      assert {%Query{}, %{rows: [[4]]}} =
               Sediment.prepare_execute!(pool, "q", "select ? + 1", [3])
    end

    test "errors come back as Sediment.Error, or raise with the bang variants", %{pool: pool} do
      assert {:error, %Sediment.Error{}} = Sediment.prepare(pool, "bad", "selec 1")
      assert {:error, %Sediment.Error{}} = Sediment.query(pool, "select * from nope")
      assert_raise Sediment.Error, fn -> Sediment.query!(pool, "select * from nope") end
      assert_raise Sediment.Error, fn -> Sediment.prepare!(pool, "bad", "selec 1") end
    end

    test "query accepts iodata", %{pool: pool} do
      assert %{rows: [[1]]} = Sediment.query!(pool, ["select ", ?1])
    end

    test "transaction and rollback", %{pool: pool} do
      assert {:error, :nope} =
               Sediment.transaction(pool, fn conn ->
                 Sediment.query!(conn, "insert into t values (9)")
                 Sediment.rollback(conn, :nope)
               end)

      assert %{rows: [[0]]} = Sediment.query!(pool, "select count(*) from t")
    end

    test "child_spec starts a pool under a supervisor" do
      spec = Sediment.child_spec(database: Temp.path!())
      pid = start_supervised!(spec)
      assert %{rows: [[1]]} = Sediment.query!(pid, "select 1")
    end
  end

  describe "Sediment.Basic" do
    test "open, exec, rows and close" do
      assert {:ok, conn} = Basic.open(":memory:")
      assert {:ok, [], []} = conn |> Basic.exec("create table t (x integer)") |> Basic.rows()
      Basic.exec(conn, "insert into t values (?), (?)", [1, 2])
      assert {:ok, [[1], [2]], ["x"]} = conn |> Basic.exec("select x from t") |> Basic.rows()

      assert {:error, "no such table: nope"} =
               conn |> Basic.exec("select * from nope") |> Basic.rows()

      assert :ok = Basic.close(conn)
      assert {:error, "connection_closed"} = conn |> Basic.exec("select 1") |> Basic.rows()
    end

    test "extension loading is not supported" do
      {:ok, conn} = Basic.open(":memory:")
      assert {:error, :not_supported} = Basic.load_extension(conn, "ext.so")
      assert {:error, _} = Basic.enable_load_extension(conn)
      assert {:error, _} = Basic.disable_load_extension(conn)
      Basic.close(conn)
    end
  end

  describe "Sediment.Query" do
    test "build infers the command from the statement" do
      assert %Query{command: :insert, name: "n"} =
               Query.build(statement: "INSERT INTO t VALUES (1)", name: "n")

      assert %Query{command: :update} = Query.build(statement: "UPDATE t SET x = 1")
      assert %Query{command: :delete} = Query.build(statement: "DELETE FROM t")
      assert %Query{command: nil} = Query.build(statement: "SELECT 1")
      assert %Query{command: nil, statement: nil} = Query.build([])
      assert %Query{command: :insert} = Query.build(statement: "SELECT 1", command: :insert)
    end

    test "to_string is the statement" do
      assert to_string(%Query{statement: "select 1"}) == "select 1"
    end
  end
end
