defmodule Sediment.ExtensionsTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine
  alias Sediment.Vector

  doctest Sediment.Vector

  defp query(conn, sql, params \\ []) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    :ok = Engine.bind(stmt, params)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    :ok = Engine.release(conn, stmt)
    rows
  end

  describe "vector functions" do
    setup do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table docs (id integer primary key, embedding blob)")
      [conn: conn]
    end

    test "stores vectors and orders by cosine distance", %{conn: conn} do
      for {id, v} <- [{1, "[1, 0, 0]"}, {2, "[0, 1, 0]"}, {3, "[0.9, 0.1, 0]"}] do
        [] = query(conn, "insert into docs values (?, vector32(?))", [id, v])
      end

      rows =
        query(
          conn,
          "select id from docs order by vector_distance_cos(embedding, vector32(?)) limit 2",
          ["[1, 0, 0]"]
        )

      assert rows == [[1], [3]]
    end

    test "vector_extract renders a stored vector", %{conn: conn} do
      [] = query(conn, "insert into docs values (1, vector32('[1.5, 2, 3]'))")
      assert [["[1.5,2,3]"]] = query(conn, "select vector_extract(embedding) from docs")
    end

    test "float32 vectors are raw little-endian blobs", %{conn: conn} do
      [[blob]] = query(conn, "select vector32('[1, 2]')")
      assert blob == <<1.0::float-32-little, 2.0::float-32-little>>
    end
  end

  describe "Sediment.Vector" do
    setup do
      {:ok, conn} = Engine.open(":memory:")
      [conn: conn]
    end

    test "f32 blobs round trip through vector functions", %{conn: conn} do
      assert [["[1.5,-2]"]] = query(conn, "select vector_extract(?)", [Vector.new([1.5, -2])])
      [[blob]] = query(conn, "select vector32('[0.5, 4]')")
      assert Vector.to_list(blob) == [0.5, 4.0]
    end

    test "f64 blobs round trip through vector functions", %{conn: conn} do
      assert [["[1.5,-2]"]] =
               query(conn, "select vector_extract(?)", [Vector.new([1.5, -2], :f64)])

      [[blob]] = query(conn, "select vector64('[0.5, 4]')")
      assert Vector.to_list(blob) == [0.5, 4.0]
    end

    test "to_list rejects blobs that aren't dense f32 or f64 vectors", %{conn: conn} do
      dims = Enum.map_join(1..41, ", ", fn _ -> "1" end)
      [[blob]] = query(conn, "select vector1bit('[#{dims}]')")
      assert byte_size(blob) == 9
      assert_raise ArgumentError, fn -> Vector.to_list(blob) end
    end

    test "distance between bound vectors", %{conn: conn} do
      assert [[distance]] =
               query(conn, "select vector_distance_l2(?, ?)", [
                 Vector.new([0, 0]),
                 Vector.new([3, 4])
               ])

      assert_in_delta distance, 5.0, 1.0e-6
    end
  end

  describe "full-text search" do
    setup do
      {:ok, conn} = Engine.open(Temp.path!(), experimental: [:index_method])
      :ok = Engine.execute(conn, "create table docs (id integer primary key, body text)")

      :ok =
        Engine.execute(
          conn,
          "insert into docs values (1, 'hello world'), (2, 'goodbye moon'), (3, 'hello moon')"
        )

      [conn: conn]
    end

    test "fts index answers fts_match and ranks with fts_score", %{conn: conn} do
      :ok = Engine.execute(conn, "CREATE INDEX docs_fts ON docs USING fts(body)")

      rows =
        query(
          conn,
          "select id, fts_score(body, ?1) from docs where fts_match(body, ?1) order by id",
          ["hello"]
        )

      assert [[1, score], [3, score]] = rows
      assert score > 0
    end

    # Turso only scores through the index when fts_score and fts_match share
    # the same query expression; two separate parameters score 0.0.
    test "fts_score with a separate parameter is not scored", %{conn: conn} do
      :ok = Engine.execute(conn, "CREATE INDEX docs_fts ON docs USING fts(body)")

      assert [[1, +0.0], [3, +0.0]] =
               query(
                 conn,
                 "select id, fts_score(body, ?) from docs where fts_match(body, ?) order by id",
                 ["hello", "hello"]
               )
    end

    test "fts indexes require the index_method experimental feature" do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table docs (id integer primary key, body text)")

      assert {:error, "index method is an experimental feature" <> _} =
               Engine.execute(conn, "CREATE INDEX docs_fts ON docs USING fts(body)")
    end
  end
end
