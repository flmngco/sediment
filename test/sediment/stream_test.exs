defmodule Sediment.StreamTest do
  use ExUnit.Case, async: true

  alias Sediment.Query

  setup do
    {:ok, pool} = Sediment.start_link(database: Temp.path!())
    Sediment.query!(pool, "create table t (id integer primary key)")
    values = Enum.map_join(1..1_000, ", ", &"(#{&1})")
    Sediment.query!(pool, "insert into t values #{values}")
    [pool: pool]
  end

  defp stream(conn, query, params, opts \\ []) do
    %Sediment.Stream{conn: conn, query: query, params: params, options: opts}
  end

  test "streams a raw statement in chunks", %{pool: pool} do
    {:ok, chunks} =
      Sediment.transaction(pool, fn conn ->
        conn
        |> stream("select id from t where id > ? order by id", [100], max_rows: 250)
        |> Enum.to_list()
      end)

    assert Enum.map(chunks, & &1.num_rows) == [250, 250, 250, 150]
    assert chunks |> Enum.flat_map(& &1.rows) |> List.flatten() == Enum.to_list(101..1_000)
  end

  test "streams a prepared query", %{pool: pool} do
    {:ok, query} = Sediment.prepare(pool, "", "select id from t where id <= ?")

    {:ok, rows} =
      Sediment.transaction(pool, fn conn ->
        conn
        |> stream(query, [10])
        |> Enum.flat_map(& &1.rows)
      end)

    assert List.flatten(rows) == Enum.to_list(1..10)
  end

  test "halting a stream early releases the statement", %{pool: pool} do
    {:ok, first} =
      Sediment.transaction(pool, fn conn ->
        conn |> stream("select id from t order by id", [], max_rows: 10) |> Enum.take(1)
      end)

    assert [%{rows: [[1] | _]}] = first
    assert %{rows: [[1000]]} = Sediment.query!(pool, "select count(*) from t")
  end

  test "errors while streaming surface as Sediment.Error", %{pool: pool} do
    assert_raise Sediment.Error, ~r/no such table/, fn ->
      Sediment.transaction(pool, fn conn ->
        conn |> stream(%Query{statement: "select * from missing"}, []) |> Enum.to_list()
      end)
    end
  end

  test "count, member? and slice fall back to reducing", %{pool: pool} do
    {:ok, {count, member?, slice}} =
      Sediment.transaction(pool, fn conn ->
        s = stream(conn, "select id from t where id <= 10", [], max_rows: 4)
        {Enum.count(s), Enum.member?(s, :nope), s |> Enum.slice(1, 1) |> hd()}
      end)

    assert count == 3
    refute member?
    assert slice.rows == [[5], [6], [7], [8]]
  end
end
