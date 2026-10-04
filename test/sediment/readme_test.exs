defmodule Sediment.ReadmeTest do
  use ExUnit.Case, async: true

  alias Sediment.Vector

  test "DBConnection usage" do
    {:ok, conn} = Sediment.start_link(database: Temp.path!(), journal_mode: :wal)

    Sediment.query!(conn, "create table users (id integer primary key, name text)")
    Sediment.query!(conn, "insert into users (name) values (?)", ["Alice"])

    assert {:ok, %Sediment.Result{rows: [[1, "Alice"]]}} =
             Sediment.query(conn, "select id, name from users")

    {:ok, _} =
      Sediment.transaction(conn, fn conn ->
        Sediment.query!(conn, "update users set name = ? where id = ?", ["Bob", 1])
      end)

    assert %{rows: [["Bob"]]} = Sediment.query!(conn, "select name from users")
  end

  test "vector search through DBConnection" do
    {:ok, conn} = Sediment.start_link(database: :memory)
    Sediment.query!(conn, "create table docs (id integer primary key, embedding blob)")
    Sediment.query!(conn, "insert into docs values (?, ?)", [1, Vector.new([0.1, 0.2, 0.3])])
    Sediment.query!(conn, "insert into docs values (?, ?)", [2, Vector.new([-0.3, 0.2, 0.1])])

    assert %{rows: [[1], [2]]} =
             Sediment.query!(
               conn,
               "select id from docs order by vector_distance_cos(embedding, ?) limit 5",
               [Vector.new([0.1, 0.2, 0.25])]
             )
  end

  test "full-text search through DBConnection" do
    {:ok, conn} = Sediment.start_link(database: Temp.path!(), experimental: [:index_method])
    Sediment.query!(conn, "create table docs (id integer primary key, body text)")
    Sediment.query!(conn, "create index docs_fts on docs using fts(body)")
    Sediment.query!(conn, "insert into docs values (1, 'hello world'), (2, 'bye')")

    assert %{rows: [[1, score]]} =
             Sediment.query!(
               conn,
               "select id, fts_score(body, ?1) from docs where fts_match(body, ?1) order by 2 desc",
               ["hello"]
             )

    assert score > 0
  end
end
