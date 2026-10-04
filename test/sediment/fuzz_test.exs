defmodule Sediment.FuzzTest do
  # Random SQL fragments and byte strings through prepare/bind/step: every
  # input must come back as {:ok, _} or {:error, _}, never raise or crash.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  @words ~w|SELECT INSERT INTO VALUES UPDATE SET DELETE FROM WHERE AND OR NOT NULL ( ) , ; * = < > + - / ? ?1 :a @b ' '' " t x y id 1 0 -1 1e999 x'00' CREATE TABLE INDEX ON PRIMARY KEY UNIQUE BEGIN COMMIT ROLLBACK SAVEPOINT RELEASE WITH RECURSIVE AS UNION ALL JOIN LEFT GROUP BY ORDER LIMIT OFFSET CASE WHEN THEN ELSE END IN EXISTS CAST json_extract vector32 fts_match PRAGMA ATTACH DROP ALTER RENAME COLUMN RETURNING OVER PARTITION|

  defp random_sql do
    if :rand.uniform(3) == 1 do
      :crypto.strong_rand_bytes(:rand.uniform(40))
    else
      Enum.map_join(1..:rand.uniform(25), " ", fn _ -> Enum.random(@words) end)
    end
  end

  defp open do
    {:ok, conn} = Engine.open(":memory:")
    :ok = Engine.execute(conn, "create table t (id integer primary key, x, y text)")
    :ok = Engine.execute(conn, "insert into t values (1, 2, 'a'), (2, null, 'b')")
    conn
  end

  defp run(conn, sql) do
    case Engine.prepare(conn, sql) do
      {:ok, stmt} ->
        :ok = Engine.bind(stmt, List.duplicate(1, Engine.bind_parameter_count(stmt)))
        Engine.fetch_all(conn, stmt, 10)

      error ->
        error
    end
  end

  # Runs in the default suite: it caught a guard that looped forever on an
  # unterminated literal. The slow suite runs ten times as many statements.
  test "random SQL never raises or panics" do
    fuzz(5_000, {7, 11, 13})
  end

  @tag :slow_test
  @tag timeout: 600_000
  test "random SQL never raises or panics, 50,000 statements" do
    fuzz(50_000, {System.unique_integer([:positive]), 11, 13})
  end

  defp fuzz(n, seed) do
    :rand.seed(:exsss, seed)

    Enum.reduce(1..n, open(), fn _, conn ->
      sql = random_sql()
      result = run(conn, sql)
      assert match?({:ok, _}, result) or match?({:error, _}, result), inspect({seed, sql, result})
      refute match?({:error, "internal turso error" <> _}, result), inspect({seed, sql})
      # statements like DROP TABLE or ATTACH change the schema; start fresh now and then
      if :rand.uniform(200) == 1, do: open(), else: conn
    end)
  end
end
