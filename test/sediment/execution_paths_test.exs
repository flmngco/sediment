defmodule Sediment.ExecutionPathsTest do
  # Sediment.Connection runs statements with positional parameters in one
  # native call (run_prepared) and named parameters step by step. Both paths
  # must return the same results for the same values.
  use ExUnit.Case, async: true

  setup do
    {:ok, pool} = Sediment.start_link(database: Temp.path!(), pool_size: 1)
    Sediment.query!(pool, "create table v (id integer primary key, a, b, c)")
    [pool: pool]
  end

  defp generators do
    [
      fn -> Enum.random([0, 1, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808]) end,
      fn -> :rand.uniform(1_000_000) - 500_000 end,
      fn -> Enum.random([0.0, -0.0, 1.5, -2.25e300, 1.0e-300]) end,
      fn -> :rand.uniform() * 1.0e6 end,
      fn -> nil end,
      fn -> Enum.random(["", "é", "🦀 turso", String.duplicate("x", 5000), "a\nb\tc"]) end,
      fn -> {:blob, :crypto.strong_rand_bytes(:rand.uniform(64))} end,
      fn -> Enum.random([true, false]) end,
      fn -> Enum.random([:atom, :"with space"]) end,
      fn -> Date.utc_today() end,
      fn -> ~N[2026-09-30 12:34:56.789] end,
      fn -> ~U[2026-09-30 12:34:56Z] end
    ]
  end

  defp random_value, do: Enum.random(generators()).()

  test "positional (one native call) and named (step by step) parameters agree", %{pool: pool} do
    :rand.seed(:exsss, {3, 5, 7})

    for i <- 1..300 do
      [a, b, c] = values = for _ <- 1..3, do: random_value()
      positional_id = 2 * i
      named_id = 2 * i + 1

      inserted_positional =
        Sediment.query!(pool, "insert into v values (?, ?, ?, ?)", [positional_id | values])

      inserted_named =
        Sediment.query!(pool, "insert into v values (:id, :a, :b, :c)", %{
          ":id" => named_id,
          ":a" => a,
          ":b" => b,
          ":c" => c
        })

      assert inserted_positional == inserted_named

      positional = Sediment.query!(pool, "select a, b, c from v where id = ?", [positional_id])

      named =
        Sediment.query!(pool, "select a, b, c from v where id = :id", %{":id" => named_id})

      assert positional.rows == named.rows, inspect(values)
      assert positional.columns == named.columns
      assert positional.num_rows == named.num_rows
    end
  end

  test "changes counts and RETURNING agree too", %{pool: pool} do
    positional =
      Sediment.query!(pool, "insert into v (a) values (?), (?) returning id, a", [1, 2],
        command: :insert
      )

    params = %{":x" => 1, ":y" => 2}
    sql = "insert into v (a) values (:x), (:y) returning id, a"
    named = Sediment.query!(pool, sql, params, command: :insert)

    assert positional.num_rows == 2 and named.num_rows == 2
    assert Enum.map(positional.rows, &tl/1) == Enum.map(named.rows, &tl/1)

    assert Sediment.query!(pool, "update v set b = ? where a = ?", [9, 1], command: :update).num_rows ==
             Sediment.query!(pool, "update v set b = :b where a = :a", %{":b" => 9, ":a" => 1},
               command: :update
             ).num_rows
  end

  test "values that can't be bound are errors, and the connection stays usable", %{pool: pool} do
    for value <- [Integer.pow(2, 70), %{a: 1}, {1, 2}, self(), [1 | 2]] do
      assert {:error, %Sediment.Error{statement: "select ?"}} =
               Sediment.query(pool, "select ?", [value])
    end

    assert %{rows: [[1]]} = Sediment.query!(pool, "select 1")
  end
end
