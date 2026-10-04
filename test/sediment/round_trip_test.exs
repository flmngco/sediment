defmodule Sediment.RoundTripTest do
  use ExUnit.Case, async: true

  import Bitwise, only: [<<<: 2]

  alias Sediment.Engine

  defp random_value do
    case :rand.uniform(6) do
      1 -> :rand.uniform(1 <<< 62) * Enum.random([1, -1])
      2 -> (:rand.uniform() - 0.5) * :math.pow(10, :rand.uniform(300))
      3 -> random_text()
      4 -> {:blob, :crypto.strong_rand_bytes(:rand.uniform(64) - 1)}
      5 -> nil
      6 -> Enum.random([0, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 0.0, -0.5])
    end
  end

  defp random_text do
    for _ <- 1..:rand.uniform(40), into: "" do
      <<Enum.random([?a..?z, 0x00..0x7F, 0x80..0x7FF, 0x800..0xD7FF, 0x10000..0x10FFFF])
        |> Enum.random()::utf8>>
    end
  end

  defp expected({:blob, bytes}), do: bytes
  defp expected(value), do: value

  test "random values survive bind -> insert -> select" do
    {:ok, conn} = Engine.open(":memory:")
    :ok = Engine.execute(conn, "create table v (id integer primary key, x)")
    {:ok, insert} = Engine.prepare(conn, "insert into v (id, x) values (?, ?)")

    values = for id <- 1..3_000, do: {id, random_value()}

    for {id, value} <- values do
      :ok = Engine.bind(insert, [id, value])
      :done = Engine.step(conn, insert)
    end

    {:ok, select} = Engine.prepare(conn, "select id, x from v order by id")
    {:ok, rows} = Engine.fetch_all(conn, select)
    assert Enum.count(rows) == 3_000

    for {[id, got], {id, value}} <- Enum.zip(rows, values) do
      assert got == expected(value), "row #{id}: #{inspect(value)} came back as #{inspect(got)}"
    end
  end
end
