defmodule Sediment.PrepareCloseRaceTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  # A statement prepared concurrently with close/1 is either refused or
  # finalized by it: none may outlive the connection (it would keep the
  # database, and an S3 lease, alive).
  test "close finalizes statements prepared concurrently" do
    for _ <- 1..8_000 do
      {:ok, db} = Engine.open(":memory:")
      task = Task.async(fn -> Engine.prepare(db, "SELECT ?1") end)
      :ok = Engine.close(db)

      case Task.await(task) do
        {:ok, stmt} -> assert {:error, :invalid_statement} = Engine.bind_parameter_count(stmt)
        {:error, _} -> :ok
      end
    end
  end
end
