defmodule Sediment.S3PoolTest do
  use ExUnit.Case, async: true

  alias Sediment.S3

  # Without S3 configured, a pool's S3 calls reach a connection and fail
  # there: what matters is that they get there, not their result.
  defp not_s3?({:error, _}), do: true
  defp not_s3?(_), do: false

  test "S3 pool functions reach the connection of a DBConnection pool" do
    {:ok, pool} = Sediment.start_link(database: ":memory:", pool_size: 1)
    assert not_s3?(S3.info(pool))
    assert not_s3?(S3.flush(pool, 1_000))

    {:ok, owned} =
      Sediment.start_link(database: ":memory:", pool: DBConnection.Ownership, pool_size: 1)

    assert not_s3?(S3.info(owned))

    name = :"s3_pool_test_#{System.unique_integer([:positive])}"
    {:ok, _} = Sediment.start_link(database: ":memory:", pool_size: 1, name: name)
    assert not_s3?(S3.refresh(name))
  end

  test "a process that isn't a pool is refused at once" do
    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)

    for call <- [
          &S3.snapshot/1,
          &S3.info/1,
          &S3.refresh/1,
          &S3.acknowledge_loss/1,
          &S3.flush(&1, 60_000)
        ] do
      {time, _} =
        :timer.tc(fn ->
          assert_raise ArgumentError, ~r/expects a db reference or a DBConnection pool/, fn ->
            call.(sup)
          end
        end)

      assert time < 1_000_000
    end

    assert_raise ArgumentError, ~r/no process :no_such_pool/, fn -> S3.info(:no_such_pool) end
    dead = spawn(fn -> :ok end)
    Process.sleep(10)
    assert_raise ArgumentError, ~r/expects a db reference/, fn -> S3.info(dead) end
  end

  test "an Ecto repo gets a hint" do
    test = self()

    repo =
      spawn_link(fn ->
        # What an Ecto repo's supervisor reports (ecto isn't a dependency here).
        Process.put(:"$initial_call", {:supervisor, Ecto.Repo.Supervisor, 1})
        send(test, :ready)
        Process.sleep(:infinity)
      end)

    assert_receive :ready

    assert_raise ArgumentError, ~r/Ecto repo .*Ecto.Adapter.lookup_meta\(repo\).pid/, fn ->
      S3.snapshot(repo)
    end
  end
end
