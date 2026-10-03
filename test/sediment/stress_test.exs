defmodule Sediment.StressTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  @moduletag :slow_test

  for mode <- [:wal, :mvcc] do
    test "a pool of #{mode} connections handles concurrent writers and readers" do
      {:ok, pool} =
        Sediment.start_link(
          database: Temp.path!(),
          journal_mode: unquote(mode),
          pool_size: 5,
          busy_timeout: 5_000
        )

      Sediment.query!(
        pool,
        "create table t (id integer primary key, worker integer, n integer)"
      )

      1..20
      |> Task.async_stream(
        fn worker ->
          for n <- 1..50 do
            Sediment.query!(pool, "insert into t (worker, n) values (?, ?)", [worker, n])

            %{rows: [[count]]} =
              Sediment.query!(pool, "select count(*) from t where worker = ?", [worker])

            assert count == n
          end
        end,
        timeout: 60_000,
        max_concurrency: 20
      )
      |> Stream.run()

      assert %{rows: [[1000]]} = Sediment.query!(pool, "select count(*) from t")
    end
  end

  test "many processes sharing one raw connection" do
    {:ok, conn} = Engine.open(":memory:")
    :ok = Engine.execute(conn, "create table t (x integer)")

    1..16
    |> Task.async_stream(
      fn i ->
        for j <- 1..200 do
          {:ok, stmt} = Engine.prepare(conn, "insert into t values (?)")
          :ok = Engine.bind(stmt, [i * 1000 + j])
          :done = Engine.step(conn, stmt)

          if rem(j, 3) == 0 do
            :ok = Engine.release(conn, stmt)
          end

          {:ok, select} = Engine.prepare(conn, "select count(*) from t")
          {:row, [_]} = Engine.step(conn, select)
        end
      end,
      timeout: 60_000
    )
    |> Stream.run()

    {:ok, stmt} = Engine.prepare(conn, "select count(*) from t")
    assert {:row, [3200]} = Engine.step(conn, stmt)
    :erlang.garbage_collect()
    assert :ok = Engine.close(conn)
  end

  test "closing while other processes use the connection" do
    for _ <- 1..20 do
      {:ok, conn} = Engine.open(":memory:")
      :ok = Engine.execute(conn, "create table t (x integer)")

      users =
        for _ <- 1..4 do
          Task.async(fn ->
            Enum.reduce_while(1..500, :ok, fn i, _ ->
              with {:ok, stmt} <- Engine.prepare(conn, "insert into t values (?)"),
                   :ok <- Engine.bind(stmt, [i]),
                   :done <- Engine.step(conn, stmt) do
                {:cont, :ok}
              else
                {:error, _} -> {:halt, :closed}
              end
            end)
          end)
        end

      Process.sleep(5)
      :ok = Engine.close(conn)
      Enum.each(users, &Task.await(&1, 10_000))
    end
  end
end
