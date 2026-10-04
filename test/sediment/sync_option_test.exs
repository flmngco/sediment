defmodule Sediment.SyncOptionTest do
  # sync: true (S3 async durability) on connections without S3 is a no-op.
  use ExUnit.Case, async: true

  setup do
    {:ok, pool} = Sediment.start_link(database: Temp.path!(), pool_size: 1)
    Sediment.query!(pool, "create table t (x integer)")
    [pool: pool]
  end

  test "transactions and statements accept sync: true without S3", %{pool: pool} do
    assert {:ok, :done} =
             Sediment.transaction(
               pool,
               fn conn ->
                 Sediment.query!(conn, "insert into t values (1)")
                 :done
               end,
               sync: true
             )

    assert {:ok, _} = Sediment.query(pool, "insert into t values (2)", [], sync: true)
    assert %{rows: [[2]]} = Sediment.query!(pool, "select count(*) from t")
  end
end
