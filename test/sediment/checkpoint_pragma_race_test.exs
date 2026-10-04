defmodule Sediment.CheckpointPragmaRaceTest do
  use ExUnit.Case, async: true

  # mvcc_checkpoint_threshold is per database and each connection's open
  # sets it, so a pool's later connections must not undo the custom pragma.
  test "a custom mvcc_checkpoint_threshold wins in every pool connection" do
    for _ <- 1..20 do
      {:ok, pool} =
        Sediment.start_link(
          database: Temp.path!(),
          journal_mode: :mvcc,
          pool_size: 8,
          custom_pragmas: [mvcc_checkpoint_threshold: 4096]
        )

      assert %{rows: [[4096]]} = Sediment.query!(pool, "PRAGMA mvcc_checkpoint_threshold")
      GenServer.stop(pool)
    end
  end
end
