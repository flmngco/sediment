defmodule Sediment.TortureOracleTest do
  # The crash-torture test's oracle must reject every way a restore can be
  # wrong, not just missing acknowledged rows.
  use ExUnit.Case, async: true

  alias Sediment.TortureOracle, as: Oracle

  defp tx(ids, status, pad \\ 0), do: {hd(ids), %{ids: ids, pad: pad, status: status}}
  defp rows(ids, pad \\ 0), do: Map.new(ids, &{&1, Oracle.expected_row(&1, pad)})

  defp txs do
    Map.new([
      tx([1, 2, 3], :acked),
      tx([11, 12], :maybe),
      tx([21, 22], :aborted),
      tx([31], :acked, 16_384)
    ])
  end

  test "a complete restore passes, with or without in-flight transactions" do
    base = Map.merge(rows([1, 2, 3]), rows([31], 16_384))
    assert Oracle.violations(txs(), base, MapSet.new()) == []
    assert Oracle.violations(txs(), Map.merge(base, rows([11, 12])), MapSet.new()) == []
  end

  test "a missing member of an acknowledged transaction is rejected" do
    restored = Map.merge(rows([1, 3]), rows([31], 16_384))
    assert [violation] = Oracle.violations(txs(), restored, MapSet.new())
    assert violation =~ "2 of 3 rows"
  end

  test "a partially restored in-flight transaction is rejected" do
    restored = rows([1, 2, 3]) |> Map.merge(rows([31], 16_384)) |> Map.merge(rows([11]))
    assert [violation] = Oracle.violations(txs(), restored, MapSet.new())
    assert violation =~ "partially"
  end

  test "rows of a transaction that failed with a conflict are rejected" do
    restored = rows([1, 2, 3]) |> Map.merge(rows([31], 16_384)) |> Map.merge(rows([21, 22]))
    assert [violation] = Oracle.violations(txs(), restored, MapSet.new())
    assert violation =~ "conflict"
  end

  test "changed values are rejected" do
    good = Map.merge(rows([1, 2, 3]), rows([31], 16_384))

    for changed <- [
          Map.put(good, 2, {9, "v2", nil}),
          Map.put(good, 2, {0, "v2x", nil}),
          Map.update!(good, 31, fn {w, v, pad} -> {w, v, binary_part(pad, 0, 100)} end)
        ] do
      assert [violation] = Oracle.violations(txs(), changed, MapSet.new())
      assert violation =~ "holds"
    end
  end

  test "unknown rows and rows that disappeared are rejected" do
    good = Map.merge(rows([1, 2, 3]), rows([31], 16_384))
    assert [unknown] = Oracle.violations(txs(), Map.merge(good, rows([99])), MapSet.new())
    assert unknown =~ "no reported transaction"
    assert [gone] = Oracle.violations(txs(), good, MapSet.new([11]))
    assert gone =~ "now missing"
  end

  describe "durability: async" do
    # Process "p": 1 acked (events 1-2), 11 acked (3-4), 21 acked (5-6).
    defp async_txs(extra \\ []) do
      Map.new(
        [
          {1, %{ids: [1, 2], pad: 0, status: :acked, proc: "p", began: 1, acked_at: 2}},
          {11, %{ids: [11], pad: 0, status: :acked, proc: "p", began: 3, acked_at: 4}},
          {21, %{ids: [21], pad: 0, status: :acked, proc: "p", began: 5, acked_at: 6}}
        ] ++ extra
      )
    end

    defp check(txs, ids, flushes \\ []),
      do: Oracle.violations(txs, rows(ids), MapSet.new(), mode: :async, flushes: flushes)

    test "any prefix of the commit order passes" do
      for ids <- [[], [1, 2], [1, 2, 11], [1, 2, 11, 21]],
          do: assert(check(async_txs(), ids) == [])
    end

    test "a hole is rejected" do
      assert [violation] = check(async_txs(), [1, 2, 21])
      assert violation =~ "acknowledged before it began"
    end

    test "a hole across a fence of the process is not: it continued from what was durable" do
      fenced = fn at ->
        Oracle.violations(async_txs(), rows([1, 2, 21]), MapSet.new(),
          mode: :async,
          fences: [%{proc: "p", at: at}]
        )
      end

      # 11 acknowledged at 4, 21 at 6
      assert fenced.(5) == []
      # a fence before 11 was acknowledged, or after 21 committed, explains nothing
      assert [_] = fenced.(3)
      assert [_] = fenced.(7)
    end

    test "commits flushed or synced must be there" do
      assert [_] = check(async_txs(), [1, 2], [%{proc: "p", start: 5}])
      assert check(async_txs(), [1, 2], [%{proc: "p", start: 3}]) == []

      synced = %{ids: [31], pad: 0, status: :synced, proc: "p", began: 7, acked_at: 8}
      assert [violation] = check(async_txs([{31, synced}]), [1, 2, 11, 21])
      assert violation =~ "synced"
    end

    test "another process's lost tail is no hole" do
      other = %{ids: [41], pad: 0, status: :acked, proc: "q", began: 0, acked_at: 1}
      assert check(async_txs([{41, other}]), [1, 2, 11, 21]) == []
    end
  end
end
