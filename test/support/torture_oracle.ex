defmodule Sediment.TortureOracle do
  @moduledoc false
  # Checks a database restored in the crash-torture test
  # (test/sediment/s3_torture_test.exs) against the transactions the
  # writers reported. A transaction is identified by its row ids; its rows'
  # values are a function of the id (see `expected_row/2`).
  #
  #   :acked    - the commit returned :ok: all rows present (with
  #               durability: async, see violations/4)
  #   :synced   - committed, then flushed: all rows present
  #   :aborted  - it failed with a conflict: no row present
  #   :maybe    - in flight at a kill, or failed ambiguously: all or none
  #
  # Rows once seen in a restore must stay, and every present row must belong
  # to a reported transaction and hold its expected values.

  @type status :: :acked | :aborted | :maybe
  @type tx :: %{ids: [integer()], pad: non_neg_integer(), status: status()}

  @doc "The row a writer inserts for `id` with `pad` bytes of padding: {w, v, pad}."
  def expected_row(id, pad) do
    w = div(rem(id, 10_000_000), 1_000_000)
    padding = if pad == 0, do: nil, else: :binary.copy(<<rem(id, 251)>>, pad)
    {w, "v#{id}", padding}
  end

  @doc """
  Returns the violations (strings) of `rows` (a map of id => {w, v, pad}),
  given the reported transactions (`txs`, keyed by first id) and the ids an
  earlier restore showed (`seen`).

  Options, for `durability: async` (`mode: :async`): an acknowledged
  transaction may be lost unless it was `:synced` (committed, then flushed)
  or a flush of its writer process started after it was acknowledged and
  succeeded (`flushes`: `%{proc: p, start: event_index}`); and what was
  restored of one writer process is a prefix of its commit order: a
  restored transaction implies every transaction of the same process
  acknowledged before it began (`proc`, `began`, `acked_at`), unless the
  process was fenced in between (`fences`: `%{proc: p, at: event_index}`):
  its pool then reconnected from what was durable, so a commit acknowledged
  before the fence may be gone while later ones are there.
  """
  def violations(txs, rows, seen, opts \\ []) do
    mode = Keyword.get(opts, :mode, :sync)
    flushes = Keyword.get(opts, :flushes, [])
    fences = Keyword.get(opts, :fences, [])
    present = MapSet.new(Map.keys(rows))

    found =
      Map.new(txs, fn {first, tx} -> {first, Enum.count(tx.ids, &MapSet.member?(present, &1))} end)

    by_tx =
      Enum.flat_map(txs, fn {first, tx} ->
        check_tx(tx, found[first], required?(tx, mode, flushes))
      end)

    owners = for {_first, tx} <- txs, id <- tx.ids, into: %{}, do: {id, tx}
    by_row = Enum.flat_map(rows, fn {id, row} -> check_row(id, row, owners[id]) end)

    lost =
      seen
      |> MapSet.difference(present)
      |> Enum.map(&"row #{&1} was restored before, now missing")

    holes = if mode == :async, do: holes(txs, found, fences), else: []
    by_tx ++ by_row ++ lost ++ holes
  end

  # Whether all of the transaction's rows must be there.
  defp required?(%{status: :synced}, _mode, _flushes), do: true
  defp required?(%{status: :acked}, :sync, _flushes), do: true

  defp required?(%{status: :acked} = tx, :async, flushes),
    do: Enum.any?(flushes, &(&1.proc == tx.proc and &1.start > tx.acked_at))

  defp required?(_tx, _mode, _flushes), do: false

  # A restored transaction implies every one of its process acknowledged
  # before it began, unless the process was fenced after that acknowledgement
  # and before the restored one committed (acknowledged; any time later if it
  # wasn't).
  defp holes(txs, found, fences) do
    complete? = fn first, tx -> found[first] > 0 and found[first] == length(tx.ids) end

    missing =
      for {first, x} <- txs, x.status in [:acked, :synced], not complete?.(first, x), do: x

    for {first, y} <- txs,
        complete?.(first, y),
        x <- missing,
        x.proc == y.proc and x.acked_at < y.began,
        not fenced_between?(fences, x, y) do
      "transaction #{inspect(y.ids)} was restored but #{inspect(x.ids)}, " <>
        "acknowledged before it began, was not"
    end
  end

  defp fenced_between?(fences, x, y) do
    Enum.any?(fences, fn fence ->
      fence.proc == x.proc and fence.at > x.acked_at and
        (is_nil(y.acked_at) or fence.at < y.acked_at)
    end)
  end

  defp check_row(id, _row, nil), do: ["row #{id} belongs to no reported transaction"]

  defp check_row(id, row, tx) do
    if row == expected_row(id, tx.pad),
      do: [],
      else: ["row #{id} holds #{inspect(row, limit: 5)}"]
  end

  defp check_tx(%{status: :aborted, ids: ids}, found, _required) when found > 0,
    do: ["transaction #{inspect(ids)} failed with a conflict, yet #{found} rows restored"]

  defp check_tx(%{ids: ids} = tx, found, true) when found != length(ids),
    do: ["#{tx.status} transaction #{inspect(ids)}: #{found} of #{length(ids)} rows restored"]

  defp check_tx(%{ids: ids}, found, _required) when found > 0 and found < length(ids),
    do: ["transaction #{inspect(ids)} restored partially: #{found} of #{length(ids)} rows"]

  defp check_tx(_tx, _found, _required), do: []
end
