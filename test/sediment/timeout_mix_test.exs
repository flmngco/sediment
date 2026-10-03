defmodule Sediment.TimeoutMixTest do
  # Not async: the disconnect logs would show up in other tests' capture_log.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag :slow_test

  # Client timeouts close connections under queries, transactions and
  # streams. Every client must get a proper error (never a DBConnection
  # "bad return value" or a finalized statement's "invalid_statement"), and
  # the pool must recover.
  for mode <- [:wal, :mvcc] do
    test "queries, transactions and streams timing out in #{mode} mode" do
      mode = unquote(mode)

      {:ok, pool} =
        Sediment.start_link(
          database: Temp.path!(),
          journal_mode: mode,
          pool_size: 4,
          default_transaction_mode: transaction_mode(mode)
        )

      Sediment.query!(pool, "create table t (x integer)")
      Sediment.query!(pool, "insert into t select value from generate_series(1, 20000)")

      capture_log(fn ->
        results =
          1..600
          |> Task.async_stream(&run(pool, &1), max_concurrency: 30, timeout: 60_000)
          |> Enum.map(fn {:ok, result} -> result end)

        messages =
          for {_, %{message: message}} <- results, is_binary(message), uniq: true, do: message

        refute Enum.any?(messages, &(&1 =~ "bad return value")), inspect(messages)
        refute "invalid_statement" in messages
      end)

      assert %{rows: [[_]]} = Sediment.query!(pool, "select count(*) from t")
    end
  end

  defp transaction_mode(:mvcc), do: :concurrent
  defp transaction_mode(_mode), do: :deferred

  defp run(pool, i) do
    case rem(i, 3) do
      0 ->
        Sediment.transaction(
          pool,
          fn conn ->
            Sediment.query!(conn, "insert into t values (?)", [i])
            Sediment.query!(conn, "select count(*) from t")
          end,
          timeout: 2
        )

      1 ->
        Sediment.transaction(
          pool,
          fn conn ->
            %Sediment.Stream{
              conn: conn,
              query: "select * from t",
              params: [],
              options: [max_rows: 100]
            }
            |> Enum.count()
          end,
          timeout: 3
        )

      2 ->
        Sediment.query(pool, "select sum(x) from t", [], timeout: 1)
    end
  catch
    kind, reason -> {kind, reason}
  end
end
