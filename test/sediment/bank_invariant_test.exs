defmodule Sediment.BankInvariantTest do
  # Concurrent transfers through pools and DBConnection transactions: however
  # conflicts, busy errors and retries interleave, no money is created or
  # lost, and every committed transfer is recorded exactly once. In MVCC mode
  # a run sees hundreds of write-write conflicts, some only at COMMIT.
  use ExUnit.Case, async: true

  @accounts 20
  @initial 1_000

  for {mode, tx_mode} <- [wal: :immediate, mvcc: :concurrent] do
    test "transfers keep the total in #{mode} mode" do
      mode = unquote(mode)
      tx_mode = unquote(tx_mode)

      {:ok, pool} =
        Sediment.start_link(
          database: Temp.path!(),
          journal_mode: mode,
          pool_size: 4,
          busy_timeout: 5_000
        )

      Sediment.query!(
        pool,
        "create table accounts (id integer primary key, balance integer not null)"
      )

      Sediment.query!(pool, "create table transfers (id integer primary key, amount integer)")

      for id <- 1..@accounts,
          do: Sediment.query!(pool, "insert into accounts values (?, ?)", [id, @initial])

      committed =
        1..8
        |> Task.async_stream(fn worker -> worker_loop(pool, tx_mode, worker, 150) end,
          timeout: 120_000
        )
        |> Enum.map(fn {:ok, n} -> n end)
        |> Enum.sum()

      assert %{rows: [[total]]} = Sediment.query!(pool, "select sum(balance) from accounts")
      assert total == @accounts * @initial
      assert %{rows: [[^committed]]} = Sediment.query!(pool, "select count(*) from transfers")
      assert committed > 0
    end
  end

  # Returns how many transfers committed; retries conflicts and busy errors.
  defp worker_loop(pool, tx_mode, worker, n) do
    :rand.seed(:exsss, {worker, 2, 3})

    Enum.count(1..n, fn _ ->
      from = :rand.uniform(@accounts)
      to = rem(from + :rand.uniform(@accounts - 1), @accounts) + 1
      transfer_with_retries(pool, tx_mode, from, to, :rand.uniform(50), 20)
    end)
  end

  defp transfer_with_retries(_pool, _mode, _from, _to, _amount, 0), do: false

  defp transfer_with_retries(pool, mode, from, to, amount, attempts) do
    result =
      Sediment.transaction(
        pool,
        fn conn ->
          [[balance]] =
            Sediment.query!(conn, "select balance from accounts where id = ?", [from]).rows

          if balance < amount, do: Sediment.rollback(conn, :insufficient)

          Sediment.query!(conn, "update accounts set balance = balance - ? where id = ?", [
            amount,
            from
          ])

          Sediment.query!(conn, "update accounts set balance = balance + ? where id = ?", [
            amount,
            to
          ])

          Sediment.query!(conn, "insert into transfers (amount) values (?)", [amount])
          :ok
        end,
        mode: mode
      )

    case result do
      {:ok, :ok} -> true
      {:error, :insufficient} -> false
      {:error, _} -> retry(pool, mode, from, to, amount, attempts)
    end
  rescue
    e in [Sediment.Error, DBConnection.ConnectionError] ->
      _ = e
      retry(pool, mode, from, to, amount, attempts)
  end

  defp retry(pool, mode, from, to, amount, attempts) do
    Process.sleep(:rand.uniform(5))
    transfer_with_retries(pool, mode, from, to, amount, attempts - 1)
  end
end
