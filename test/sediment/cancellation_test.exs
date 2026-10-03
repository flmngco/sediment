defmodule Sediment.CancellationTest do
  @moduledoc """
  Tests for query cancellation and deadlock prevention (issue #192).

  Validates that queries can be cancelled in flight and that disconnect
  properly interrupts stuck queries to prevent pool deadlocks.
  """

  use ExUnit.Case

  alias Sediment.Engine

  @moduletag :slow_test

  @long_running_select """
  WITH RECURSIVE r(i) AS (
    VALUES(0) UNION ALL SELECT i FROM r LIMIT 1000000000
  ) SELECT i FROM r WHERE i = 1;
  """

  @long_running_insert """
  WITH RECURSIVE r(i) AS (
    VALUES(0) UNION ALL SELECT i+1 FROM r LIMIT 1000000000
  ) INSERT INTO t SELECT i FROM r;
  """

  defp with_db(fun), do: with_db(":memory:", fun)

  defp with_db(path, fun) do
    {:ok, db} = Engine.open(path)

    try do
      fun.(db)
    after
      Engine.close(db)
    end
  end

  defp with_file_db(fun) do
    path = Temp.path!()

    try do
      fun.(path)
    after
      File.rm(path)
      File.rm(path <> "-wal")
      File.rm(path <> "-shm")
    end
  end

  defp interrupt_long_query(db, query, opts \\ []) do
    delay = Keyword.get(opts, :delay, 200)
    parent = self()

    spawn(fn ->
      result =
        case Keyword.get(opts, :mode, :multi_step) do
          :multi_step ->
            {:ok, stmt} = Engine.prepare(db, query)
            Engine.multi_step(db, stmt, 50)

          :execute ->
            Engine.execute(db, query)
        end

      send(parent, {:query_result, result})
    end)

    Process.sleep(delay)
    :ok = Engine.interrupt(db)

    receive do
      {:query_result, result} -> result
    after
      5_000 -> flunk("query did not return within 5s after interrupt")
    end
  end

  # -- Interrupt basics -------------------------------------------------------

  describe "interrupt" do
    test "aborts a long-running SELECT via multi_step" do
      with_db(fn db ->
        assert {:error, _} = interrupt_long_query(db, @long_running_select)
      end)
    end

    test "aborts a long-running SELECT via execute" do
      with_db(fn db ->
        assert {:error, _} =
                 interrupt_long_query(db, @long_running_select, mode: :execute)
      end)
    end

    test "aborts a long-running INSERT" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (i INTEGER)")

        assert {:error, _} =
                 interrupt_long_query(db, @long_running_insert, mode: :execute)
      end)
    end

    test "interrupt latency is under 2 seconds" do
      with_db(fn db ->
        {:ok, stmt} = Engine.prepare(db, @long_running_select)
        parent = self()

        spawn(fn ->
          result = Engine.multi_step(db, stmt, 50)
          send(parent, {:query_result, result})
        end)

        Process.sleep(200)
        t0 = System.monotonic_time(:millisecond)
        :ok = Engine.interrupt(db)
        assert_receive {:query_result, {:error, _}}, 5_000
        assert System.monotonic_time(:millisecond) - t0 < 2_000
      end)
    end

    test "has no effect when called before a query starts" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(42)")

        :ok = Engine.interrupt(db)

        {:ok, stmt} = Engine.prepare(db, "SELECT x FROM t")
        assert {:done, [[42]]} = Engine.multi_step(db, stmt, 50)
      end)
    end
  end

  # -- Connection state after interrupt ----------------------------------------

  describe "connection after interrupt" do
    test "is still usable for new queries" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(42)")

        {:error, _} = interrupt_long_query(db, @long_running_select)

        {:ok, stmt} = Engine.prepare(db, "SELECT x FROM t")
        assert {:done, [[42]]} = Engine.multi_step(db, stmt, 50)
      end)
    end

    test "survives 10 consecutive interrupt cycles" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(1)")

        for _cycle <- 1..10 do
          {:error, _} = interrupt_long_query(db, @long_running_select, delay: 50)
        end

        {:ok, stmt} = Engine.prepare(db, "SELECT x FROM t")
        assert {:done, [[1]]} = Engine.multi_step(db, stmt, 50)
      end)
    end

    test "interrupted write inside BEGIN leaves transaction rollback-able" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(1)")
        :ok = Engine.execute(db, "BEGIN IMMEDIATE")

        {:error, _} = interrupt_long_query(db, @long_running_insert, mode: :execute)

        {:ok, status} = Engine.transaction_status(db)

        case status do
          :transaction -> :ok = Engine.execute(db, "ROLLBACK")
          :idle -> :ok
        end

        {:ok, stmt} = Engine.prepare(db, "SELECT count(*) FROM t")
        assert {:done, [[1]]} = Engine.multi_step(db, stmt, 50)
      end)
    end
  end

  # -- Interrupt + close race (PR #342 validation) ----------------------------

  describe "interrupt + close race safety" do
    test "100 cycles of query/interrupt/close/reopen" do
      for cycle <- 1..100 do
        {:ok, db} = Engine.open(":memory:")
        parent = self()

        spawn(fn ->
          {:ok, stmt} = Engine.prepare(db, @long_running_select)
          result = Engine.multi_step(db, stmt, 50)
          send(parent, {:done, result})
        end)

        # An interrupt that lands before the statement starts running is
        # lost (as in SQLite), so keep interrupting until the query returns.
        interrupt_until_done = fn again, deadline ->
          :ok = Engine.interrupt(db)

          receive do
            {:done, _} -> :ok
          after
            10 ->
              if System.monotonic_time(:millisecond) > deadline,
                do: flunk("cycle #{cycle}: query did not return after interrupt"),
                else: again.(again, deadline)
          end
        end

        interrupt_until_done.(interrupt_until_done, System.monotonic_time(:millisecond) + 5_000)
        :ok = Engine.close(db)
      end
    end
  end

  # -- Busy handler vs interrupt -----------------------------------------------

  describe "interrupt vs busy handler" do
    test "interrupt does NOT break through busy handler sleep" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")
        :ok = Engine.set_busy_timeout(db2, 30_000)

        # db1 holds an exclusive write lock
        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(2)")

        parent = self()

        # db2 tries to write — enters busy handler sleep loop
        spawn(fn ->
          result = Engine.execute(db2, "BEGIN IMMEDIATE")
          send(parent, {:db2_result, result})
        end)

        Process.sleep(500)
        :ok = Engine.interrupt(db2)

        # db2 should NOT return within 5s — interrupt doesn't affect busy handler
        got_response =
          receive do
            {:db2_result, _} -> true
          after
            5_000 -> false
          end

        # Release the lock so db2 can finish
        Engine.execute(db1, "ROLLBACK")

        unless got_response do
          receive do
            {:db2_result, _} -> :ok
          after
            35_000 -> :ok
          end
        end

        Engine.close(db1)
        Engine.close(db2)

        refute got_response,
               "interrupt broke through busy handler — expected it to stay blocked"
      end)
    end
  end

  # -- Concurrent reads on single connection -----------------------------------

  describe "concurrent reads on single connection" do
    test "20 concurrent readers all get correct results" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

        for i <- 1..100 do
          :ok = Engine.execute(db, "INSERT INTO t VALUES(#{i})")
        end

        parent = self()

        for idx <- 1..20 do
          spawn(fn ->
            {:ok, stmt} = Engine.prepare(db, "SELECT count(*) FROM t")
            result = Engine.multi_step(db, stmt, 50)
            send(parent, {:reader, idx, result})
          end)
        end

        for _ <- 1..20 do
          assert_receive {:reader, _idx, {:done, [[100]]}}, 10_000
        end
      end)
    end
  end

  # -- Single-connection isolation ---------------------------------------------

  describe "single-connection isolation" do
    test "reads see own uncommitted writes" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(1)")

        :ok = Engine.execute(db, "BEGIN")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(2)")

        {:ok, stmt} = Engine.prepare(db, "SELECT count(*) FROM t")
        {:done, [[count]]} = Engine.multi_step(db, stmt, 50)

        assert count == 2
        :ok = Engine.execute(db, "ROLLBACK")
      end)
    end
  end

  # -- Implicit transaction staleness (two connections, WAL) -------------------

  describe "implicit transaction staleness" do
    test "completed statement allows reader to see new data" do
      with_file_db(fn path ->
        {:ok, writer} = Engine.open(path)
        {:ok, reader} = Engine.open(path)

        :ok = Engine.execute(writer, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(writer, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(writer, "INSERT INTO t VALUES(1)")

        {:ok, stmt1} = Engine.prepare(reader, "SELECT * FROM t")
        {:done, [[1]]} = Engine.multi_step(reader, stmt1, 50)

        :ok = Engine.execute(writer, "INSERT INTO t VALUES(2)")

        {:ok, stmt2} = Engine.prepare(reader, "SELECT count(*) FROM t")
        {:done, [[count]]} = Engine.multi_step(reader, stmt2, 50)
        assert count == 2

        Engine.close(writer)
        Engine.close(reader)
      end)
    end

    test "in-progress statement keeps reader on stale snapshot" do
      with_file_db(fn path ->
        {:ok, writer} = Engine.open(path)
        {:ok, reader} = Engine.open(path)

        :ok = Engine.execute(writer, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(writer, "CREATE TABLE t (i INTEGER)")
        for i <- 1..10, do: Engine.execute(writer, "INSERT INTO t VALUES(#{i})")

        # Step partially — implicit read tx still open
        {:ok, stmt1} = Engine.prepare(reader, "SELECT * FROM t")
        {:rows, _partial} = Engine.multi_step(reader, stmt1, 3)

        :ok = Engine.execute(writer, "INSERT INTO t VALUES(99)")

        {:ok, stmt2} = Engine.prepare(reader, "SELECT count(*) FROM t")
        {:done, [[count_stale]]} = Engine.multi_step(reader, stmt2, 50)
        assert count_stale == 10

        # Finish stmt1
        _rest = Engine.multi_step(reader, stmt1, 50)

        {:ok, stmt3} = Engine.prepare(reader, "SELECT count(*) FROM t")
        {:done, [[count_fresh]]} = Engine.multi_step(reader, stmt3, 50)
        assert count_fresh == 11

        Engine.close(writer)
        Engine.close(reader)
      end)
    end
  end

  # -- Close blocks on mutex ---------------------------------------------------

  describe "close vs running query" do
    test "close blocks until interrupt releases the mutex" do
      {:ok, db} = Engine.open(":memory:")
      {:ok, stmt} = Engine.prepare(db, @long_running_select)
      parent = self()

      spawn(fn ->
        result = Engine.multi_step(db, stmt, 50)
        send(parent, {:query_done, result})
      end)

      Process.sleep(200)

      spawn(fn ->
        result = Engine.close(db)
        send(parent, {:close_done, result})
      end)

      # Close should NOT return yet — it's blocked on the mutex
      refute_receive {:close_done, _}, 500

      :ok = Engine.interrupt(db)

      assert_receive {:query_done, {:error, _}}, 5_000
      assert_receive {:close_done, :ok}, 5_000
    end
  end

  # -- WAL concurrent access patterns -----------------------------------------

  describe "WAL mode" do
    test "concurrent reads from two connections succeed" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        for i <- 1..100, do: Engine.execute(db1, "INSERT INTO t VALUES(#{i})")

        {:ok, s1} = Engine.prepare(db1, "SELECT count(*) FROM t")
        {:ok, s2} = Engine.prepare(db2, "SELECT count(*) FROM t")

        assert {:done, [[100]]} = Engine.multi_step(db1, s1, 50)
        assert {:done, [[100]]} = Engine.multi_step(db2, s2, 50)

        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "second writer gets SQLITE_BUSY with busy_timeout=0" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)

        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db2, "PRAGMA journal_mode=WAL")
        :ok = Engine.set_busy_timeout(db1, 0)
        :ok = Engine.set_busy_timeout(db2, 0)
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")

        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")
        :ok = Engine.execute(db1, "INSERT INTO t VALUES(1)")

        assert {:error, _} = Engine.execute(db2, "BEGIN IMMEDIATE")

        Engine.execute(db1, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "a second writer waits busy_timeout, then fails or gets the lock" do
      with_file_db(fn path ->
        {:ok, db1} = Engine.open(path)
        {:ok, db2} = Engine.open(path)
        :ok = Engine.execute(db1, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(db1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(db1, "BEGIN IMMEDIATE")

        :ok = Engine.set_busy_timeout(db2, 300)
        {micros, result} = :timer.tc(fn -> Engine.execute(db2, "BEGIN IMMEDIATE") end)
        assert {:error, "database is locked"} = result
        assert micros >= 250_000

        # released while the second writer waits
        spawn(fn ->
          Process.sleep(200)
          Engine.execute(db1, "COMMIT")
        end)

        :ok = Engine.set_busy_timeout(db2, 10_000)
        {micros, result} = :timer.tc(fn -> Engine.execute(db2, "BEGIN IMMEDIATE") end)
        assert result == :ok
        assert micros < 5_000_000

        Engine.execute(db2, "ROLLBACK")
        Engine.close(db1)
        Engine.close(db2)
      end)
    end

    test "deferred read tx conflicts with concurrent write" do
      with_file_db(fn path ->
        {:ok, c1} = Engine.open(path)
        {:ok, c2} = Engine.open(path)

        :ok = Engine.execute(c1, "PRAGMA journal_mode=WAL")
        :ok = Engine.set_busy_timeout(c1, 0)
        :ok = Engine.set_busy_timeout(c2, 0)
        :ok = Engine.execute(c1, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(c1, "INSERT INTO t VALUES(1)")

        :ok = Engine.execute(c1, "BEGIN")
        {:ok, stmt} = Engine.prepare(c1, "SELECT * FROM t")
        {:done, [[1]]} = Engine.multi_step(c1, stmt, 50)

        :ok = Engine.execute(c2, "DELETE FROM t WHERE i = 1")

        assert {:error, _} = Engine.execute(c1, "INSERT INTO t VALUES(2)")

        Engine.execute(c1, "ROLLBACK")
        Engine.close(c1)
        Engine.close(c2)
      end)
    end
  end

  # -- DBConnection pool recovery after timeout --------------------------------

  describe "DBConnection timeout recovery" do
    @tag timeout: 30_000
    test "pool recovers after a long query is interrupted on disconnect" do
      with_file_db(fn path ->
        {:ok, conn} =
          Sediment.start_link(
            database: path,
            journal_mode: :wal,
            timeout: 1_000,
            busy_timeout: 0,
            pool_size: 1
          )

        Sediment.query!(conn, "CREATE TABLE t (i INTEGER)", [])
        Sediment.query!(conn, "INSERT INTO t VALUES(1)", [])

        # The long query should be interrupted when DBConnection's timeout
        # triggers a disconnect, which now calls Engine.cancel/1.
        result =
          try do
            Sediment.query(conn, @long_running_select, [], timeout: 1_000)
          rescue
            e -> {:exception, e}
          catch
            :exit, reason -> {:exit, reason}
          end

        assert match?({:exit, _}, result) or match?({:error, _}, result)

        # Pool should recover — next query should work
        assert {:ok, %Sediment.Result{rows: [[1]]}} =
                 Sediment.query(conn, "SELECT count(*) FROM t", [], timeout: 5_000)

        GenServer.stop(conn, :normal, 5_000)
      end)
    end

    @tag timeout: 30_000
    test "pool recovers when query is stuck in busy handler" do
      with_file_db(fn path ->
        # Open a raw connection that will hold an exclusive lock
        {:ok, blocker} = Engine.open(path)
        :ok = Engine.execute(blocker, "PRAGMA journal_mode=WAL")
        :ok = Engine.execute(blocker, "CREATE TABLE t (i INTEGER)")
        :ok = Engine.execute(blocker, "INSERT INTO t VALUES(1)")

        # Pool connection gets a long busy_timeout so it enters the sleep loop
        {:ok, conn} =
          Sediment.start_link(
            database: path,
            journal_mode: :wal,
            timeout: 2_000,
            busy_timeout: 60_000,
            pool_size: 1
          )

        # Blocker grabs exclusive write lock
        :ok = Engine.execute(blocker, "BEGIN IMMEDIATE")
        :ok = Engine.execute(blocker, "INSERT INTO t VALUES(2)")

        parent = self()

        # Pool tries to write — enters busy handler sleep loop waiting for lock
        spawn(fn ->
          result =
            try do
              Sediment.query(conn, "INSERT INTO t VALUES(3)", [], timeout: 2_000)
            rescue
              e -> {:exception, e}
            catch
              :exit, reason -> {:exit, reason}
            end

          send(parent, {:pool_result, result})
        end)

        # The pool query should return within 15s because disconnect calls
        # cancel(), which breaks through the busy handler sleep loop
        # (the fix for issue #192).
        pool_returned =
          receive do
            {:pool_result, _result} -> true
          after
            15_000 -> false
          end

        # Release the lock so everything can unwind
        Engine.execute(blocker, "ROLLBACK")

        unless pool_returned do
          receive do
            {:pool_result, _} -> :ok
          after
            65_000 -> :ok
          end
        end

        Engine.close(blocker)

        assert pool_returned,
               "Pool is stuck in busy handler — disconnect needs a custom busy handler to fix this"
      end)
    end
  end

  # -- Watchdog pattern --------------------------------------------------------

  describe "watchdog pattern" do
    test "auto-interrupts a query after a timeout" do
      with_db(fn db ->
        {:ok, stmt} = Engine.prepare(db, @long_running_select)
        timeout_ms = 500

        watchdog =
          spawn(fn ->
            receive do
              :cancel -> :ok
            after
              timeout_ms -> Engine.interrupt(db)
            end
          end)

        {elapsed_us, result} =
          :timer.tc(fn -> Engine.multi_step(db, stmt, 50) end)

        send(watchdog, :cancel)

        assert {:error, _} = result
        assert div(elapsed_us, 1000) < timeout_ms + 2_000
      end)
    end

    test "cancelling the watchdog lets the query complete normally" do
      with_db(fn db ->
        :ok = Engine.execute(db, "CREATE TABLE t (x INTEGER)")
        :ok = Engine.execute(db, "INSERT INTO t VALUES(1)")

        {:ok, stmt} = Engine.prepare(db, "SELECT x FROM t")

        watchdog =
          spawn(fn ->
            receive do
              :cancel -> :ok
            after
              5_000 -> Engine.interrupt(db)
            end
          end)

        result = Engine.multi_step(db, stmt, 50)
        send(watchdog, :cancel)

        assert {:done, [[1]]} = result
      end)
    end
  end
end
