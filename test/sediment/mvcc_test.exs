defmodule Sediment.MvccTest do
  use ExUnit.Case, async: true

  alias Sediment.Engine

  setup do
    path = Temp.path!()
    # Connection events of these pools only (logs are global to the VM and
    # also show other async tests' deliberate disconnects).
    opts = [
      database: path,
      journal_mode: :mvcc,
      default_transaction_mode: :concurrent,
      connection_listeners: [self()]
    ]

    {:ok, a} = Sediment.start_link(opts)
    {:ok, b} = Sediment.start_link(opts)
    Sediment.query!(a, "create table t (id integer primary key, v integer)")
    Sediment.query!(a, "insert into t values (1, 0), (2, 0)")
    [a: a, b: b]
  end

  defp begin!(conn), do: Sediment.query!(conn, "BEGIN CONCURRENT")

  test "concurrent transactions touching different rows both commit", %{a: a, b: b} do
    begin!(a)
    begin!(b)
    Sediment.query!(a, "update t set v = 1 where id = 1")
    Sediment.query!(b, "update t set v = 2 where id = 2")
    Sediment.query!(a, "COMMIT")
    Sediment.query!(b, "COMMIT")

    assert %{rows: [[1, 1], [2, 2]]} = Sediment.query!(a, "select id, v from t order by id")
  end

  test "a write-write conflict aborts the second transaction", %{a: a, b: b} do
    begin!(a)
    begin!(b)
    Sediment.query!(a, "update t set v = 10 where id = 1")

    assert {:error, %Sediment.Error{message: "Write-write conflict"}} =
             Sediment.query(b, "update t set v = 20 where id = 1")

    # b's transaction is gone: statements are refused until it is rolled back
    assert {:error, %Sediment.Error{message: "the transaction was rolled back" <> _}} =
             Sediment.query(b, "select v from t where id = 1")

    Sediment.query!(b, "ROLLBACK")
    Sediment.query!(a, "COMMIT")
    assert %{rows: [[10]]} = Sediment.query!(b, "select v from t where id = 1")
  end

  test "statements after a conflict inside a transaction don't run in autocommit", %{
    a: a,
    b: b
  } do
    begin!(a)
    Sediment.query!(a, "update t set v = 10 where id = 1")

    result =
      Sediment.transaction(b, fn conn ->
        {:error, _conflict} = Sediment.query(conn, "update t set v = 20 where id = 1")
        Sediment.query(conn, "update t set v = 99 where id = 2")
      end)

    assert {:error, :rollback} = result
    Sediment.query!(a, "COMMIT")
    assert %{rows: [[10], [0]]} = Sediment.query!(b, "select v from t order by id")
  end

  test "DBConnection transactions roll back cleanly after a conflict", %{a: a, b: b} do
    begin!(a)
    Sediment.query!(a, "update t set v = 10 where id = 1")

    result =
      Sediment.transaction(b, fn conn ->
        case Sediment.query(conn, "update t set v = 20 where id = 1") do
          {:ok, _} -> :updated
          {:error, error} -> Sediment.rollback(conn, error)
        end
      end)

    assert {:error, %Sediment.Error{message: "Write-write conflict"}} = result
    Sediment.query!(a, "COMMIT")

    # b is still usable after the aborted transaction
    assert {:ok, :done} =
             Sediment.transaction(b, fn conn ->
               Sediment.query!(conn, "update t set v = 30 where id = 2")
               :done
             end)

    assert %{rows: [[10], [30]]} = Sediment.query!(b, "select v from t order by id")
  end

  test "parallel connects to a new database in mvcc mode all succeed" do
    opts = [database: Temp.path!(), journal_mode: :mvcc]

    results =
      1..6
      |> Enum.map(fn _ -> Task.async(fn -> Sediment.Connection.connect(opts) end) end)
      |> Enum.map(&Task.await/1)

    assert Enum.all?(results, &match?({:ok, _}, &1))
  end

  describe "default_transaction_mode: :concurrent" do
    test "a transaction that starts with DDL begins as IMMEDIATE", %{a: a} do
      assert {:ok, :migrated} =
               Sediment.transaction(a, fn conn ->
                 Sediment.query!(conn, "-- migration\n/* v2 */ CREATE TABLE m (x integer)")
                 Sediment.query!(conn, "ALTER TABLE m ADD COLUMN y integer")
                 Sediment.query!(conn, "insert into m values (1, 2)")
                 :migrated
               end)

      assert %{rows: [[1, 2]]} = Sediment.query!(a, "select x, y from m")
    end

    test "a DDL transaction rolls back", %{a: a} do
      assert {:error, :undo} =
               Sediment.transaction(a, fn conn ->
                 Sediment.query!(conn, "CREATE TABLE gone (x integer)")
                 Sediment.rollback(conn, :undo)
               end)

      assert {:error, %Sediment.Error{message: "no such table: gone"}} =
               Sediment.query(a, "select * from gone")
    end

    test "transactions that start with DML stay concurrent", %{a: a, b: b} do
      parent = self()

      task =
        Task.async(fn ->
          Sediment.transaction(b, fn conn ->
            Sediment.query!(conn, "update t set v = 2 where id = 2")
            send(parent, :b_wrote)

            receive do
              :commit -> :ok
            end
          end)
        end)

      assert_receive :b_wrote, 5_000

      # a writes while b's transaction is open: only possible when both are
      # BEGIN CONCURRENT
      assert {:ok, :ok} =
               Sediment.transaction(a, fn conn ->
                 Sediment.query!(conn, "update t set v = 1 where id = 1")
                 :ok
               end)

      send(task.pid, :commit)
      assert {:ok, :ok} = Task.await(task)
      assert %{rows: [[1], [2]]} = Sediment.query!(a, "select v from t order by id")
    end

    test "an explicit mode: :concurrent still refuses DDL", %{a: a} do
      assert_raise Sediment.Error, fn ->
        Sediment.transaction(
          a,
          fn conn -> Sediment.query!(conn, "CREATE TABLE nope (x integer)") end,
          mode: :concurrent
        )
      end
    end

    test "empty transactions commit and roll back", %{a: a} do
      assert {:ok, :nothing} = Sediment.transaction(a, fn _conn -> :nothing end)

      assert {:error, :nope} =
               Sediment.transaction(a, fn conn -> Sediment.rollback(conn, :nope) end)

      assert %{rows: [[2]]} = Sediment.query!(a, "select count(*) from t")
    end

    test "nested transactions begin the outer one first", %{a: a} do
      assert {:ok, {:ok, :inner}} =
               Sediment.transaction(a, fn conn ->
                 Sediment.transaction(conn, fn inner ->
                   Sediment.query!(inner, "update t set v = 5 where id = 1")
                   :inner
                 end)
               end)

      assert %{rows: [[5]]} = Sediment.query!(a, "select v from t where id = 1")
    end
  end

  test "a conflict detected at COMMIT fails the transaction without disconnecting", %{
    a: a,
    b: b
  } do
    parent = self()

    run = fn conn, v ->
      Task.async(fn ->
        try do
          Sediment.transaction(conn, fn tx ->
            Sediment.query!(tx, "insert into t values (100, ?)", [v])
            send(parent, :inserted)
            receive do: (:commit -> :ok)
          end)
        rescue
          error -> {:raised, error}
        end
      end)
    end

    ExUnit.CaptureLog.capture_log(fn ->
      t1 = run.(a, 1)
      t2 = run.(b, 2)
      for _ <- 1..2, do: assert_receive(:inserted, 5_000)
      send(t1.pid, :commit)
      assert {:ok, :ok} = Task.await(t1)
      send(t2.pid, :commit)

      assert {:raised, %Sediment.Error{message: "Write-write conflict"}} =
               Task.await(t2)

      assert %{rows: [[1]]} = Sediment.query!(b, "select v from t where id = 100")
    end)

    # neither pool's connection disconnected
    refute_receive {:disconnected, _pid}, 200
  end

  test "begin concurrent is rejected outside mvcc mode" do
    {:ok, conn} = Engine.open(":memory:")
    assert {:error, _} = Engine.execute(conn, "BEGIN CONCURRENT")
  end

  describe "AUTOINCREMENT inserts while another connection holds the write lock" do
    setup %{a: a, b: b} do
      Sediment.query!(a, "create table s (id integer primary key autoincrement, v integer)")
      Sediment.query!(a, "create table p (v text)")
      Sediment.query!(b, "BEGIN IMMEDIATE")
      Sediment.query!(b, "insert into p values ('b')")
      # b commits while a waits out the lock
      commit_b = Task.async(fn -> Process.sleep(150) && Sediment.query!(b, "COMMIT") end)
      [commit_b: commit_b]
    end

    test "never commit part of the surrounding transaction", %{a: a, commit_b: commit_b} do
      begin!(a)
      Sediment.query!(a, "insert into p values ('a')")

      assert {:error, %Sediment.Error{}} =
               Sediment.query(a, "insert into s (v) values (1) returning id")

      Task.await(commit_b)
      Sediment.query(a, "ROLLBACK")

      assert %{rows: [["b"]]} = Sediment.query!(a, "select v from p")
      assert %{rows: []} = Sediment.query!(a, "select id from s")
    end

    test "wait for the lock outside a transaction", %{a: a, commit_b: commit_b} do
      assert %{rows: [[1]]} = Sediment.query!(a, "insert into s (v) values (1) returning id")
      Task.await(commit_b)
      assert %{rows: [[1, 1]]} = Sediment.query!(a, "select id, v from s")
    end
  end
end
