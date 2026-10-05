defmodule Sediment.MvccLogTest do
  # turso names a database's MVCC log after the file without its extension
  # (app.1 and app.2 -> app.db-log). Distinct database files that would share
  # one log are refused before turso replays, checkpoints or truncates it.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  setup do
    dir = Path.join(System.tmp_dir!(), "mvcc-log-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp query(conn, sql) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    :ok = Engine.release(conn, stmt)
    rows
  end

  # An MVCC database with rows a and b, closed.
  defp mvcc_db(path) do
    {:ok, conn} = Engine.open(path, journal_mode: :mvcc)
    :ok = Engine.execute(conn, "create table t (v text); insert into t values ('a'), ('b')")
    :ok = Engine.close(conn)
    path
  end

  defp assert_intact(path) do
    {:ok, conn} = Engine.open(path)
    assert [["mvcc"]] = query(conn, "PRAGMA journal_mode")
    assert [["a"], ["b"]] = query(conn, "select v from t order by v")
    :ok = Engine.close(conn)
  end

  defp shared_log_error(dir, db, other) do
    "#{Path.join(dir, db)} would share its MVCC log #{Path.join(dir, "app.db-log")} " <>
      "with #{Path.join(dir, other)}"
  end

  test "a copy of an MVCC database next to it under the same stem is refused", %{dir: dir} do
    first = mvcc_db(Path.join(dir, "app.1"))
    log = File.read!(Path.join(dir, "app.db-log"))
    File.cp!(first, Path.join(dir, "app.2"))

    assert {:error, message} = Engine.open(Path.join(dir, "app.2"))
    assert message =~ shared_log_error(dir, "app.2", "app.1")
    assert message =~ "app-1.db and app-2.db"

    assert File.read!(Path.join(dir, "app.db-log")) == log
    # Refused both ways while both exist; renaming one ends it.
    assert {:error, _} = Engine.open(first)
    File.rename!(Path.join(dir, "app.2"), Path.join(dir, "app-2.db"))
    assert_intact(first)
  end

  test "an MVCC database open in this VM keeps its log", %{dir: dir} do
    {:ok, first} = Engine.open(Path.join(dir, "app.1"), journal_mode: :mvcc)
    :ok = Engine.execute(first, "create table t (v text); insert into t values ('a')")

    assert {:error, message} = Engine.open(Path.join(dir, "app.2"), journal_mode: :mvcc)
    assert message =~ "would share its MVCC log #{Path.join(dir, "app.db-log")}"

    :ok = Engine.execute(first, "insert into t values ('b')")
    :ok = Engine.close(first)
    assert_intact(Path.join(dir, "app.1"))
  end

  test "switching to MVCC is refused when another database file maps to the log", %{dir: dir} do
    {:ok, wal} = Engine.open(Path.join(dir, "app.sqlite"), journal_mode: :wal)
    :ok = Engine.execute(wal, "create table t (v text)")
    :ok = Engine.close(wal)

    assert {:error, message} = Engine.open(Path.join(dir, "app.db"), journal_mode: :mvcc)
    assert message =~ shared_log_error(dir, "app.db", "app.sqlite")

    {:ok, conn} = Engine.open(Path.join(dir, "app.db"))
    assert {:error, ^message} = Engine.execute(conn, "PRAGMA journal_mode = 'mvcc'")
    assert [["wal"]] = query(conn, "PRAGMA journal_mode")
    :ok = Engine.close(conn)
  end

  test "a WAL database next to another database's MVCC log says whose it is", %{dir: dir} do
    first = mvcc_db(Path.join(dir, "app.1"))
    elsewhere = Path.join(dir, "elsewhere")
    File.mkdir_p!(elsewhere)
    {:ok, wal} = Engine.open(Path.join(elsewhere, "app.x"), journal_mode: :wal)
    :ok = Engine.close(wal)
    File.cp!(Path.join(elsewhere, "app.x"), Path.join(dir, "app.x"))
    log = File.read!(Path.join(dir, "app.db-log"))
    assert byte_size(log) > 0

    # turso refuses the file itself (its header says WAL but a log exists).
    assert {:error, message} = Engine.open(Path.join(dir, "app.x"))
    assert message =~ "MVCC logical log file exists"
    assert message =~ "would share its MVCC log"

    assert File.read!(Path.join(dir, "app.db-log")) == log
    File.rm!(Path.join(dir, "app.x"))
    assert_intact(first)
  end

  test "export refuses a source whose log another database file maps to", %{dir: dir} do
    first = mvcc_db(Path.join(dir, "app.1"))
    File.cp!(first, Path.join(dir, "app.2"))

    assert {:error, message} =
             Sediment.export_sqlite(Path.join(dir, "app.2"), Path.join(dir, "out.sqlite"))

    assert message =~ shared_log_error(dir, "app.2", "app.1")
    refute File.exists?(Path.join(dir, "out.sqlite"))
  end

  test "databases with distinct stems have their own logs", %{dir: dir} do
    {:ok, one} = Engine.open(Path.join(dir, "app-1.db"), journal_mode: :mvcc)
    {:ok, two} = Engine.open(Path.join(dir, "app-2.db"), journal_mode: :mvcc)

    for {conn, v} <- [{one, "one"}, {two, "two"}] do
      :ok = Engine.execute(conn, "create table t (v text); insert into t values ('#{v}')")
    end

    assert [["one"]] = query(one, "select v from t")
    assert [["two"]] = query(two, "select v from t")
    :ok = Engine.close(one)
    :ok = Engine.close(two)
    assert File.exists?(Path.join(dir, "app-1.db-log"))
    assert File.exists?(Path.join(dir, "app-2.db-log"))
  end

  test "an export or backup next to an MVCC database doesn't lock it out", %{dir: dir} do
    first = mvcc_db(Path.join(dir, "app.db"))
    {:ok, _} = Sediment.export_sqlite(first, Path.join(dir, "app.sqlite"))
    File.cp!(Path.join(dir, "app.sqlite"), Path.join(dir, "app.bak"))

    assert_intact(first)

    # A new MVCC database there would: the export couldn't be opened next to
    # its log anymore.
    Enum.each([first, first <> "-wal", Path.join(dir, "app.db-log")], &File.rm/1)
    assert {:error, message} = Engine.open(first, journal_mode: :mvcc)
    assert message =~ "would share its MVCC log"
  end

  test "a symlink to an MVCC database is checked where its log is", %{dir: dir} do
    releases = Path.join(dir, "releases")
    File.mkdir_p!(releases)
    first = mvcc_db(Path.join(releases, "app.1"))
    File.ln_s!(first, Path.join(dir, "one.db"))
    {:ok, one} = Engine.open(Path.join(dir, "one.db"))
    :ok = Engine.execute(one, "insert into t values ('c')")

    File.cp!(first, Path.join(releases, "app.2"))
    File.ln_s!(Path.join(releases, "app.2"), Path.join(dir, "two.db"))
    assert {:error, message} = Engine.open(Path.join(dir, "two.db"))
    assert message =~ "would share its MVCC log #{Path.join(releases, "app.db-log")}"

    :ok = Engine.close(one)
    File.rm!(Path.join(releases, "app.2"))
    {:ok, conn} = Engine.open(first)
    assert [["a"], ["b"], ["c"]] = query(conn, "select v from t order by v")
    :ok = Engine.close(conn)
  end

  describe "ATTACH" do
    setup %{dir: dir} do
      {:ok, main} =
        Engine.open(Path.join(dir, "main.db"), journal_mode: :mvcc, experimental: [:attach])

      on_exit(fn -> Engine.close(main) end)
      %{main: main}
    end

    test "of a file whose log another database uses is refused", %{dir: dir, main: main} do
      first = mvcc_db(Path.join(dir, "app.1"))
      :ok = Engine.execute(main, "ATTACH '#{first}' AS a")
      :ok = Engine.execute(main, "insert into a.t values ('c')")
      File.cp!(first, Path.join(dir, "app.2"))

      assert {:error, message} = Engine.execute(main, "ATTACH '#{Path.join(dir, "app.2")}' AS b")
      assert message =~ shared_log_error(dir, "app.2", "app.1")
      {:ok, stmt} = Engine.prepare(main, "select 1")
      :ok = Engine.release(main, stmt)
      assert {:error, ^message} = Engine.prepare(main, "ATTACH '#{Path.join(dir, "app.2")}' AS b")
    end

    test "needs a literal file name", %{dir: dir, main: main} do
      assert {:error, "ATTACH needs its file name as a string literal" <> _} =
               Engine.prepare(main, "ATTACH ? AS a")

      mvcc_db(Path.join(dir, "ok.db"))
      :ok = Engine.execute(main, "ATTACH 'file:#{Path.join(dir, "ok.db")}' AS ok")
      assert [["a"], ["b"]] = query(main, "select v from ok.t order by v")
    end
  end
end
