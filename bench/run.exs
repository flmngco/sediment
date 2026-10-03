# Compares sediment (WAL and MVCC) with exqlite on the same operations,
# through the low-level APIs and through DBConnection pools.
#
#   cd bench && mix deps.get && MIX_ENV=prod mix run run.exs
#
# BENCH_TIME (seconds per scenario, default 3) and BENCH_DIR (default a
# temporary directory) can be set in the environment.

defmodule Bench.Raw do
  @moduledoc false
  # The same operations on either driver's low-level module.

  def open(:exqlite, path, _mode) do
    {:ok, db} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode = wal; PRAGMA synchronous = normal")
    {Exqlite.Sqlite3, db}
  end

  def open(:turso, path, mode) do
    {:ok, db} = Sediment.Engine.open(path, journal_mode: mode)
    :ok = Sediment.Engine.execute(db, "PRAGMA synchronous = 1")
    {Sediment.Engine, db}
  end

  def setup({mod, db}, rows) do
    :ok = mod.execute(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)")
    :ok = mod.execute(db, "BEGIN")
    {:ok, stmt} = mod.prepare(db, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)")

    for i <- 1..rows do
      :ok = mod.bind(stmt, ["user #{i}", "user#{i}@example.com", rem(i, 90)])
      :done = mod.step(db, stmt)
    end

    :ok = mod.execute(db, "COMMIT")
  end

  def insert_stmt({mod, db}) do
    {:ok, stmt} = mod.prepare(db, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)")
    stmt
  end

  def insert({mod, db}, stmt, i) do
    :ok = mod.bind(stmt, ["new #{i}", "new#{i}@example.com", 42])
    :done = mod.step(db, stmt)
  end

  def point_stmt({mod, db}) do
    {:ok, stmt} = mod.prepare(db, "SELECT id, name, email, age FROM users WHERE id = ?")
    stmt
  end

  def point({mod, db}, stmt, id) do
    :ok = mod.reset(stmt)
    :ok = mod.bind(stmt, [id])
    {:row, _} = mod.step(db, stmt)
  end

  def scan({mod, db}) do
    {:ok, stmt} = mod.prepare(db, "SELECT id, name, email, age FROM users LIMIT 1000")
    {:ok, rows} = mod.fetch_all(db, stmt)
    :ok = mod.release(db, stmt)
    rows
  end

  def transaction({mod, db} = conn, stmt) do
    :ok = mod.execute(db, "BEGIN")
    for i <- 1..1000, do: insert(conn, stmt, i)
    :ok = mod.execute(db, "COMMIT")
  end
end

dir = System.get_env("BENCH_DIR") || Path.join(System.tmp_dir!(), "sediment_bench_#{System.os_time()}")
File.rm_rf!(dir)
File.mkdir_p!(dir)
time = String.to_integer(System.get_env("BENCH_TIME", "3"))

setups = [
  {"exqlite (WAL)", :exqlite, nil},
  {"sediment (WAL)", :turso, :wal},
  {"sediment (MVCC)", :turso, :mvcc}
]

raw =
  for {name, driver, mode} <- setups, into: %{} do
    conn = Bench.Raw.open(driver, Path.join(dir, "#{driver}-#{mode}.db"), mode)
    Bench.Raw.setup(conn, 10_000)
    {name, {conn, Bench.Raw.insert_stmt(conn), Bench.Raw.point_stmt(conn)}}
  end

scenario = fn title, fun ->
  IO.puts("\n## #{title}\n")

  Benchee.run(
    Map.new(raw, fn {name, ctx} -> {name, fn -> fun.(ctx) end} end),
    time: time,
    warmup: 1,
    memory_time: 0,
    print: [configuration: false, benchmarking: false]
  )
end

scenario.("Insert one row (autocommit, prepared)", fn {conn, insert, _} ->
  Bench.Raw.insert(conn, insert, :rand.uniform(1_000_000))
end)

scenario.("Point select by primary key (prepared)", fn {conn, _, point} ->
  Bench.Raw.point(conn, point, :rand.uniform(10_000))
end)

scenario.("Transaction of 1000 inserts", fn {conn, insert, _} ->
  Bench.Raw.transaction(conn, insert)
end)

scenario.("Select 1000 rows", fn {conn, _, _} -> Bench.Raw.scan(conn) end)

# DBConnection pools, one query per call (prepare + execute + release).
pools = %{
  "exqlite (WAL)" =>
    Exqlite.start_link(database: Path.join(dir, "pool-exqlite.db"), journal_mode: :wal, pool_size: 4, busy_timeout: 10_000),
  "sediment (WAL)" =>
    Sediment.start_link(database: Path.join(dir, "pool-turso-wal.db"), journal_mode: :wal, pool_size: 4, busy_timeout: 10_000),
  "sediment (MVCC)" =>
    Sediment.start_link(database: Path.join(dir, "pool-turso-mvcc.db"), journal_mode: :mvcc, pool_size: 4, busy_timeout: 10_000)
}

query = fn
  "exqlite" <> _, pool, sql, params -> Exqlite.query!(pool, sql, params)
  _, pool, sql, params -> Sediment.query!(pool, sql, params)
end

for {name, {:ok, pool}} <- pools do
  query.(name, pool, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)", [])

  for i <- 1..1000 do
    query.(name, pool, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)", ["u#{i}", "u#{i}@x", i])
  end
end

IO.puts("\n## DBConnection pool: insert one row (query/3)\n")

Benchee.run(
  Map.new(pools, fn {name, {:ok, pool}} ->
    {name,
     fn ->
       i = :rand.uniform(1_000_000)
       query.(name, pool, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)", ["p#{i}", "p#{i}@x", 42])
     end}
  end),
  time: time,
  warmup: 1,
  print: [configuration: false, benchmarking: false]
)

IO.puts("\n## DBConnection pool: point select (query/3)\n")

Benchee.run(
  Map.new(pools, fn {name, {:ok, pool}} ->
    {name, fn -> query.(name, pool, "SELECT * FROM users WHERE id = ?", [:rand.uniform(1000)]) end}
  end),
  time: time,
  warmup: 1,
  print: [configuration: false, benchmarking: false]
)

# Concurrent writers: 4 processes x 250 single-row transactions each. exqlite
# serializes writers on SQLite's lock; turso MVCC uses BEGIN CONCURRENT.
IO.puts("\n## 4 concurrent writers, 1000 single-row transactions in total\n")

for {name, {:ok, pool}} <- pools do
  begin = if name =~ "MVCC", do: [mode: :concurrent], else: [mode: :immediate]

  {micros, _} =
    :timer.tc(fn ->
      1..4
      |> Task.async_stream(
        fn w ->
          for i <- 1..250 do
            {:ok, _} =
              DBConnection.transaction(
                pool,
                fn conn -> query.(name, conn, "INSERT INTO users (name, email, age) VALUES (?, ?, ?)", ["w#{w}", "#{i}", w]) end,
                begin
              )
          end
        end,
        timeout: :infinity
      )
      |> Stream.run()
    end)

  IO.puts("#{String.pad_trailing(name, 22)} #{div(micros, 1000)} ms (#{round(1000 / (micros / 1_000_000))} tx/s)")
end

File.rm_rf!(dir)
