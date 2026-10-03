# Best-of-batches comparison used for the before/after table in RESULTS.md:
# robust against load from other jobs, unlike Benchee means.
#
#   cd bench && MIX_ENV=prod mix run ab.exs

# Best of 15 batches per operation (robust against load from other jobs).
dir = Path.join(System.tmp_dir!(), "core_ab_#{System.os_time()}")
File.mkdir_p!(dir)

best = fn n, f ->
  for b <- 1..15 do
    {us, _} = :timer.tc(fn -> for i <- 1..n, do: f.(b * n + i) end)
    us / n
  end
  |> Enum.min()
end

schema = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
ins = "INSERT INTO users (name, email, age) VALUES (?, ?, ?)"

raw = fn mod, db ->
  :ok = mod.execute(db, schema)
  {:ok, s} = mod.prepare(db, ins)

  auto =
    best.(300, fn i ->
      :ok = mod.bind(s, ["n#{i}", "e#{i}", 42])
      :done = mod.step(db, s)
    end)

  tx =
    best.(3, fn i ->
      :ok = mod.execute(db, "BEGIN")

      for j <- 1..1000,
          do:
            (
              :ok = mod.bind(s, ["n#{i}#{j}", "e", 42])
              :done = mod.step(db, s)
            )

      :ok = mod.execute(db, "COMMIT")
    end)

  {auto, tx / 1000}
end

pool = fn q, p ->
  q.(p, schema, [])
  for i <- 1..1000, do: q.(p, ins, ["u#{i}", "u@x", i])

  {best.(300, fn i -> q.(p, ins, ["p#{i}", "p@x", 42]) end),
   best.(300, fn i -> q.(p, "SELECT * FROM users WHERE id = ?", [rem(i, 1000) + 1]) end)}
end

{:ok, ex} = Exqlite.Sqlite3.open(Path.join(dir, "ex.db"))
:ok = Exqlite.Sqlite3.execute(ex, "PRAGMA journal_mode = wal; PRAGMA synchronous = normal")
{:ok, tw} = Sediment.Engine.open(Path.join(dir, "tw.db"), journal_mode: :wal)
:ok = Sediment.Engine.execute(tw, "PRAGMA synchronous = 1")
{:ok, tm} = Sediment.Engine.open(Path.join(dir, "tm.db"), journal_mode: :mvcc)
:ok = Sediment.Engine.execute(tm, "PRAGMA synchronous = 1")

{:ok, pe} =
  Exqlite.start_link(database: Path.join(dir, "pe.db"), journal_mode: :wal, pool_size: 1)

{:ok, pw} =
  Sediment.start_link(database: Path.join(dir, "pw.db"), journal_mode: :wal, pool_size: 1)

{:ok, pm} =
  Sediment.start_link(database: Path.join(dir, "pm.db"), journal_mode: :mvcc, pool_size: 1)

fmt = fn {a, b} -> "#{Float.round(a, 1)} / #{Float.round(b, 1)}" end

IO.puts(
  "raw autocommit insert / insert in tx (us):  exqlite #{fmt.(raw.(Exqlite.Sqlite3, ex))}  turso WAL #{fmt.(raw.(Sediment.Engine, tw))}  turso MVCC #{fmt.(raw.(Sediment.Engine, tm))}"
)

IO.puts(
  "pool insert / point select (us):            exqlite #{fmt.(pool.(&Exqlite.query!/3, pe))}  turso WAL #{fmt.(pool.(&Sediment.query!/3, pw))}  turso MVCC #{fmt.(pool.(&Sediment.query!/3, pm))}"
)

File.rm_rf!(dir)
