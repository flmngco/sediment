defmodule Sediment.StatementCacheModelTest do
  # A pooled connection (statement cache of 64, under eviction pressure from
  # ~90 distinct statements) and a raw connection that prepares every
  # statement afresh run the same random operations, including schema
  # changes; every read must agree.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  defp fresh(db, sql) do
    {:ok, stmt} = Engine.prepare(db, sql)
    {:ok, rows} = Engine.fetch_all(db, stmt)
    columns = Engine.columns(db, stmt)
    :ok = Engine.release(db, stmt)
    {columns, rows}
  end

  defp read_sql do
    case :rand.uniform(4) do
      1 -> "select * from t where id > #{:rand.uniform(30)} order by id"
      2 -> "select count(*), sum(id) from t where id % #{:rand.uniform(30)} = 0"
      3 -> "select * from t order by id limit #{:rand.uniform(30)}"
      4 -> "pragma table_info(t)"
    end
  end

  defp write_sql(step) do
    case :rand.uniform(8) do
      n when n <= 4 -> "insert into t (id) values (#{step})"
      5 -> "delete from t where id % 7 = #{rem(step, 7)}"
      6 -> "alter table t add column c#{step} integer default #{step}"
      7 -> "update t set id = id + 1000 where id = #{:rand.uniform(30)}"
      8 -> "pragma cache_size = #{-1000 - :rand.uniform(1000)}"
    end
  end

  test "cached statements agree with fresh ones through schema changes and eviction" do
    seed = String.to_integer(System.get_env("CACHE_MODEL_SEED", "11"))
    :rand.seed(:exsss, {seed, 13, 17})
    path = Temp.path!()
    {:ok, pool} = Sediment.start_link(database: path, pool_size: 1)
    Sediment.query!(pool, "create table t (id integer primary key)")
    {:ok, raw} = Engine.open(path)

    for step <- 1..1500 do
      if :rand.uniform(3) == 1 do
        Sediment.query!(pool, write_sql(step))
      else
        sql = read_sql()
        %{columns: columns, rows: rows} = Sediment.query!(pool, sql)
        assert {{:ok, columns}, rows} == fresh(raw, sql), "step #{step}: #{sql}"
      end
    end
  end
end
