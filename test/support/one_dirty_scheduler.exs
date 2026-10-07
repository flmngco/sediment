# Run by Sediment.OneDirtySchedulerTest in a VM with one dirty IO scheduler
# (+SDio 1), so a native call queued behind a running one stays queued.
# Writing to stdout needs a dirty IO scheduler too: a failing step exits at
# once with its own status (without flushing); output only once all passed.
alias Sediment.{Connection, Engine}

{:ok, _} = Application.ensure_all_started(:sediment)

# Loading a module reads its file on a dirty IO scheduler as well: load
# everything up front, as a release does.
for app <- [:sediment, :db_connection, :telemetry, :elixir, :stdlib, :kernel],
    {:ok, modules} = :application.get_key(app, :modules),
    module <- modules,
    do: Code.ensure_loaded(module)

defmodule OneDirtyScheduler do
  @endless "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

  def endless, do: @endless

  # Runs `fun` in a process that sends {tag, result}; returns once that
  # process is in (or queued for) the native call `nif`.
  def start(tag, nif, fun, status) do
    parent = self()
    pid = spawn(fn -> send(parent, {tag, fun.()}) end)

    unless Enum.any?(1..500, fn _ -> in_nif?(pid, nif) or (Process.sleep(10) && false) end),
      do: fail(status)

    pid
  end

  defp in_nif?(pid, nif), do: Process.info(pid, :current_function) == {:current_function, nif}

  def expect(tag, ok?, status) do
    receive do
      {^tag, result} -> unless ok?.(result), do: fail(status)
    after
      10_000 -> fail(status)
    end
  end

  def fail(status), do: :erlang.halt(status, flush: false)
end

interrupted? = &match?({:error, "interrupted"}, &1)
execute = {Sediment.Native, :execute, 3}
parent = self()

# A pool disconnect while the only dirty IO scheduler runs the query: the
# stop must not need a dirty scheduler. (Exit statuses 10-14.)
{:ok, state} = Connection.connect(database: ":memory:")
query = fn -> Engine.execute(state.db, OneDirtyScheduler.endless()) end
OneDirtyScheduler.start(:query, execute, query, 10)
Process.sleep(200)
spawn(fn -> send(parent, {:disconnect, Connection.disconnect(nil, state)}) end)
OneDirtyScheduler.expect(:query, &match?({:error, _}, &1), 11)
OneDirtyScheduler.expect(:disconnect, &(&1 == :ok), 12)
{:ok, other} = Engine.open(":memory:")
:ok = Engine.close(other)

# A queued, cancelled; then B, on the same connection, admitted after the
# cancel: B's start must not erase A's cancel. (Exit statuses 20-25.)
{:ok, blocker} = Engine.open(":memory:")
{:ok, victim} = Engine.open(":memory:")
block = fn -> Engine.execute(blocker, OneDirtyScheduler.endless()) end
OneDirtyScheduler.start(:blocker, execute, block, 20)

OneDirtyScheduler.start(
  :a,
  execute,
  fn -> Engine.execute(victim, OneDirtyScheduler.endless()) end,
  21
)

:ok = Engine.cancel(victim)
OneDirtyScheduler.start(:b, execute, fn -> Engine.execute(victim, "select 42") end, 22)
:ok = Engine.cancel(blocker)
OneDirtyScheduler.expect(:blocker, interrupted?, 23)
OneDirtyScheduler.expect(:a, interrupted?, 24)
OneDirtyScheduler.expect(:b, &(&1 == :ok), 25)

IO.puts("all passed")
System.halt(0)
