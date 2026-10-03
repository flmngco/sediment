defmodule Sediment.S3TortureTest do
  # Crash torture for S3 durability (Jepsen-lite):
  #
  #   mix test --only torture                     (30 minutes)
  #   TORTURE_MINUTES=2 mix test --only torture
  #
  # A writer BEAM (test/support/torture_writer.exs) commits through a fault
  # proxy (random delays, dropped connections) and is kill -9'ed at random
  # points, during open, at a random time, or right after a snapshot starts.
  # After each kill the database is restored directly from S3 into a fresh
  # directory (which verifies the log's CRC chain and the snapshot), and
  # every commit acknowledged by any writer so far must be there; any other
  # row must belong to a commit that was in flight or failed.
  use ExUnit.Case, async: false

  alias Sediment.{Engine, S3, TcpProxy, TortureOracle}

  @moduletag :torture
  @moduletag timeout: :infinity

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  # TORTURE_DURABILITY=sync runs the writers with durability: :sync.
  defp durability, do: String.to_existing_atom(System.get_env("TORTURE_DURABILITY", "async"))

  defp bucket, do: Sediment.S3Bucket.name()
  defp endpoint, do: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
  defp access_key, do: System.get_env("S3_TEST_ACCESS_KEY_ID", "any")
  defp secret_key, do: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any")

  test "every acknowledged commit survives kill -9 at random points" do
    # Under this run's System.tmp_dir!() (see test_helper), not ExUnit's
    # checkout-relative :tmp_dir, which another run in the checkout would wipe.
    dir = Path.join(System.tmp_dir!(), "torture-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    minutes = String.to_integer(System.get_env("TORTURE_MINUTES", "30"))
    deadline = System.monotonic_time(:millisecond) + minutes * 60_000
    prefix = Sediment.S3Bucket.prefix("torture/#{System.os_time()}")
    proxy = if direct?(), do: nil, else: start_proxy()

    state = %{
      proxy: proxy,
      dir: dir,
      prefix: prefix,
      txs: %{},
      flushes: [],
      fences: [],
      seen: MapSet.new(),
      runs: 0,
      kills: %{},
      unacked_present: 0,
      lost: 0,
      last_db: nil
    }

    state = loop(state, deadline)

    IO.puts(
      "\ntorture: #{state.runs} kills #{inspect(state.kills)}, " <>
        "#{acked_rows(state.txs)} acknowledged rows checked after every kill (#{durability()}), " <>
        "#{state.unacked_present} unacknowledged rows (in flight or failed) found durable, " <>
        "#{state.lost} acknowledged rows lost (allowed with durability: async unless synced or flushed)"
    )
  end

  # Against a remote provider (HTTPS) the writers connect directly: the
  # network is the only fault source, and kills still happen at random points.
  defp direct?,
    do: Sediment.S3Bucket.remote?() or System.get_env("TORTURE_PROXY") == "off"

  defp start_proxy do
    %URI{port: upstream} = URI.parse(endpoint())
    {:ok, proxy} = TcpProxy.start_link(upstream)
    proxy
  end

  defp loop(state, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      state
    else
      state |> run_once() |> loop(deadline)
    end
  end

  defp run_once(state) do
    run = state.runs + 1

    faults =
      if state.proxy,
        do: [
          max_delay_ms: Enum.random([0, 0, 5, 20, 100]),
          drop: Enum.random([0.0, 0.0, 0.01, 0.05, 0.2])
        ],
        else: [max_delay_ms: 0]

    if state.proxy, do: :ok = TcpProxy.set_faults(state.proxy, faults)
    owner = if :rand.uniform(5) == 1, do: "writer-#{run}", else: "writer"

    db =
      if state.last_db && :rand.uniform(3) == 1,
        do: state.last_db,
        else: Path.join(state.dir, "run-#{run}/db")

    File.mkdir_p!(Path.dirname(db))
    # The first run stops right after the empty database is published.
    kill_at =
      if run == 1,
        do: :bootstrap,
        else: Enum.random([:open, :time, :time, :snapshot, :snapshot, :zombie])

    {events, kill} =
      if kill_at == :zombie do
        zombie(state, run, owner, db)
      else
        port = start_writer(state, run, owner, db, kill_at == :bootstrap)
        watch(port, kill_at, System.monotonic_time(:millisecond), "#{run}")
      end

    # Delayed chunks of the dead writer may still reach S3 through the proxy.
    Process.sleep(200 + Keyword.fetch!(faults, :max_delay_ms) * 20)

    txs = Map.merge(state.txs, events.txs)
    flushes = events.flushes ++ state.flushes
    fences = events.fences ++ state.fences
    rows = restore_rows(state, run)

    opts = [mode: durability(), flushes: flushes, fences: fences]

    assert TortureOracle.violations(txs, rows, state.seen, opts) == [],
           "run #{run} (#{kill}, #{inspect(faults)}, owner #{owner})"

    present = MapSet.new(Map.keys(rows))

    IO.puts(
      :stderr,
      "torture run #{run}: #{kill} kill, #{inspect(faults)}, owner #{owner}, " <>
        "#{acked_rows(events.txs)} acked this run, #{MapSet.size(present)} rows present" <>
        if(events.ready,
          do:
            ", killed #{events.killed_at - events.ready} ms after READY, " <>
              "#{map_size(events.txs)} begun",
          else: " (killed before READY)"
        ) <>
        errors(events.log)
    )

    %{
      state
      | runs: run,
        txs: txs,
        flushes: flushes,
        fences: fences,
        seen: present,
        kills: Map.update(state.kills, kill, 1, &(&1 + 1)),
        unacked_present: MapSet.size(present) - (acked_rows(txs) - lost_rows(txs, present)),
        lost: lost_rows(txs, present),
        last_db: db
    }
  end

  defp acked_rows(txs) do
    txs
    |> Map.values()
    |> Enum.filter(&(&1.status in [:acked, :synced]))
    |> Enum.map(&length(&1.ids))
    |> Enum.sum()
  end

  # Acknowledged rows missing from `present`: lost with durability: async.
  defp lost_rows(txs, present) do
    for {_, tx} <- txs,
        tx.status in [:acked, :synced],
        id <- tx.ids,
        not MapSet.member?(present, id),
        reduce: 0,
        do: (n -> n + 1)
  end

  # Split brain: writer A is paused (SIGSTOP) past its lease while writer B
  # (same owner, or another one once A's lease expired) takes over and
  # commits; then A runs again for a while before both are killed. A may
  # upload frames, but must not acknowledge a commit B's database lacks.
  defp zombie(state, run, owner, db) do
    a = start_writer(state, run, owner, db, false)
    events = gather(%{a => new_events("#{run}a")}, &(&1[a].ready != false), 60_000)
    events = gather(events, fn _ -> false end, 300 + :rand.uniform(3_000))
    signal(a, "STOP")

    successor = if :rand.uniform(2) == 1, do: owner, else: owner <> "-successor"
    b = start_writer(state, run, successor, db <> "-b", false, 5_000_000)
    events = gather(Map.put(events, b, new_events("#{run}b")), &(&1[b].ready != false), 60_000)
    events = gather(events, fn _ -> false end, 500 + :rand.uniform(2_000))
    signal(a, "CONT")
    events = gather(events, fn _ -> false end, 1_000 + :rand.uniform(3_000))

    {a_events, b_events} = {kill(a, events[a]), kill(b, events[b])}

    merged = %{
      a_events
      | txs: Map.merge(a_events.txs, b_events.txs),
        flushes: a_events.flushes ++ b_events.flushes,
        fences: a_events.fences ++ b_events.fences,
        log: b_events.log ++ a_events.log
    }

    {merged, :zombie}
  end

  # Collects events of several writers until `done?` holds or `timeout` ms
  # passed; a writer exiting on its own fails the test.
  defp gather(events, done?, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    gather_until(events, done?, deadline)
  end

  defp gather_until(events, done?, deadline) do
    if done?.(events) do
      events
    else
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {port, {:data, {:eol, line}}} when is_map_key(events, port) ->
          gather_until(Map.update!(events, port, &event(line, &1)), done?, deadline)

        {port, {:data, {:noeol, _}}} when is_map_key(events, port) ->
          gather_until(events, done?, deadline)

        {port, {:exit_status, status}} when is_map_key(events, port) ->
          flunk(
            "a writer exited by itself (#{status}):\n" <>
              Enum.join(Enum.reverse(Enum.take(events[port].log, 40)), "\n")
          )
      after
        remaining -> events
      end
    end
  end

  defp signal(port, signal) do
    {:os_pid, pid} = Port.info(port, :os_pid)
    {_, 0} = System.cmd("kill", ["-" <> signal, Integer.to_string(pid)])
  end

  defp writer_endpoint(%{proxy: nil}), do: endpoint()
  defp writer_endpoint(state), do: "http://127.0.0.1:#{TcpProxy.port(state.proxy)}"

  defp start_writer(state, run, owner, db, pause_before_schema, offset \\ 0) do
    elixir = System.find_executable("elixir")

    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    script = Path.expand("../support/torture_writer.exs", __DIR__)

    env =
      for {key, value} <- [
            {"TORTURE_BASE", Integer.to_string(run * 10_000_000 + offset)},
            {"TORTURE_BUCKET", bucket()},
            {"TORTURE_PREFIX", state.prefix},
            {"TORTURE_ENDPOINT", writer_endpoint(state)},
            {"TORTURE_REGION", Sediment.S3Bucket.region()},
            {"TORTURE_ACCESS_KEY_ID", access_key()},
            {"TORTURE_SECRET_ACCESS_KEY", secret_key()},
            {"TORTURE_OWNER", owner},
            {"TORTURE_GROUP_COMMIT", to_string(rem(run, 3) == 0)},
            {"TORTURE_DB", db},
            {"TORTURE_PAUSE_BEFORE_SCHEMA", to_string(pause_before_schema)},
            {"TORTURE_DURABILITY", Atom.to_string(durability())}
          ],
          do: {String.to_charlist(key), String.to_charlist(value)}

    Port.open({:spawn_executable, elixir}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      {:line, 65_536},
      args: paths ++ [script],
      env: env
    ])
  end

  # Collects the writer's events and kills it according to `kill_at`.
  defp watch(port, kill_at, started, proc) do
    kill_after =
      case kill_at do
        :open -> :rand.uniform(1_500)
        :time -> 500 + :rand.uniform(8_000)
        :snapshot -> nil
        :bootstrap -> nil
      end

    collect(port, kill_at, kill_after, started, new_events(proc))
  end

  defp new_events(proc) do
    %{
      txs: %{},
      ready: false,
      killed_at: nil,
      log: [],
      proc: proc,
      n: 0,
      flush_starts: %{},
      flushes: [],
      fences: []
    }
  end

  defp collect(port, kill_at, kill_after, started, events) do
    {timeout, kind} = next_kill(kill_at, kill_after, started, events)

    receive do
      {^port, {:data, {:eol, line}}} ->
        events = event(line, events)

        cond do
          kill_at == :snapshot and line == "S0" ->
            Process.sleep(:rand.uniform(400) - 1)
            {kill(port, events), :snapshot}

          kill_at == :bootstrap and line == "OPENED" ->
            {kill(port, events), :bootstrap}

          true ->
            collect(port, kill_at, kill_after, started, events)
        end

      {^port, {:data, {:noeol, _}}} ->
        collect(port, kill_at, kill_after, started, events)

      {^port, {:exit_status, status}} ->
        flunk(
          "the writer exited by itself (#{status}):\n" <>
            Enum.join(Enum.reverse(Enum.take(events.log, 40)), "\n")
        )
    after
      timeout -> {kill(port, events), kind}
    end
  end

  # How long until the planned kill: :open counts from the start, :time from
  # READY; whatever the plan, a writer is killed after 60 s at the latest.
  defp next_kill(kill_at, kill_after, started, events) do
    now = System.monotonic_time(:millisecond)
    cap = max(60_000 - (now - started), 0)

    planned =
      cond do
        kill_after && kill_at == :open -> max(kill_after - (now - started), 0)
        kill_after && events.ready -> max(kill_after - (now - events.ready), 0)
        true -> nil
      end

    if planned && planned <= cap, do: {planned, kill_at}, else: {cap, :cap}
  end

  defp errors(log) do
    case for("E " <> rest <- log, do: rest) do
      [] -> ""
      errors -> ", #{length(errors)} failed commits, e.g. #{List.last(errors)}"
    end
  end

  # Every line gets an index: the order of events within a writer process.
  defp event(line, events), do: record(line, %{events | n: events.n + 1})

  defp record("READY", events),
    do: %{events | ready: System.monotonic_time(:millisecond), log: ["READY" | events.log]}

  defp record("B " <> rest = line, events) do
    [ids, pad] = String.split(rest, " ")
    ids = parse(ids)

    tx = %{
      ids: ids,
      pad: String.to_integer(pad),
      status: :maybe,
      proc: events.proc,
      began: events.n,
      acked_at: nil
    }

    %{events | txs: Map.put(events.txs, hd(ids), tx), log: [line | events.log]}
  end

  defp record("A " <> ids = line, events) do
    txs = Map.update!(events.txs, hd(parse(ids)), &%{&1 | status: :acked, acked_at: events.n})
    %{events | txs: txs, log: [line | events.log]}
  end

  defp record("S " <> ids = line, events),
    do: %{events | txs: set_status(events.txs, ids, :synced), log: [line | events.log]}

  defp record("F0 " <> k = line, events),
    do: %{
      events
      | flush_starts: Map.put(events.flush_starts, k, events.n),
        log: [line | events.log]
    }

  defp record("FENCED" = line, events) do
    fence = %{proc: events.proc, at: events.n}
    %{events | fences: [fence | events.fences], log: [line | events.log]}
  end

  defp record("F1 " <> k = line, events) do
    flush = %{proc: events.proc, start: Map.fetch!(events.flush_starts, k)}
    %{events | flushes: [flush | events.flushes], log: [line | events.log]}
  end

  # A conflict is a definite failure: the rows must never become durable.
  # Other failures may still have committed.
  defp record("E " <> rest = line, events) do
    [ids | _] = String.split(rest, " ", parts: 2)

    txs =
      if rest =~ "Write-write conflict",
        do: set_status(events.txs, ids, :aborted),
        else: events.txs

    %{events | txs: txs, log: [line | events.log]}
  end

  defp record(line, events), do: %{events | log: [line | events.log]}

  defp parse(ids), do: ids |> String.split(",") |> Enum.map(&String.to_integer/1)

  defp set_status(txs, ids, status),
    do: Map.update!(txs, hd(parse(ids)), &%{&1 | status: status})

  # kill -9, then take the lines the writer printed before it died.
  defp kill(port, events) do
    events = %{events | killed_at: System.monotonic_time(:millisecond)}
    {:os_pid, pid} = Port.info(port, :os_pid)
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(pid)])
    drain(port, events)
  end

  defp drain(port, events) do
    receive do
      {^port, {:data, {:eol, line}}} -> drain(port, event(line, events))
      {^port, {:data, {:noeol, _}}} -> drain(port, events)
      {^port, {:exit_status, _}} -> events
    end
  end

  # The restored rows, id => {w, v, pad}. No database yet, or one without the
  # table (killed between bootstrap and CREATE TABLE), is no rows: the oracle
  # rejects that once anything was acknowledged or seen.
  defp restore_rows(state, run) do
    path = Path.join(state.dir, "verify-#{run}/db")
    File.mkdir_p!(Path.dirname(path))

    s3 = [
      bucket: bucket(),
      prefix: state.prefix,
      endpoint: endpoint(),
      region: Sediment.S3Bucket.region(),
      access_key_id: access_key(),
      encryption: false,
      secret_access_key: secret_key()
    ]

    rows =
      case S3.restore(path, s3) do
        {:ok, _} ->
          {:ok, db} = Engine.open(path)
          rows = select_rows(db)
          :ok = Engine.close(db)
          rows

        {:error, reason} ->
          if to_string(reason) =~ "no database",
            do: %{},
            else: flunk("run #{run}: restore failed: #{reason}")
      end

    File.rm_rf!(Path.dirname(path))
    rows
  end

  defp select_rows(db) do
    case Engine.prepare(db, "SELECT id, w, v, pad FROM t") do
      {:ok, stmt} ->
        {:ok, rows} = Engine.fetch_all(db, stmt)
        Map.new(rows, fn [id, w, v, pad] -> {id, {w, v, pad}} end)

      {:error, reason} ->
        if to_string(reason) =~ "no such table", do: %{}, else: flunk("select: #{reason}")
    end
  end
end
