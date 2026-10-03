# A real-S3 soak under a hard request budget: the S3 suites, crash tortures and benchmarks
# against a paid provider, every request counted and the run stopped at a cost cap.
#
#   elixir scripts/s3_soak.exs --target local     # a projection, against a local S3 server
#   elixir scripts/s3_soak.exs --target tigris --projection <summary.json>
#
# Local only: refuses to start under CI (CI or GITHUB_ACTIONS set). The tigris target needs
# an approval file, named by SOAK_GO_FILE (required), with a line starting with GO and
# naming the cap ("GO cap $2"), and only then reads the
# credentials file (~/.config/turso-elixir/tigris.env: AWS_ENDPOINT_URL_S3, AWS_BUCKET_NAME,
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION) into the environment of the child
# processes (never argv, never a log).
#
# Phases: suite (bucket listing, provider probe, the S3 suites once), async
# (crash torture, 30 min), sync (crash torture in sync mode, 10 min), bench
# (bench/s3_durability.exs), cleanup (delete everything under the run's root).
# Every S3 request is counted by the driver's meter (SEDIMENT_S3_METER_DIR, one
# directory per phase). The run aborts, drops a STOP file that makes the driver
# refuse further requests, kills the phase and goes to cleanup on: projected
# cost >= cap, a torture (oracle) failure, a failed probe, more than 2x the
# projected requests in a 5-minute window, or more than 5% failed requests.

defmodule Soak do
  @driver Path.expand("..", __DIR__)
  @price %{"A" => 0.005 / 1000, "B" => 0.0005 / 1000, "free" => 0.0}
  @reserve 0.02
  @window_ms 300_000

  def main(argv) do
    {opts, _} =
      OptionParser.parse!(argv,
        strict: [
          target: :string,
          cap: :float,
          projection: :string,
          out: :string,
          phases: :string,
          async_minutes: :integer,
          sync_minutes: :integer,
          commit_interval_ms: :integer
        ]
      )

    for var <- ["CI", "GITHUB_ACTIONS"],
        System.get_env(var) not in [nil, ""],
        do: abort!("#{var} is set: the S3 soak runs only locally, never in CI")

    target = Keyword.get(opts, :target, "local")
    {target_env, go_cap} = target_env(target)
    cap = min(Keyword.get(opts, :cap, 2.0), go_cap || 2.0)
    out = Keyword.get(opts, :out, "/tmp/tigris-soak")
    File.rm_rf!(out)
    File.mkdir_p!(out)
    stamp = "soak-" <> Calendar.strftime(DateTime.utc_now(), "%Y%m%d-%H%M%S")

    projection =
      case opts[:projection] do
        nil -> %{}
        path -> path |> File.read!() |> JSON.decode!() |> Map.fetch!("phases")
      end

    phases =
      opts
      |> Keyword.get(:phases, "suite,async,sync,bench")
      |> String.split(",", trim: true)

    settings = %{
      async_minutes: Keyword.get(opts, :async_minutes, 30),
      sync_minutes: Keyword.get(opts, :sync_minutes, 10),
      commit_interval_ms: Keyword.get(opts, :commit_interval_ms, 100)
    }

    st = %{
      out: out,
      stamp: stamp,
      target: target,
      cap: cap,
      projection: projection,
      target_env: target_env,
      settings: settings,
      phases: %{},
      order: [],
      offsets: %{},
      partial: %{},
      last_progress: nil,
      aborted: nil
    }

    say(st, "soak #{stamp}: target #{target}, cap $#{cap}, phases #{Enum.join(phases, ",")}")

    try do
      run(st, phases)
    catch
      kind, reason ->
        # Whatever broke, nothing of this run may keep sending requests.
        for dir <- Path.wildcard(Path.join([out, "meter", "*"])),
            do: File.write!(Path.join(dir, "STOP"), "")

        kill_marked(stamp <> "-", :prefix)

        say(
          st,
          "HARNESS CRASHED: requests stopped, processes killed; clean up with the cleanup phase"
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp run(st, phases) do
    precompile(st, "bench" in phases)

    st =
      Enum.reduce(phases, st, fn phase, st ->
        if st.aborted, do: st, else: run_phase(st, phase)
      end)

    st = run_phase(%{st | aborted: nil} |> Map.put(:abort_reason, st.aborted), "cleanup")
    report(st)
  end

  ## Target

  defp target_env("local") do
    {[
       {"S3_TEST_ENDPOINT", System.get_env("SOAK_LOCAL_ENDPOINT", "http://127.0.0.1:8333")},
       {"S3_TEST_BUCKET", System.get_env("SOAK_LOCAL_BUCKET", "sediment-tests")},
       {"S3_TEST_ACCESS_KEY_ID", "any"},
       {"S3_TEST_SECRET_ACCESS_KEY", "any"},
       {"S3_TEST_REGION", "us-east-1"},
       # Skip what a remote provider can't run (TcpProxy tests), so the
       # projection runs exactly the Tigris workload.
       {"S3_TEST_REMOTE", "1"}
     ], nil}
  end

  defp target_env("tigris") do
    go =
      System.get_env("SOAK_GO_FILE") ||
        abort!("set SOAK_GO_FILE to the approval file: no request goes to Tigris without it")

    text =
      case File.read(go) do
        {:ok, text} -> text
        _ -> abort!("#{go} is missing: no request may go to Tigris without an approved GO")
      end

    # A line starting with the word GO ("GO cap $2"); never "NO GO", "GOT IT".
    (Regex.match?(~r/^\s*GO\b/m, text) and not Regex.match?(~r/\bNO[\s-]*GO\b/i, text)) ||
      abort!("#{go} doesn't say GO")

    cap =
      case Regex.run(~r/cap\D{0,4}(\d+(?:\.\d+)?)/i, text) do
        [_, n] -> String.to_float(if String.contains?(n, "."), do: n, else: n <> ".0")
        _ -> abort!("#{go} names no cap")
      end

    creds = Path.expand("~/.config/turso-elixir/tigris.env")

    vars =
      creds
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.reject(&String.starts_with?(String.trim(&1), "#"))
      |> Map.new(fn line ->
        [k, v] =
          line |> String.trim() |> String.trim_leading("export ") |> String.split("=", parts: 2)

        {k, v |> String.trim() |> String.trim("\"") |> String.trim("'")}
      end)

    get = fn k -> Map.get(vars, k) || abort!("#{creds} has no #{k}") end

    {[
       {"S3_TEST_ENDPOINT", get.("AWS_ENDPOINT_URL_S3")},
       {"S3_TEST_BUCKET", get.("AWS_BUCKET_NAME")},
       {"S3_TEST_ACCESS_KEY_ID", get.("AWS_ACCESS_KEY_ID")},
       {"S3_TEST_SECRET_ACCESS_KEY", get.("AWS_SECRET_ACCESS_KEY")},
       {"S3_TEST_REGION", Map.get(vars, "AWS_REGION", "auto")}
     ], cap}
  end

  defp target_env(other), do: abort!("unknown target #{other}")

  ## Phases

  defp steps(st, "suite") do
    # Tigris: the whole bucket; locally only the run's root (the shared test
    # bucket holds other runs' objects, and Tigris's is expected empty).
    listing = if st.target == "tigris", do: [{"S3_TEST_PREFIX_ROOT", ""}], else: []

    [
      # Before anything else: what the bucket holds (only our soak roots may be
      # there), then the provider probe (conditional PUTs honoured).
      {:bucket_check, "cargo", ~w(test --release soak_list -- --ignored --nocapture),
       "native/sediment_nif", listing},
      {:probe, "mix", ["run", "--no-start", "-e", probe_code()], ".", [{"MIX_ENV", "test"}]},
      {:suite, "mix", ~w(test --only s3), ".", [{"MIX_ENV", "test"}]},
      {:suite, "cargo", ~w(test --release seaweed), "native/sediment_nif", []}
    ]
  end

  defp steps(st, "async"), do: [torture(st, "async", st.settings.async_minutes)]
  defp steps(st, "sync"), do: [torture(st, "sync", st.settings.sync_minutes)]

  defp steps(_st, "bench"),
    do: [
      {:bench, "mix", ~w(run s3_durability.exs), "bench",
       [{"MIX_ENV", "prod"}, {"COMMITS", "300"}]}
    ]

  defp steps(_st, "cleanup") do
    [
      {:cleanup, "cargo", ~w(test --release soak_cleanup -- --ignored --nocapture),
       "native/sediment_nif", []}
    ]
  end

  defp torture(st, durability, minutes) do
    {:torture, "mix", ~w(test --only torture), ".",
     [
       {"MIX_ENV", "test"},
       {"TORTURE_MINUTES", Integer.to_string(minutes)},
       {"TORTURE_DURABILITY", durability},
       {"TORTURE_PROXY", "off"},
       {"TORTURE_COMMIT_INTERVAL_MS", Integer.to_string(st.settings.commit_interval_ms)}
     ]}
  end

  defp probe_code do
    """
    Application.ensure_all_started(:sediment)
    e = &System.fetch_env!/1
    dir = Path.join(System.tmp_dir!(), "soak-probe-\#{System.os_time()}")
    File.mkdir_p!(dir)
    s3 = [bucket: e.("S3_TEST_BUCKET"), endpoint: e.("S3_TEST_ENDPOINT"), region: e.("S3_TEST_REGION"),
          access_key_id: e.("S3_TEST_ACCESS_KEY_ID"), secret_access_key: e.("S3_TEST_SECRET_ACCESS_KEY"),
          prefix: e.("S3_TEST_PREFIX_ROOT") <> "probe", owner: "soak-probe", durability: :sync, encryption: false]
    case Sediment.Engine.open(Path.join(dir, "p.db"), s3: s3) do
      {:ok, db} ->
        :ok = Sediment.Engine.execute(db, "CREATE TABLE t (x)")
        :ok = Sediment.Engine.close(db)
        IO.puts("SOAK_PROBE ok")
      {:error, reason} ->
        IO.puts("SOAK_PROBE failed: " <> inspect(reason))
        System.halt(1)
    end
    File.rm_rf!(dir)
    """
  end

  defp run_phase(st, phase) do
    meter = Path.join([st.out, "meter", phase])
    File.mkdir_p!(meter)
    started = now()

    ph = %{
      meter: meter,
      started: started,
      ended: nil,
      counts: %{},
      errors: 0,
      window: :queue.new(),
      err_window: :queue.new(),
      max_window: 0,
      steps: []
    }

    st = %{st | phases: Map.put(st.phases, phase, ph), order: st.order ++ [phase]}
    say(st, "phase #{phase} starts")

    st =
      Enum.reduce(steps(st, phase), st, fn step, st ->
        if st.aborted, do: st, else: run_step(st, phase, step)
      end)

    st = poll(st, phase)
    put_in(st, [:phases, phase, :ended], now())
  end

  defp run_step(st, phase, {kind, cmd, args, dir, extra}) do
    root = if phase == "cleanup", do: st.stamp <> "/", else: "#{st.stamp}/#{phase}/"
    meter = st.phases[phase].meter
    # Every process of the step inherits the marker (torture writers too):
    # an abort kills them all by it.
    marker = "#{st.stamp}-#{phase}-#{System.unique_integer([:positive])}"

    env =
      Enum.map(System.get_env(), fn {k, _} -> k end)
      |> Enum.filter(&String.starts_with?(&1, "AWS_"))
      |> Enum.map(&{&1, false})
      |> Kernel.++(st.target_env)
      |> Kernel.++([
        {"S3_TEST_PREFIX_ROOT", root},
        {"SEDIMENT_S3_METER_DIR", meter},
        {"SOAK_STEP", marker}
      ])
      |> Kernel.++(extra)
      |> Enum.map(fn {k, v} -> {String.to_charlist(k), v && String.to_charlist(v)} end)

    log = Path.join(st.out, "#{phase}.log")

    File.write!(
      log,
      "\n## #{kind}: #{cmd} #{Enum.join(Enum.reject(args, &String.contains?(&1, "\n")), " ")}\n",
      [:append]
    )

    port =
      Port.open({:spawn_executable, System.find_executable(cmd)}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args,
        cd: Path.join(@driver, dir),
        env: env
      ])

    {status, st, output} = pump(st, phase, port, marker, log, "")
    step = %{kind: kind, cmd: cmd, status: status}
    st = update_in(st, [:phases, phase, :steps], &(&1 ++ [step]))
    judge(st, phase, kind, status, output)
  end

  # Relays the step's output to its log, polls the meter every 2 s (however
  # much the step prints), and kills the step's processes when the run must
  # abort.
  defp pump(st, phase, port, marker, log, tail, next_poll \\ nil) do
    now = now()

    if next_poll == nil or now >= next_poll do
      st = poll(st, phase)

      case st.aborted do
        nil ->
          wait(st, phase, port, marker, log, tail, now + 2_000)

        reason ->
          say(st, "ABORT (#{reason}): stopping S3 requests and phase #{phase}")
          stop_requests(st)
          kill_marked(marker)
          drain(port)
          # Anything the step started late (a writer spawning as it died).
          kill_marked(marker)
          {:killed, st, tail}
      end
    else
      wait(st, phase, port, marker, log, tail, next_poll)
    end
  end

  defp wait(st, phase, port, marker, log, tail, next_poll) do
    receive do
      {^port, {:data, data}} ->
        File.write!(log, data, [:append])
        tail = tail <> data
        size = byte_size(tail)
        tail = if size > 20_000, do: binary_part(tail, size - 20_000, 20_000), else: tail
        pump(st, phase, port, marker, log, tail, next_poll)

      {^port, {:exit_status, status}} ->
        {status, poll(st, phase), tail}
    after
      min(max(next_poll - now(), 0), 2_000) -> pump(st, phase, port, marker, log, tail, next_poll)
    end
  end

  defp kill_marked(marker, match \\ :exact) do
    needle = "SOAK_STEP=" <> marker

    for dir <- Path.wildcard("/proc/[0-9]*"),
        {:ok, environ} <- [File.read(Path.join(dir, "environ"))],
        vars = String.split(environ, <<0>>),
        if(match == :exact,
          do: needle in vars,
          else: Enum.any?(vars, &String.starts_with?(&1, needle))
        ) do
      System.cmd("kill", ["-KILL", Path.basename(dir)], stderr_to_stdout: true)
    end
  end

  defp drain(port) do
    receive do
      {^port, {:exit_status, _}} -> :ok
      {^port, _} -> drain(port)
    after
      10_000 -> :ok
    end
  end

  defp judge(st, _phase, :bucket_check, 0, output) do
    case Regex.run(~r/SOAK_LIST root="[^"]*" objects=(\d+) tops=(.*)/, output) do
      [_, n, tops] ->
        foreign =
          Regex.scan(~r/"([^"]*)"/, tops)
          |> Enum.map(&List.last/1)
          |> Enum.reject(&String.starts_with?(&1, "soak-"))

        say(st, "bucket holds #{n} objects; outside soak roots: #{inspect(foreign)}")

        if foreign == [] or st.target == "local",
          do: st,
          else: %{st | aborted: "bucket holds foreign objects #{inspect(foreign)}"}

      _ ->
        %{st | aborted: "bucket listing failed"}
    end
  end

  defp judge(st, _phase, :probe, status, output) do
    if status == 0 and output =~ "SOAK_PROBE ok",
      do: st,
      else: %{
        st
        | aborted:
            "provider probe failed: #{output |> String.split("\n") |> Enum.find("", &(&1 =~ "SOAK_PROBE"))}"
      }
  end

  defp judge(st, _phase, :torture, status, _) when status not in [0, :killed],
    do: %{st | aborted: st.aborted || "torture failed (oracle violation?): evidence in #{st.out}"}

  defp judge(st, phase, kind, status, _) do
    if status not in [0, :killed],
      do: say(st, "#{phase}/#{kind} exited with #{status} (see #{phase}.log)")

    st
  end

  defp stop_requests(st) do
    for {_, ph} <- st.phases, do: File.write!(Path.join(ph.meter, "STOP"), "")
  end

  ## Meter

  defp poll(st, phase) do
    st =
      Enum.reduce(st.phases, st, fn {name, ph}, st ->
        Path.wildcard(Path.join(ph.meter, "requests-*.log"))
        |> Enum.reduce(st, &read_log(&2, name, &1))
      end)

    st = trim_windows(st, phase)

    st =
      if st.last_progress == nil or now() - st.last_progress >= 60_000,
        do: progress(st, phase),
        else: st

    check(st, phase)
  end

  defp read_log(st, phase, file) do
    offset = Map.get(st.offsets, file, 0)
    {:ok, io} = File.open(file, [:read, :binary])
    {:ok, _} = :file.position(io, offset)
    data = IO.binread(io, :eof)
    File.close(io)

    case data do
      bin when is_binary(bin) and bin != "" ->
        text = Map.get(st.partial, file, "") <> bin
        {lines, rest} = split_complete(text)

        st = %{
          st
          | offsets: Map.put(st.offsets, file, offset + byte_size(bin)),
            partial: Map.put(st.partial, file, rest)
        }

        Enum.reduce(lines, st, &count_line(&2, phase, &1))

      _ ->
        st
    end
  end

  defp split_complete(text) do
    parts = String.split(text, "\n")
    {Enum.drop(parts, -1), List.last(parts)}
  end

  defp count_line(st, phase, line) do
    update_in(st, [:phases, phase], fn ph ->
      case String.split(line, " ") do
        ["R", ms, op, _class] ->
          %{
            ph
            | counts: Map.update(ph.counts, op, 1, &(&1 + 1)),
              window: :queue.in(String.to_integer(ms), ph.window)
          }

        ["E", ms, _op | _] ->
          %{
            ph
            | errors: ph.errors + 1,
              err_window: :queue.in(String.to_integer(ms), ph.err_window)
          }

        _ ->
          ph
      end
    end)
  end

  defp trim_windows(st, phase) do
    cutoff = System.os_time(:millisecond) - @window_ms

    update_in(st, [:phases, phase], fn ph ->
      window = drop_before(ph.window, cutoff)

      %{
        ph
        | window: window,
          err_window: drop_before(ph.err_window, cutoff),
          max_window: max(ph.max_window, :queue.len(window))
      }
    end)
  end

  defp drop_before(q, cutoff) do
    case :queue.peek(q) do
      {:value, t} when t < cutoff -> drop_before(:queue.drop(q), cutoff)
      _ -> q
    end
  end

  defp check(%{aborted: reason} = st, _) when reason != nil, do: st

  defp check(st, phase) do
    ph = st.phases[phase]
    requests = :queue.len(ph.window)
    errors = :queue.len(ph.err_window)
    projected = get_in(st.projection, [phase, "max_requests_per_5min"])

    cond do
      phase == "cleanup" ->
        st

      cost(st) >= st.cap - @reserve ->
        %{st | aborted: "budget: $#{Float.round(cost(st), 4)} of $#{st.cap}"}

      projected && requests > 2 * projected ->
        %{st | aborted: "rate: #{requests} requests in 5 min, projected #{projected}"}

      requests >= 200 and errors / requests > 0.05 ->
        %{st | aborted: "errors: #{errors} of #{requests} requests in 5 min"}

      true ->
        st
    end
  end

  defp classes(counts) do
    Enum.reduce(counts, %{"A" => 0, "B" => 0, "free" => 0}, fn {op, n}, acc ->
      Map.update!(acc, class(op), &(&1 + n))
    end)
  end

  defp class(op) when op in ~w(GetObject HeadObject), do: "B"
  defp class(op) when op in ~w(DeleteObject DeleteObjects AbortMultipartUpload), do: "free"
  defp class(_), do: "A"

  defp phase_cost(ph),
    do: ph.counts |> classes() |> Enum.map(fn {c, n} -> n * @price[c] end) |> Enum.sum()

  defp cost(st), do: st.phases |> Map.values() |> Enum.map(&phase_cost/1) |> Enum.sum()

  defp progress(st, phase) do
    ph = st.phases[phase]
    c = classes(ph.counts)

    total =
      st.phases
      |> Map.values()
      |> Enum.map(& &1.counts)
      |> Enum.reduce(%{}, &Map.merge(&1, &2, fn _, a, b -> a + b end))
      |> classes()

    line =
      "#{DateTime.utc_now() |> DateTime.to_iso8601()} phase=#{phase} " <>
        "phase_A=#{c["A"]} phase_B=#{c["B"]} phase_free=#{c["free"]} " <>
        "total_A=#{total["A"]} total_B=#{total["B"]} cost=$#{Float.round(cost(st), 4)} cap=$#{st.cap} " <>
        "req_5min=#{:queue.len(ph.window)} err_5min=#{:queue.len(ph.err_window)}"

    File.write!(Path.join(st.out, "progress.log"), line <> "\n", [:append])
    %{st | last_progress: now()}
  end

  ## Report

  defp report(st) do
    phases =
      Map.new(st.order, fn name ->
        ph = st.phases[name]
        c = classes(ph.counts)

        {name,
         %{
           "minutes" => Float.round((ph.ended - ph.started) / 60_000, 1),
           "counts" => ph.counts,
           "A" => c["A"],
           "B" => c["B"],
           "free" => c["free"],
           "errors" => ph.errors,
           "cost_usd" => Float.round(phase_cost(ph), 5),
           "max_requests_per_5min" => ph.max_window,
           "steps" =>
             Enum.map(
               ph.steps,
               &%{"kind" => to_string(&1.kind), "cmd" => &1.cmd, "status" => to_string(&1.status)}
             )
         }}
      end)

    summary = %{
      "stamp" => st.stamp,
      "target" => st.target,
      "cap_usd" => st.cap,
      "cost_usd" => Float.round(cost(st), 5),
      "aborted" => Map.get(st, :abort_reason),
      "settings" => Map.new(st.settings, fn {k, v} -> {to_string(k), v} end),
      "phases" => phases
    }

    File.write!(Path.join(st.out, "summary.json"), JSON.encode!(summary))

    say(
      st,
      "done: $#{summary["cost_usd"]} (#{summary["aborted"] || "not aborted"}); #{st.out}/summary.json"
    )

    if summary["aborted"], do: System.halt(2)
  end

  ## Helpers

  defp now, do: System.monotonic_time(:millisecond)

  defp say(st, line) do
    line = "#{DateTime.utc_now() |> DateTime.to_iso8601()} #{line}"
    IO.puts(line)
    File.write!(Path.join(st.out, "progress.log"), line <> "\n", [:append])
  end

  defp precompile(st, bench?) do
    bench =
      if bench?,
        do: [
          {"bench", "mix", ~w(deps.get), [{"MIX_ENV", "prod"}]},
          {"bench", "mix", ~w(compile), [{"MIX_ENV", "prod"}]}
        ],
        else: []

    for {dir, cmd, args, env} <-
          [
            {".", "mix", ~w(compile), [{"MIX_ENV", "test"}]},
            {"native/sediment_nif", "cargo", ~w(test --release --no-run), []}
          ] ++ bench do
      {out, status} =
        System.cmd(cmd, args, cd: Path.join(@driver, dir), env: env, stderr_to_stdout: true)

      if status != 0,
        do: abort!("precompile #{cmd} in #{dir} failed:\n#{String.slice(out, -2000, 2000)}")
    end

    say(st, "precompiled (no S3 requests)")
  end

  defp abort!(msg) do
    IO.puts(:stderr, "s3_soak: " <> msg)
    System.halt(1)
  end
end

Soak.main(System.argv())
