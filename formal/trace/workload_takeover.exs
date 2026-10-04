# Sequential takeover across processes: writer A (a separate OS process) commits through a
# proxy that delays every request by up to 3 s, and is killed with kill -9 while a commit is
# in flight. The proxy lives here, in the parent, so the dead writer's buffered PUT still
# reaches S3 later. Writer B (another process, same owner) then opens directly, takes over,
# commits, waits for A's late PUT to land, and commits again. Finally the parent checks
# (outside the meter) whether A's unanswered frame landed after B's takeover: LATE_LANDED.
# Prints the prefix last. Run by formal/trace/run (the per-process logs are merged).
alias Sediment.TcpProxy

endpoint = System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
bucket = System.get_env("S3_TEST_BUCKET", "sediment-tests")
{:ok, proxy} = TcpProxy.start_link(URI.parse(endpoint).port, max_delay_ms: 3_000)
prefix = "trace/takeover-#{System.os_time()}"
dir = Path.join(System.tmp_dir!(), "s3trace-#{System.os_time()}")
File.mkdir_p!(dir)

writer = fn name, url, body ->
  script = Path.join(dir, "#{name}.exs")

  File.write!(script, """
  alias Sediment.Engine
  s3 = [bucket: #{inspect(bucket)}, prefix: #{inspect(prefix)}, endpoint: #{inspect(url)},
        region: "us-east-1", access_key_id: "any", secret_access_key: "any", encryption: false, owner: "w",
        durability: :sync, lease_ttl_ms: 60_000, request_timeout_ms: 20_000,
        checkpoint_threshold: -1]
  {:ok, db} = Engine.open(#{inspect(Path.join(dir, name <> ".db"))}, s3: s3)
  #{body}
  """)

  paths = Enum.flat_map(:code.get_path(), &["-pa", to_string(&1)])
  Port.open({:spawn_executable, System.find_executable("elixir")}, [
    :binary, :exit_status, :stderr_to_stdout, {:line, 1000}, args: paths ++ [script]
  ])
end

wait_for = fn port, text, timeout ->
  receive do
    {^port, {:data, {:eol, line}}} -> if line =~ text, do: :ok, else: :continue
    {^port, {:exit_status, s}} -> {:exit, s}
  after
    timeout -> :timeout
  end
end

wait_line = fn wait_line, port, text, timeout ->
  case wait_for.(port, text, timeout) do
    :continue -> wait_line.(wait_line, port, text, timeout)
    other -> other
  end
end

# Writer A: commits until it is killed.
a = writer.("a", "http://127.0.0.1:#{TcpProxy.port(proxy)}", """
_ = Engine.execute(db, "create table if not exists t (x integer)")
Enum.each(Stream.iterate(1, &(&1 + 1)), fn i ->
  IO.puts("BEGIN " <> to_string(i))
  :ok = Engine.execute(db, "insert into t values (" <> to_string(i) <> ")")
  IO.puts("ACK " <> to_string(i))
end)
""")

:ok = wait_line.(wait_line, a, "ACK 2", 120_000)
# Kill A the moment its own trace shows a frame PUT sent and not answered (the proxy
# holds it).
:ok = wait_line.(wait_line, a, "BEGIN 3", 120_000)
{:os_pid, a_pid} = Port.info(a, :os_pid)
a_log = Path.join(System.fetch_env!("SEDIMENT_S3_METER_DIR"), "requests-#{a_pid}.log")

in_flight? = fn ->
  lines = String.split(File.read!(a_log), "\n")
  answered = MapSet.new(for("A " <> rest <- lines, do: rest |> String.split(" ") |> Enum.at(1)))

  Enum.any?(
    for "T " <> rest <- lines,
        [_, id, "PutObject", path | _] <- [String.split(rest, " ")],
        String.contains?(path, "/log/"),
        do: not MapSet.member?(answered, id)
  )
end

Enum.find(1..1_000, fn _ -> in_flight?.() or (Process.sleep(10) && false) end)
System.cmd("kill", ["-9", to_string(a_pid)])
killed_at = System.os_time(:millisecond)

# Writer B: takes over directly, then waits long enough for A's delayed PUT to land.
b = writer.("b", endpoint, """
IO.puts("OPENED")
:ok = Engine.execute(db, "insert into t values (100)")
Process.sleep(6_000)
:ok = Engine.execute(db, "insert into t values (101)")
:ok = Engine.close(db)
IO.puts("DONE")
""")

:ok = wait_line.(wait_line, b, "OPENED", 120_000)
opened_at = System.os_time(:millisecond)
:ok = wait_line.(wait_line, b, "DONE", 120_000)

# Did A's last unanswered frame PUT land (SeaweedFS answers unsigned HEADs)?
meter = System.fetch_env!("SEDIMENT_S3_METER_DIR")

# Request ids are per process: find unanswered PUTs log by log.
unanswered =
  for log <- Path.wildcard(Path.join(meter, "requests-*.log")),
      lines = String.split(File.read!(log), "\n"),
      answered = MapSet.new(for("A " <> rest <- lines, do: rest |> String.split(" ") |> Enum.at(1))),
      "T " <> rest <- lines,
      [_, id, "PutObject", path | _] <- [String.split(rest, " ")],
      String.contains?(path, "/log/"),
      not MapSet.member?(answered, id),
      do: path

:inets.start()

for path <- unanswered do
  url = String.to_charlist("#{endpoint}#{path}")
  {:ok, {{_, status, _}, _, _}} = :httpc.request(:head, {url, []}, [], [])
  # B's open garbage-collects A's epoch after its takeover, so a frame of A's epoch still
  # there now landed after that (B's restore didn't find it either).
  # For the trace: an observation at the end that this object exists in S3.
  if status == 200, do: File.write!(Path.join(meter, "present.txt"), path <> "\n", [:append])

  # Not there: it never landed, or it found B's seal of A's epoch at its key.
  IO.puts(:stderr, "#{if status == 200, do: "LATE_LANDED", else: "NOT_LANDED"} #{path} " <>
    "(A killed at #{killed_at}, B took over by #{opened_at}; " <>
    if(status == 200,
      do: "B collected A's epoch at open, so it landed after the takeover)",
      else: "never landed, or refused by B's seal)"))
end

IO.puts(prefix)
