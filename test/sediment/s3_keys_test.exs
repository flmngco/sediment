defmodule Sediment.S3KeysTest do
  # Key rings without a server: option forms, and no key in an exception.
  use ExUnit.Case, async: true

  alias Sediment.Engine
  alias Sediment.S3
  alias Sediment.S3.Keys

  @k1 String.duplicate("1a", 32)
  @k2 String.duplicate("2b", 32)
  @secret "S3-SECRET-ACCESS-KEY-VALUE"

  defp ring, do: [keys: [{"r2", @k2}, {"r1", @k1}], cipher: "aegis256"]

  defp s3 do
    [
      bucket: "b",
      prefix: "p",
      endpoint: "http://127.0.0.1:1",
      access_key_id: "id",
      secret_access_key: @secret
    ]
  end

  # The exception, its message and every frame of its stack trace.
  defp raised(fun) do
    fun.()
    flunk("expected an exception")
  rescue
    exception -> {exception, Exception.format(:error, exception, __STACKTRACE__), __STACKTRACE__}
  end

  defp refute_secrets({exception, formatted, stacktrace}) do
    for secret <- [@k1, @k2, String.slice(@k1, 0, 16), @secret] do
      refute formatted =~ secret
      refute inspect(stacktrace, limit: :infinity) =~ secret
      refute inspect(exception, limit: :infinity) =~ secret
    end

    exception
  end

  test "the option forms" do
    assert %{keys: [{"default", "aegis256", @k1}], active: nil, key_for: nil} =
             Keys.normalize(cipher: "aegis256", key: @k1)

    assert %{keys: [{"r2", "aegis256", @k2}, {"r1", "aes256gcm", @k1}]} =
             Keys.normalize(keys: [{"r2", @k2}, {"r1", @k1, cipher: "aes256gcm"}])

    fun = fn _ -> nil end
    assert %{keys: [], active: "r2", key_for: ^fun} = Keys.normalize(key_for: fun, active: "r2")
    assert Keys.normalize(false) == false
    assert Keys.normalize(nil) == nil

    assert %{resolvable: true, active: "r2"} =
             Keys.native(Keys.normalize(key_for: fun, active: "r2"))
  end

  test "a bad option is refused before the NIF, and no key or secret shows" do
    for {call, fun} <- [
          rekey: fn -> S3.rekey("/tmp/x.db", s3(), encryption: ring(), force: "true") end,
          reuse: fn -> S3.rekey("/tmp/x.db", s3(), encryption: ring(), allow_reuse: nil) end,
          release: fn -> S3.release_key("/tmp/x.db", s3(), "r1", encryption: ring(), force: 1) end,
          destroy: fn -> S3.destroy(s3(), force: "yes") end,
          bad_entry: fn -> Engine.open("/tmp/x.db", encryption: [keys: [{"r1", @k1, :aes}]]) end
        ] do
      assert %ArgumentError{} = fun |> raised() |> refute_secrets(), "#{call}"
    end
  end

  test "a NIF's badarg is raised without its arguments" do
    # A path the NIF can't decode: badarg, with the ring in the top frame
    # unless it is scrubbed.
    {exception, _formatted, stacktrace} =
      raised = raised(fn -> Engine.open(123, encryption: ring(), s3: s3()) end)

    refute_secrets(raised)
    assert %ArgumentError{} = exception
    assert [{Sediment.Native, :open, 2, _} | _] = stacktrace
  end

  # Every way a process's state is printed: inspect and Erlang's ~p, of the
  # status (:sys.get_status/1) and of the state a termination report shows.
  # The path of the database the process has open, if it is a connection.
  defp refute_in_process(pid) do
    status = :sys.get_status(pid)
    state = :sys.get_state(pid)

    {terminate, path} =
      case state do
        {_state, %{mod: _, state: %Sediment.Connection{path: path}} = data} ->
          {[DBConnection.Connection.format_status(:terminate, [[], :no_state, data])], path}

        _ ->
          {[], nil}
      end

    for term <- [status | terminate],
        text <- [inspect(term, limit: :infinity), to_string(:io_lib.format(~c"~tp", [term]))] do
      refute text =~ @k1
      refute text =~ @k2
    end

    path
  catch
    # Another test's pool, stopping meanwhile.
    :exit, _ -> nil
  end

  # The pool, and every pool's connection supervisors (which keep their
  # child specs) and connections, started under DBConnection's application
  # supervisor; other tests' pools may stop meanwhile.
  defp pool_processes(pool) do
    supervisors =
      for {_, pid, _, _} <- Supervisor.which_children(DBConnection.ConnectionPool.Supervisor),
          is_pid(pid),
          do: pid

    connections =
      for supervisor <- supervisors,
          {_, pid, _, _} <- children(supervisor),
          is_pid(pid),
          do: pid

    [pool | supervisors] ++ connections
  end

  defp children(supervisor) do
    Supervisor.which_children(supervisor)
  catch
    :exit, _ -> []
  end

  test "a connection's state and a pool's processes never show a key" do
    path = Temp.path!()

    {:ok, state} = Sediment.Connection.connect(database: path, encryption: ring())

    for text <- [inspect(state, limit: :infinity), to_string(:io_lib.format(~c"~tp", [state]))] do
      refute text =~ @k1
      refute text =~ @k2
    end

    :ok = Sediment.Connection.disconnect(%RuntimeError{}, state)

    {:ok, pool} = Sediment.start_link(database: path, encryption: ring(), pool_size: 2)
    assert {:ok, _} = Sediment.query(pool, "SELECT 1", [])
    paths = pool |> pool_processes() |> Enum.map(&refute_in_process/1)
    # Both of this pool's connections were looked at.
    assert Enum.count(paths, &(&1 == path)) == 2
    GenServer.stop(pool)
  end

  @k128 String.duplicate("4d", 16)
  @uuid String.duplicate("5e", 16)

  # Every way the reviews found a mistyped :encryption to show a key: the
  # option, and the key it must never show.
  defp mistyped do
    [
      {"{key, r2}, aegis256", [keys: [{@k1, "r2"}], cipher: "aegis256"], @k1},
      {"{key, r2}, aes128gcm", [keys: [{@k128, "r2"}], cipher: "aes128gcm"], @k128},
      {"{key, uuid}, aes128gcm", [keys: [{@k128, @uuid}], cipher: "aes128gcm", active: @uuid],
       @k128},
      {"{key, uuid}, aegis256", [keys: [{@k1, @uuid}], cipher: "aegis256"], @k1},
      {"active: key", [keys: [{"r1", @k128}], cipher: "aes128gcm", active: @k128], @k128},
      {"cipher: key, key: cipher", [cipher: @k1, key: "aegis256"], @k1},
      {"nested [key: k] entry", [keys: [{"r1", [key: @k1]}]], @k1},
      {"atom-keyed entry", [keys: [{"r1", String.to_atom(@k1)}]], @k1},
      {"raising key_for MFA", [key_for: {:maps, :get, [%{"r1" => @k1}]}, active: "r2"], @k1}
    ]
  end

  # What a call shows: its result, or its exception (message, inspect,
  # stack trace); a returned exception as DBConnection logs it.
  defp shown(fun) do
    case fun.() do
      {:ok, _} = ok ->
        {:ok, [inspect(ok)]}

      {:error, %{__exception__: true} = error} = result ->
        {:error, [Exception.format_banner(:error, error), inspect(result, limit: :infinity)]}

      result ->
        {:error, [inspect(result, limit: :infinity)]}
    end
  rescue
    exception ->
      {:raised,
       [
         Exception.format(:error, exception, __STACKTRACE__),
         inspect(exception, limit: :infinity),
         inspect(__STACKTRACE__, limit: :infinity)
       ]}
  end

  defp refute_key(texts, key, case) do
    for text <- texts, i <- 0..(byte_size(key) - 16) do
      refute text =~ binary_part(key, i, 16), "#{case}: #{text}"
    end
  end

  test "a mistyped :encryption never shows a key" do
    for {case, encryption, key} <- mistyped(),
        {call, fun} <- [
          open: fn -> Engine.open(Temp.path!(), encryption: encryption) end,
          connect: fn ->
            Sediment.Connection.connect(database: Temp.path!(), encryption: encryption)
          end
        ] do
      {outcome, texts} = shown(fun)
      assert outcome != :ok, "#{case} (#{call}) opened"
      refute_key(texts, key, "#{case} (#{call})")
    end

    # A key given as a rekey's :to or a release's key id, as a string or not.
    ring = [keys: [{"r1", @k128}], cipher: "aes128gcm"]

    for {case, key, fun} <- [
          {"to: key", @k128, fn -> S3.rekey(Temp.path!(), s3(), encryption: ring, to: @k128) end},
          {"to: key256", @k1, fn -> S3.rekey(Temp.path!(), s3(), encryption: ring, to: @k1) end},
          {"release key", @k128,
           fn -> S3.release_key(Temp.path!(), s3(), @k128, encryption: ring) end},
          {"release [key: k]", @k1,
           fn -> S3.release_key(Temp.path!(), s3(), [key: @k1], encryption: ring) end}
        ] do
      {outcome, texts} = shown(fun)
      assert outcome != :ok, "#{case} succeeded"
      refute_key(texts, key, case)
    end
  end

  test "a pool logs a mistyped ring's failed connects without the key" do
    for {case, encryption, key} <- Enum.take(mistyped(), 3) do
      log =
        ExUnit.CaptureLog.capture_log([level: :error], fn ->
          {:ok, pool} = Sediment.start_link(database: Temp.path!(), encryption: encryption)
          Process.sleep(300)
          GenServer.stop(pool)
        end)

      assert log =~ "ring entry 1", case
      refute_key([log], key, case)
    end
  end

  test "an invalid key is named by its id, never shown" do
    for bad <- [String.slice(@k1, 0, 62), String.duplicate("zz", 32)] do
      assert {:error, message} =
               Engine.open(Temp.path!(), encryption: [keys: [{"r1", bad}]])

      assert message =~ "key r1"
      refute message =~ String.slice(bad, 0, 16)
    end
  end
end
