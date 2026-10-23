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

  # Options of the wrong shape, around a correct ring: a map, a tuple, a
  # key, a bare ring, a list that isn't a keyword list, a ring option at
  # the top level.
  defp misshapen(ring) do
    [
      {"map", %{encryption: ring}},
      {"tuple", {:encryption, ring}},
      {"a key", @k1},
      {"a bare ring", ring},
      {"a bare [cipher:, key:]", [cipher: "aegis256", key: @k1]},
      {"a list of entries", [{"r1", @k1}]},
      {"an improper list", [{:encryption, ring} | @k1]},
      {"keys: beside encryption:", [encryption: ring, keys: [{"r3", @k1}]]}
    ]
  end

  # S3 options holding a key: as an option's value, as an option's name
  # (a ring entry given as the options), as a map, a tuple or a bare ring.
  defp misshapen_s3 do
    s3 = Keyword.delete(s3(), :secret_access_key)

    [
      {"s3 encryption: key", s3 ++ [encryption: @k1]},
      {"s3 durability: key", s3 ++ [durability: @k1]},
      {"s3 must_exist: key", s3 ++ [must_exist: @k1]},
      {"s3 lease_ttl_ms: key", s3 ++ [lease_ttl_ms: @k1]},
      {"s3 endpoint: key", Keyword.put(s3, :endpoint, @k1)},
      {"s3 {key, id}", s3 ++ [{@k1, "r1"}]},
      {"s3 map", Map.new(s3 ++ [encryption: @k1])},
      {"s3 tuple", {:bucket, @k1}},
      {"s3 bare ring", ring()},
      {"s3 [encryption: ring]", s3 ++ [encryption: ring()]}
    ]
  end

  test "options of the wrong shape never show a key, at any entry point" do
    ring = ring()
    db_path = Temp.path!()
    {:ok, db} = Engine.open(db_path)

    # Every public function taking keys, with its options.
    entry_points = [
      {"Engine.open", &Engine.open(Temp.path!(), &1)},
      {"Connection.connect", &Sediment.Connection.connect(&1)},
      {"Sediment.start_link", &Sediment.start_link/1},
      {"Sediment.child_spec", &Sediment.child_spec/1},
      {"S3.refresh", &S3.refresh(db, &1)},
      {"S3.restore", &S3.restore(Temp.path!(), s3(), &1)},
      {"S3.import", &S3.import(db_path, s3(), &1)},
      {"S3.rekey", &S3.rekey(Temp.path!(), s3(), &1)},
      {"S3.release_key", &S3.release_key(Temp.path!(), s3(), "r1", &1)},
      {"S3.destroy", &S3.destroy(s3(), &1)},
      {"export_sqlite", &Sediment.export_sqlite(db_path, Temp.path!(), &1)}
    ]

    for {call, fun} <- entry_points, {case, opts} <- misshapen(ring) do
      {outcome, texts} = shown(fn -> fun.(opts) end)
      assert outcome != :ok, "#{call}, #{case}: accepted"
      for key <- [@k1, @k2], do: refute_key(texts, key, "#{call}, #{case}")
    end

    # `:key` beside `:encryption` is refused by the pool's connections
    # (connect/1), and the pool keeps it hidden meanwhile.
    {:error, _} =
      result = Sediment.Connection.connect(database: Temp.path!(), encryption: ring, key: @k1)

    refute_key([inspect(result, limit: :infinity)], @k1, "connect, key: beside encryption:")
    {:ok, pool} = Sediment.start_link(database: Temp.path!(), encryption: ring, key: @k1)
    pool |> pool_processes() |> Enum.each(&refute_in_process/1)
    GenServer.stop(pool)

    # Every function taking S3 options, with S3 options holding a key.
    s3_entry_points = [
      {"Engine.open", &Engine.open(Temp.path!(), s3: &1, encryption: ring)},
      {"Connection.connect",
       &Sediment.Connection.connect(database: Temp.path!(), s3: &1, encryption: ring)},
      {"S3.restore", &S3.restore(Temp.path!(), &1, encryption: ring)},
      {"S3.import", &S3.import(db_path, &1, encryption: ring)},
      {"S3.rekey", &S3.rekey(Temp.path!(), &1, encryption: ring)},
      {"S3.release_key", &S3.release_key(Temp.path!(), &1, "r1", encryption: ring)},
      {"S3.destroy", &S3.destroy(&1)},
      {"S3.exists?", &S3.exists?/1},
      {"S3.key_census", &S3.key_census/1},
      {"export_sqlite", &Sediment.export_sqlite(nil, Temp.path!(), from_s3: &1, encryption: ring)}
    ]

    for {call, fun} <- s3_entry_points, {case, s3} <- misshapen_s3() do
      {outcome, texts} = shown(fn -> fun.(s3) end)
      assert outcome != :ok, "#{call}, #{case}: accepted"
      for key <- [@k1, @k2], do: refute_key(texts, key, "#{call}, #{case}")
    end

    Engine.close(db)
  end

  test "options of the wrong shape are refused with a fixed message" do
    assert_raise ArgumentError, "Sediment.S3.rekey/3: the options must be a keyword list", fn ->
      S3.rekey(Temp.path!(), s3(), %{encryption: ring()})
    end

    assert_raise ArgumentError,
                 ~r"^Sediment.Engine.open/2: :keys is an option of the key ring",
                 fn -> Engine.open(Temp.path!(), ring()) end

    assert {:error, %Sediment.Error{message: "Sediment.Connection.connect/1: the options " <> _}} =
             Sediment.Connection.connect(%{database: Temp.path!()})

    # A function clause, a KeyError or a MatchError raised meanwhile holds
    # the value it failed on: only its type is shown.
    assert_raise ArgumentError, ~r"^FunctionClauseError \(details withheld", fn ->
      S3.restore(Temp.path!(), s3(), at: @k1, encryption: ring())
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
