defmodule Sediment.KeyNetTest do
  # Every public function of the sealed modules (and the sealed callbacks
  # of Sediment.Connection), called with arguments of wrong shapes that hold
  # a synthetic key, in every position, the other arguments nil, the same
  # shape or a valid fixture (a live pool, a callback, a Basic connection, a
  # database reference): the key never shows in a result, an exception
  # (message, inspect, stack trace), an exit, a throw, the log or a
  # telemetry event, and a connection state given in comes back unchanged.
  # Not async: it works in a directory of its own (a key given as a path
  # creates files there).
  use ExUnit.Case, async: false

  alias Sediment.Connection

  @key String.duplicate("7c", 32)
  @k128 String.duplicate("6e", 16)

  @modules [
    Sediment,
    Sediment.Engine,
    Sediment.S3,
    Sediment.Basic,
    Sediment.Pragma,
    Sediment.CDC
  ]

  # Of Sediment.Connection, the documented functions and the sealed
  # callbacks (the others are DBConnection's to call, with its arguments).
  @connection [handle_begin: 2, handle_close: 3]

  # Functions with no argument that could carry a key.
  @no_arguments [{Sediment.S3, :generate_key, 0}, {Sediment.S3, :request_counts, 0}]

  # Functions whose successful result is made of their arguments: given a
  # key as a path, Basic.open/1 opens a file of that name and names it;
  # child_spec/1 returns the pool's options (the caller's choice), with
  # `:encryption` and `:key` hidden; two internal helpers return the S3
  # options and the path they are given. Their errors are checked like any.
  @echoing [
    {Sediment.Basic, :open, 1},
    {Sediment, :child_spec, 1},
    {Sediment.S3, :with_encryption_choice, 2},
    {Sediment.S3, :generation_key, 1}
  ]

  # Telemetry metadata that is the caller's own by design: the SQL and
  # parameters of a statement, the database and the paths of an operation.
  @echoed_metadata [:query, :params, :database, :path, :source, :dest]

  @events (for(
             kind <- [:query, :prepare, :export],
             stage <- [:start, :stop, :exception],
             do: [:sediment, kind, stage]
           ) ++
             for(
               op <- [
                 :refresh,
                 :snapshot,
                 :restore,
                 :import,
                 :destroy,
                 :flush,
                 :rekey,
                 :release_key
               ],
               stage <- [:start, :stop, :exception],
               do: [:sediment, :s3, op, stage]
             )) ++ [[:sediment, :connection, :disconnect]]

  # A Basic connection's path and a statement it has cached hold runs of 32
  # hex digits (not keys): the net must leave the state as it is.
  @basic_dir String.duplicate("ab", 16)
  @cached "SELECT x'#{String.duplicate("cd", 16)}'"

  defp ring(key), do: [keys: [{"r1", key}], cipher: "aegis256"]

  # A caller's struct holding a ring: in a callback's reason or exception,
  # and as a target.
  defmodule Held do
    @moduledoc false
    defstruct [:ring]
  end

  defmodule StructTarget do
    @moduledoc false
    defstruct [:id, :path, :s3, :encryption]
  end

  # Wrong shapes, each holding `key`.
  defp shapes(key) do
    [
      key,
      String.to_atom(key),
      %{encryption: ring(key)},
      ring(key),
      [encryption: ring(key)],
      [encryption: [keys: [{"r1", [key: key]}]]],
      [{key, "r1"}],
      [cipher: key, key: "aegis256"],
      {:global, ring(key)},
      {:encryption, ring(key)},
      %{bucket: "b", encryption: key},
      [bucket: "b", encryption: key],
      [mode: String.to_atom(key)],
      [mode: [:readwrite, String.to_atom(key)]],
      [timeout: ring(key)],
      [pool: String.to_atom(key)],
      [pool_size: key]
    ]
  end

  # The fixtures an argument not probed can be, by its type in the
  # function's spec.
  @kinds %{
    {DBConnection, :conn} => [{:fixture, :pool}],
    {Sediment.Connection, :t} => [{:fixture, :basic}, {:fixture, :failed}],
    {Sediment.Engine, :db} => [{:fixture, :db}]
  }

  # The sealed callbacks have no spec of their own (DBConnection's): their
  # state is the last argument.
  @callback_kinds %{
    {:handle_begin, 2} => [[], [{:fixture, :basic}, {:fixture, :failed}]],
    {:handle_close, 3} => [[], [], [{:fixture, :basic}, {:fixture, :failed}]]
  }

  defp functions do
    for module <- @modules,
        {name, arity} <- module.__info__(:functions),
        not String.starts_with?(Atom.to_string(name), "__"),
        do: {module, name, arity}
  end

  defp connection_functions do
    {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(Connection)

    documented =
      for {{:function, name, arity}, _, _, doc, _} <- docs, doc != :hidden, do: {name, arity}

    for {name, arity} <- Enum.uniq(documented ++ @connection), do: {Connection, name, arity}
  end

  # For each position and each shape there, the other positions nil, the
  # same shape or a fixture of their type, in every combination (`:typed`),
  # or all nil or all the same shape.
  defp calls({_module, _name, arity} = function, key, :typed) do
    kinds = kinds(function)

    for position <- 0..(arity - 1),
        shape <- shapes(key),
        args <- fills(kinds, 0, position, shape),
        do: args
  end

  defp calls({_module, _name, arity}, key, :simple) do
    for position <- 0..(arity - 1),
        shape <- shapes(key),
        fill <- [nil, shape],
        uniq: true,
        do: List.duplicate(fill, arity) |> List.replace_at(position, shape)
  end

  defp fills([], _q, _position, _shape), do: [[]]

  defp fills([fixtures | rest], q, position, shape) do
    choices = if q == position, do: [shape], else: [nil, shape | fixtures]
    for choice <- choices, tail <- fills(rest, q + 1, position, shape), do: [choice | tail]
  end

  # The fixtures each argument can be, from the function's spec (a head
  # for default arguments has the leading arguments of a longer one's).
  defp kinds({Connection, name, arity}) when is_map_key(@callback_kinds, {name, arity}),
    do: @callback_kinds[{name, arity}]

  defp kinds({module, name, arity}) do
    {:ok, specs} = Code.Typespec.fetch_specs(module)

    spec =
      specs
      |> Enum.filter(fn {{n, a}, _} -> n == name and a >= arity end)
      |> Enum.min_by(fn {{_n, a}, _} -> a end, fn -> nil end)

    case spec do
      {{n, _a}, [spec | _]} ->
        n
        |> Code.Typespec.spec_to_quoted(spec)
        |> spec_args()
        |> Enum.take(arity)
        |> Enum.map(&arg_kinds/1)

      nil ->
        List.duplicate([], arity)
    end
  end

  defp spec_args({:when, _, [spec, _vars]}), do: spec_args(spec)
  defp spec_args({:"::", _, [{_name, _, args}, _result]}), do: args

  defp arg_kinds(type) do
    {_type, kinds} =
      Macro.prewalk(type, [], fn
        {{:., _, [module, name]}, _, []} = node, acc ->
          {node, acc ++ Map.get(@kinds, {module, name}, [])}

        {:->, _, _} = node, acc ->
          {node, acc ++ [{:fixture, :callback}]}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(kinds)
  end

  defp fixture({:fixture, :pool}, _dir) do
    {:ok, pool} = Sediment.start_link(database: ":memory:", pool_size: 1)
    pool
  end

  defp fixture({:fixture, :callback}, _dir), do: fn _ -> :ok end

  defp fixture({:fixture, :basic}, dir) do
    {:ok, conn} = Sediment.Basic.open(Path.join([dir, @basic_dir, "basic.db"]))
    {:ok, _, _, conn} = Sediment.Basic.exec(conn, @cached)
    conn
  end

  # A refused begin returns {:error, state}.
  defp fixture({:fixture, :failed}, dir) do
    {:ok, conn} = Sediment.Basic.open(Path.join([dir, @basic_dir, "failed.db"]))

    {:ok, _, _, conn} =
      Sediment.Basic.exec(conn, "create table if not exists t (id integer primary key)")

    {:ok, _, _, conn} = Sediment.Basic.exec(conn, "insert or ignore into t values (1)")
    {:ok, _, _, conn} = Sediment.Basic.exec(conn, @cached)
    {:ok, _, conn} = Connection.handle_begin([], conn)
    {:error, _, conn} = Sediment.Basic.exec(conn, "insert or rollback into t values (1)")
    %{transaction_status: :error} = conn
  end

  defp fixture({:fixture, :db}, _dir) do
    {:ok, db} = Sediment.Engine.open(":memory:")
    db
  end

  defp fixture(arg, _dir), do: arg

  defp release(pool) when is_pid(pool), do: if(Process.alive?(pool), do: GenServer.stop(pool))
  defp release(%Connection{} = conn), do: Sediment.Engine.close(conn.db)
  defp release(db) when is_reference(db), do: Sediment.Engine.close(db)
  defp release(_arg), do: :ok

  # Everything a call shows, and the connection states it returned.
  defp outcome(module, name, args) do
    result = apply(module, name, args)
    cleanup(result)

    texts =
      if {module, name, length(args)} in @echoing and not match?({:error, _}, result),
        do: [],
        else: [inspect(result, limit: :infinity)]

    {texts, states(result)}
  rescue
    exception ->
      {[
         Exception.format(:error, exception, __STACKTRACE__),
         inspect(exception, limit: :infinity),
         inspect(__STACKTRACE__, limit: :infinity)
       ], []}
  catch
    kind, reason ->
      {[Exception.format(kind, reason, __STACKTRACE__), inspect(reason, limit: :infinity)], []}
  end

  defp states(result) when is_tuple(result) and tuple_size(result) > 1 do
    case elem(result, tuple_size(result) - 1) do
      %Connection{} = state -> [state]
      _other -> []
    end
  end

  defp states(_result), do: []

  defp cleanup({:ok, db}) when is_reference(db), do: Sediment.Engine.close(db)
  defp cleanup({:ok, pid}) when is_pid(pid), do: Supervisor.stop(pid)
  defp cleanup({:ok, %Connection{} = state}), do: Connection.disconnect(:normal, state)
  defp cleanup(_result), do: :ok

  defp windows(key), do: for(i <- 0..(byte_size(key) - 16), do: binary_part(key, i, 16))

  defp shows?(text, key), do: Enum.any?(windows(key), &String.contains?(text, &1))

  # The telemetry events the call emitted, without the caller's own values.
  defp events do
    receive do
      {:telemetry, event, measurements, metadata} ->
        [inspect({event, measurements, Map.drop(metadata, @echoed_metadata)}, limit: :infinity)] ++
          events()
    after
      0 -> []
    end
  end

  defp call(function, args, key, dir) do
    {module, name, _arity} = function
    args = Enum.map(args, &fixture(&1, dir))
    {texts, states} = outcome(module, name, args)
    texts = texts ++ events()
    Enum.each(args, &release/1)
    drain_exits()

    for text <- texts, shows?(text, key), do: send(self(), {:leak, function, args, text})

    # A Basic connection given in comes back with its path and its cache.
    for %Connection{path: path} = given <- args,
        state <- states,
        state.path != path or not Map.has_key?(state.stmt_cache, @cached),
        do: send(self(), {:state_changed, function, Enum.map(args, &shape_of/1), given, state})
  end

  defp drain_exits do
    receive do
      {:EXIT, _pid, _reason} -> drain_exits()
    after
      0 -> :ok
    end
  end

  @tag timeout: 1_800_000
  test "no public function shows a key given in any argument, in any shape" do
    dir = Temp.path!()
    File.mkdir_p!(Path.join(dir, @basic_dir))
    functions = functions() ++ connection_functions()
    assert {Sediment.Engine, :open, 2} in functions
    assert {Sediment.S3, :refresh, 2} in functions
    assert {Sediment.S3, :flush_commit, 2} in functions
    assert {Connection, :handle_begin, 2} in functions

    test = self()
    handler = {__MODULE__, test}

    :telemetry.attach_many(
      handler,
      @events,
      &__MODULE__.forward/4,
      test
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    Process.flag(:trap_exit, true)

    {failures, log} =
      File.cd!(dir, fn ->
        ExUnit.CaptureLog.with_log(fn ->
          for function <- functions,
              function not in @no_arguments,
              {key, fills} <- [{@key, :typed}, {@k128, :simple}],
              args <- calls(function, key, fills),
              do: call(function, args, key, dir)

          receive_failures([])
        end)
      end)

    assert failures == [],
           failures |> Enum.uniq_by(&Tuple.delete_at(&1, 2)) |> Enum.map_join("\n\n", &inspect/1)

    for key <- [@key, @k128], window <- windows(key) do
      refute String.contains?(log, window), "the log shows a key:\n#{String.slice(log, 0, 2000)}"
    end

    # Every function of the modules is either called or listed as taking
    # no argument.
    for {m, f, 0} <- functions, do: assert({m, f, 0} in @no_arguments)
  end

  # The functions over many databases with a valid target (whose store
  # can't be reached) and a key in each option, and in the target: no key
  # in the result, a reason, an exception re-raised, or a callback's
  # arguments (the target given back with its :encryption hidden).
  test "the functions over many databases never show a key" do
    dir = Temp.path!()
    File.mkdir_p!(dir)

    s3 = [
      bucket: "b",
      prefix: "p",
      endpoint: "http://127.0.0.1:1",
      region: "us-east-1",
      access_key_id: "a",
      secret_access_key: "s"
    ]

    test = self()
    on_result = fn target, result -> send(test, {:callback, target, result}) end
    seen = fn target -> send(test, {:target, target}) end
    target = %{id: "a", path: Path.join(dir, "a.db"), s3: s3}

    key = @key

    for shape <- shapes(key),
        # The target is given back as given but its :encryption: one whose
        # S3 options are a key ring itself isn't checked. The callbacks'
        # failures for two of the shapes (each call takes half a second).
        {targets, opts, given?} <-
          [
            {[target], [encryption: shape], true},
            {[target], [encryption: ring(key), to: shape], true},
            {[target], [encryption: fn _ -> shape end], true},
            {[%{target | s3: s3 ++ [encryption: shape]}], [encryption: ring(key)], true},
            {[Map.put(target, :encryption, shape)], [encryption: fn t -> t.encryption end], true},
            {[{target.path, s3 ++ [encryption: shape]}], [encryption: ring(key)], true},
            {[struct(StructTarget, Map.put(target, :encryption, shape))],
             [encryption: fn t -> t.encryption end], true},
            {[%{target | s3: shape}], [encryption: ring(key)], false},
            {[target],
             [
               encryption: ring(key),
               check: fn _ -> {:error, :refused} end,
               max_concurrency: shape
             ], true}
          ] ++
            if(shape in [key, ring(key)], do: callback_cases(target, key, shape, seen), else: []),
        call <- [
          fn -> Sediment.S3.rekey_many(targets, opts ++ [attempts: 1, on_result: on_result]) end,
          fn ->
            Sediment.S3.release_key_many(
              targets,
              "r1",
              opts ++ [attempts: 1, on_result: on_result]
            )
          end,
          fn ->
            Sediment.S3.key_census_many(
              targets,
              Keyword.take(opts ++ [on_result: on_result], [:on_result])
            )
          end
        ] do
      texts =
        try do
          [inspect(call.(), limit: :infinity)]
        rescue
          e ->
            [
              Exception.format(:error, e, __STACKTRACE__),
              inspect(e, limit: :infinity),
              inspect(__STACKTRACE__, limit: :infinity)
            ]
        catch
          kind, reason ->
            [Exception.format(kind, reason, __STACKTRACE__), inspect(reason, limit: :infinity)]
        end

      messages = flush()

      callbacks =
        for {:callback, target, result} <- messages,
            do: inspect(if(given?, do: {target, result}, else: result), limit: :infinity)

      targets = for {:target, target} <- messages, given?, do: inspect(target, limit: :infinity)

      for text <- texts ++ callbacks ++ targets do
        refute shows?(text, key), String.slice(text, 0, 300)
      end
    end
  end

  # The target a callback gets back holds its ring hidden, usable as one.
  test "the functions over many databases give the callbacks the ring hidden" do
    ring = ring(@key)
    test = self()

    Sediment.S3.rekey_many(
      [%{id: "a", path: Temp.path!(), s3: [bucket: "b"], encryption: ring}],
      encryption: fn target -> target.encryption end,
      check: fn target ->
        send(test, {:target, target})
        {:error, :checked}
      end
    )

    assert_received {:target, %{encryption: %Sediment.S3.Keys.Hidden{} = hidden}}
    assert Sediment.S3.Keys.normalize(hidden) == Sediment.S3.Keys.normalize(ring)

    # A struct target: its type kept, its ring hidden.
    Sediment.S3.rekey_many(
      [%StructTarget{id: "a", path: Temp.path!(), s3: [bucket: "b"], encryption: ring}],
      encryption: fn target -> target.encryption end,
      check: fn target ->
        send(test, {:target, target})
        {:error, :checked}
      end
    )

    assert_received {:target, %StructTarget{encryption: %Sediment.S3.Keys.Hidden{}}}
  end

  # A reason's struct comes back of its type, its fields through the net.
  test "a callback's struct reason keeps its type, its key redacted" do
    assert %{failed: [{"a", {%Held{ring: [keys: [{"r1", "<redacted>"}], cipher: _]}, _}}]} =
             Sediment.S3.rekey_many(
               [%{id: "a", path: Temp.path!(), s3: [bucket: "b"]}],
               encryption: ring(@key),
               check: fn _ -> {:error, {%Held{ring: ring(@key)}, :refused}} end
             )
  end

  # The callbacks' own failures, each holding `shape`; check: and around:
  # sending the target they get to `seen`.
  defp callback_cases(target, key, shape, seen) do
    [
      {[target], [encryption: ring(key), around: fn _, _ -> raise inspect(shape) end], true},
      {[target], [encryption: ring(key), around: fn _, _ -> {:error, {:refused, shape}} end],
       true},
      {[target],
       [
         encryption: ring(key),
         around: fn t, run ->
           seen.(t)
           run.()
           {:error, {:reopen_failed, shape}}
         end
       ], true},
      {[target],
       [
         encryption: ring(key),
         check: fn t ->
           seen.(t)
           {:error, {:preflight, shape}}
         end
       ], true},
      {[target], [encryption: ring(key), on_result: fn _, _ -> raise inspect(shape) end], true},
      {[target], [encryption: ring(key), on_result: fn _, _ -> throw({:progress, shape}) end],
       true},
      {[target], [encryption: ring(key), stop_on: fn _, _ -> raise inspect(shape) end], true},
      {[target], [encryption: ring(key), stop_on: fn _, _ -> exit({:stop, shape}) end], true},
      # Structs, their fields walked.
      {[target], [encryption: ring(key), check: fn _ -> {:error, %Held{ring: shape}} end], true},
      {[target], [encryption: ring(key), around: fn _, _ -> {:error, %Held{ring: shape}} end],
       true},
      {[target],
       [
         encryption: ring(key),
         around: fn _, run ->
           run.()
           {:error, %Held{ring: shape}}
         end
       ], true},
      {[target],
       [
         encryption: ring(key),
         on_result: fn _, _ -> raise MatchError, term: %Held{ring: shape} end
       ], true},
      {[target],
       [
         encryption: ring(key),
         stop_on: fn _, _ -> raise MatchError, term: %Held{ring: shape} end
       ], true}
    ]
  end

  defp flush do
    receive do
      message -> [message | flush()]
    after
      0 -> []
    end
  end

  @doc false
  def forward(event, measurements, metadata, test),
    do: send(test, {:telemetry, event, measurements, metadata})

  defp receive_failures(acc) do
    receive do
      {:leak, function, args, text} ->
        # The arguments themselves hold the key: name them by shape only.
        failure = {:leak, function, Enum.map(args, &shape_of/1), String.slice(text, 0, 300)}
        receive_failures([failure | acc])

      {:state_changed, function, shapes, given, state} ->
        failure =
          {:state_changed, function, shapes, {given.path, Map.keys(given.stmt_cache)},
           {state.path, Map.keys(state.stmt_cache)}}

        receive_failures([failure | acc])

      {:EXIT, _pid, _reason} ->
        receive_failures(acc)
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp shape_of(arg) when is_binary(arg), do: "a string"
  defp shape_of(arg) when is_atom(arg), do: "an atom"
  defp shape_of(%Connection{}), do: "a Basic connection"
  defp shape_of(arg) when is_map(arg), do: "a map"
  defp shape_of(arg) when is_tuple(arg), do: "a tuple"
  defp shape_of(arg) when is_list(arg), do: "a list"
  defp shape_of(arg) when is_pid(arg), do: "a pool"
  defp shape_of(arg) when is_function(arg), do: "a callback"
  defp shape_of(arg) when is_reference(arg), do: "a database"
end
