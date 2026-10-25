defmodule Sediment.S3.Many do
  @moduledoc false
  # Sediment.S3.rekey_many/2, release_key_many/3 and key_census_many/2: one
  # operation over many databases, a few at a time, never stopping at a
  # failure. See their docs in Sediment.S3.

  alias Sediment.S3
  alias Sediment.S3.Keys

  @crashed "its process was killed; its operation may still be running: wait until it ends " <>
             "(an open of its path no longer busy), then run again"

  @common [
    :encryption,
    :max_concurrency,
    :attempts,
    :backoff,
    :around,
    :check,
    :on_result,
    :stop_on
  ]

  @spec rekey_many(Enumerable.t(), keyword()) :: map()
  def rekey_many(targets, opts) do
    call = "Sediment.S3.rekey_many/2"

    {targets, opts} =
      Keys.scrubbed(fn ->
        checked!(targets, opts, call, @common ++ [:to, :force, :allow_reuse])
      end)

    pass = Keyword.take(opts, [:to, :force, :allow_reuse])
    rekey = fn target, ring -> S3.rekey(target.path, target.s3, [encryption: ring] ++ pass) end

    opts = Keyword.put(opts, :precheck, {:rekey, opts[:to]})
    run(targets, opts, rekey, &rekey_outcome/2, [:rekeyed, :current, :absent])
  end

  @spec release_key_many(Enumerable.t(), String.t(), keyword()) :: map()
  def release_key_many(targets, key_id, opts) do
    call = "Sediment.S3.release_key_many/3"

    {targets, opts} =
      Keys.scrubbed(fn ->
        unless (is_binary(key_id) or is_atom(key_id)) and key_id not in [nil, true, false],
          do: raise(ArgumentError, "#{call}: the key id must be a string")

        checked!(targets, opts, call, @common ++ [:force])
      end)

    pass = Keyword.take(opts, [:force])

    release = fn target, ring ->
      S3.release_key(target.path, target.s3, key_id, [encryption: ring] ++ pass)
    end

    opts = Keyword.put(opts, :precheck, {:release, key_id})
    run(targets, opts, release, &release_outcome/2, [:released, :unchanged, :absent])
  end

  @spec key_census_many(Enumerable.t(), keyword()) :: map()
  def key_census_many(targets, opts) do
    call = "Sediment.S3.key_census_many/2"

    {targets, opts} =
      Keys.scrubbed(fn ->
        opts = Keys.options!(opts, call)
        unknown_options!(opts, [:max_concurrency, :on_result], call)
        integer!(opts, :max_concurrency, 1, call)
        function!(opts, :on_result, 2, call)
        {targets!(targets, call, false), opts}
      end)

    census = fn target -> {target, S3.key_census(target.s3)} end

    results =
      collect(targets, Keyword.get(opts, :max_concurrency, 2), census, opts[:on_result], nil)

    # Not read, after a worker died: unreadable, never a sign of a free key.
    read = MapSet.new(results, fn {target, _} -> target.index end)
    results = results ++ for(t <- targets, t.index not in read, do: {t, {:error, :skipped}})

    acc = %{
      keys: %{},
      absent: [],
      unencrypted: [],
      unreadable: [],
      targets: ids(targets),
      fingerprints: %{}
    }

    results
    |> Enum.sort_by(fn {target, _} -> target.index end)
    |> Enum.reduce(acc, fn {target, result}, acc -> tally(acc, target.id, result) end)
    |> Map.update!(:keys, fn keys ->
      Map.new(keys, fn {key, by} ->
        {key, Map.new(by, fn {k, ids} -> {k, Enum.reverse(ids)} end)}
      end)
    end)
    |> Map.update!(:absent, &Enum.reverse/1)
    |> Map.update!(:unencrypted, &Enum.reverse/1)
    |> Map.update!(:unreadable, &Enum.reverse/1)
  end

  @spec removable?(map(), String.t(), keyword()) :: boolean()
  def removable?(census, key_id, opts \\ [])

  def removable?(%{keys: _, absent: _, unreadable: _, targets: _} = census, key_id, opts) do
    case Keyword.fetch(opts, :current) do
      :error ->
        free?(census, to_string(key_id))

      {:ok, current} ->
        free?(census, to_string(key_id)) and all_current?(census, to_string(current))
    end
  end

  def removable?(_census, _key_id, _opts),
    do:
      raise(
        ArgumentError,
        "Sediment.S3.removable?/3 expects the result of key_census_many/2"
      )

  # No target needs `id`, and every one was read: a target read as absent
  # may be a misaddressed one (a wrong bucket or prefix) whose real database
  # needs the key, and a census of no target at all proves nothing.
  defp free?(%{keys: keys, absent: absent, unreadable: unreadable, targets: targets}, id) do
    targets != [] and absent == [] and unreadable == [] and
      case keys do
        %{^id => %{current: [], retained: []}} -> true
        %{^id => _} -> false
        _ -> true
      end
  end

  # Every target read, under `current`.
  defp all_current?(%{targets: targets, keys: keys}, current),
    do: targets != [] and match?(%{^current => %{current: ^targets}}, keys)

  defp ids(targets), do: Enum.map(targets, & &1.id)

  ## Outcomes

  # The last result, and the attempts before it.
  defp rekey_outcome({:ok, :current}, _earlier), do: :current
  defp rekey_outcome({:ok, %{to: _}}, _earlier), do: :rekeyed
  defp rekey_outcome(result, _earlier), do: absent_or_failed(result)

  # A release that left objects behind released its epochs: the re-run
  # that deleted them has none left to release.
  defp release_outcome({:ok, %{epochs: []}}, earlier) do
    if Enum.any?(earlier, &match?({:error, {:leftover, %{epochs: [_ | _]}}}, &1)),
      do: :released,
      else: :unchanged
  end

  defp release_outcome({:ok, %{epochs: _}}, _earlier), do: :released
  defp release_outcome(result, _earlier), do: absent_or_failed(result)

  defp absent_or_failed({:error, "s3 config: no database at " <> _}), do: :absent
  defp absent_or_failed(_result), do: :failed

  # Only {:ok, _} is an outcome: after a non-ok result the operation may
  # have happened or may still happen; only a re-run resolves it. Those are
  # retried; a refusal (the database open, the lease held, a retired key, a
  # missing key) isn't.
  defp retry?({:error, {:fenced, _}}), do: true
  defp retry?({:error, {:indeterminate, _}}), do: true
  defp retry?({:error, "s3 store: " <> _}), do: true
  defp retry?({:error, "s3 conflict: " <> _}), do: true
  defp retry?({:error, "s3 writer fenced: " <> _}), do: true
  # Released, its objects not all deleted: a re-run deletes them.
  defp retry?({:error, {:leftover, _}}), do: true
  defp retry?(_result), do: false

  defp tally(acc, id, {:ok, :no_database}), do: Map.update!(acc, :absent, &[id | &1])

  defp tally(acc, id, {:ok, %{current: nil, retained: retained}}) when retained == %{},
    do: Map.update!(acc, :unencrypted, &[id | &1])

  defp tally(acc, id, {:ok, %{current: current, retained: retained} = census}) do
    acc =
      case census do
        %{fingerprints: fingerprints} when fingerprints != %{} ->
          put_in(acc, [:fingerprints, id], fingerprints)

        _ ->
          acc
      end

    acc =
      if current,
        do: add(acc, current, :current, id),
        else: Map.update!(acc, :unencrypted, &[id | &1])

    Enum.reduce(retained, acc, fn {key, _epochs}, acc -> add(acc, key, :retained, id) end)
  end

  defp tally(acc, id, {:error, reason}), do: Map.update!(acc, :unreadable, &[{id, reason} | &1])

  defp add(acc, key, kind, id) do
    update_in(acc, [:keys, Access.key(key, %{current: [], retained: []}), kind], &[id | &1])
  end

  ## Running

  # Each target in a task of its own (at most `max_concurrency` at once):
  # the caller's around:, check: and encryption function run there, the
  # operation with its retries; on_result: and stop_on: run in the caller,
  # in completion order. Returns the outcome lists in the targets' order.
  defp run(targets, opts, op, outcome, kinds) do
    one = fn target ->
      {result, earlier} = attempt(target, opts, op)
      {Map.put(target, :earlier, earlier), result}
    end

    stop_on = opts[:stop_on]
    stop? = stop_on && fn {target, result} -> stop_on.(target.id, result) end

    results =
      collect(targets, Keyword.get(opts, :max_concurrency, 1), one, opts[:on_result], stop?)

    done = MapSet.new(results, fn {target, _} -> target.index end)
    acc = Map.new([:failed | kinds], &{&1, []})

    acc =
      results
      |> Enum.sort_by(fn {target, _} -> target.index end)
      |> Enum.reduce(acc, fn {target, result}, acc ->
        case outcome.(result, Map.get(target, :earlier, [])) do
          :failed -> Map.update!(acc, :failed, &[{target.id, failure(target, result)} | &1])
          kind -> Map.update!(acc, kind, &[target.id | &1])
        end
      end)

    acc
    |> Map.new(fn {kind, ids} -> {kind, Enum.reverse(ids)} end)
    |> Map.put(:skipped, for(target <- targets, target.index not in done, do: target.id))
  end

  # The last result's reason; after retries every attempt's, in order (an
  # indeterminate result followed by a refusal reads as what it is: the
  # operation may have happened).
  defp failure(%{earlier: [_ | _] = earlier}, result),
    do: {:attempts, Enum.map(earlier ++ [result], &reason/1)}

  defp failure(_target, result), do: reason(result)

  defp reason({:error, reason}), do: reason
  defp reason(other), do: {:unexpected, other}

  # The caller's around: wraps all of the target's attempts. The result is
  # what the operation returned when around: ran it (unless around: then
  # failed, returning another {:error, reason} or raising: {:error,
  # {:around_failed, reason, result}}, a database maybe left closed), else
  # around:'s own {:error, reason} (a refusal of its own), else {:error,
  # :not_run}; with the results of the attempts before the last.
  defp attempt(target, opts, op) do
    with {:ok, ring} <- ring(target, opts[:encryption]),
         :proceed <- precheck(target, ring, opts[:precheck]) do
      # The ring resolved once, before around:.
      around(target, Keyword.put(opts, :encryption, ring), op)
    else
      {:done, result} -> {result, []}
      {:error, _} = failed -> {failed, []}
    end
  end

  defp around(target, opts, op) do
    case opts[:around] do
      nil ->
        attempts(target, opts, op)

      around ->
        key = {__MODULE__, :result}
        Process.delete(key)

        runner = fn ->
          {result, _earlier} = outcome = attempts(target, opts, op)
          Process.put(key, outcome)
          result
        end

        returned = callback(:around, fn -> around.(target.given, runner) end)

        case {Process.get(key), returned} do
          {nil, {:error, reason}} -> {{:error, Keys.diagnostic(reason)}, []}
          {nil, _other} -> {{:error, :not_run}, []}
          {{result, _earlier} = outcome, result} -> outcome
          {{result, earlier}, {:error, reason}} -> {around_failed(reason, result), earlier}
          {outcome, _returned} -> outcome
        end
    end
  end

  defp around_failed(reason, result),
    do: {:error, Keys.diagnostic({:around_failed, reason, result})}

  defp attempts(target, opts, op) do
    with :ok <- check(target, opts[:check]),
         {:ok, ring} <- ring(target, opts[:encryption]) do
      attempts = Keyword.get(opts, :attempts, 3)
      retrying(target, ring, op, {attempts, Keyword.get(opts, :backoff, 1_000)}, [])
    else
      refused -> {refused, []}
    end
  end

  defp check(_target, nil), do: :ok

  defp check(target, check) do
    case callback(:check, fn -> check.(target.given) end) do
      {:error, reason} -> {:error, Keys.diagnostic(reason)}
      _ok -> :ok
    end
  end

  defp ring(target, fun) when is_function(fun, 1) do
    case callback(:encryption, fn -> fun.(target.given) end) do
      {:error, {:callback_raised, _, _}} = raised -> raised
      ring -> {:ok, ring}
    end
  end

  defp ring(_target, ring), do: {:ok, ring}

  # Before around: (which may close the database), the manifest read as a
  # census reads it, without the lease: a target that needs nothing is
  # reported without around:, and one whose ring holds other key material
  # under a recorded id fails before it. Advisory: anything else, or a read
  # that fails, goes to the operation, which decides under the lease. A
  # stale read is safe: a key never becomes current again without
  # `allow_reuse` (A3), so a manifest naming the target key means the
  # rekey landed and no later state undoes it; and a release only removes
  # epochs, so a key no epoch is under stays so.
  # An id given as an atom, as the operation takes it; anything else goes
  # to the operation, which refuses it (by type, never by value).
  defp precheck(target, ring, {kind, id}) when is_atom(id) and id not in [nil, true, false],
    do: precheck(target, ring, {kind, Atom.to_string(id)})

  defp precheck(_target, _ring, {_kind, id}) when not is_binary(id) and id != nil,
    do: :proceed

  defp precheck(target, ring, {:rekey, to}) do
    with {:ok, %{current: current, fingerprints: recorded}} when is_binary(current) <-
           S3.key_census(target.s3),
         %{} = ring <- Keys.normalize(ring) do
      # As the native code: the ring's active key, else its first.
      to = to || ring.active || with({id, _, _} <- List.first(ring.keys), do: id)
      {ring, current_fp} = Keys.fingerprint(ring, current)
      {_ring, to_fp} = if to == current, do: {ring, current_fp}, else: Keys.fingerprint(ring, to)

      cond do
        current_fp != nil and current_fp != recorded[current] ->
          {:done, other_material(current, current_fp, recorded[current])}

        to != current and to_fp != nil and is_map_key(recorded, to) and to_fp != recorded[to] ->
          {:done, other_material(to, to_fp, recorded[to])}

        to == current and current_fp != nil ->
          {:done, {:ok, :current}}

        true ->
          :proceed
      end
    else
      _ -> :proceed
    end
  rescue
    _ -> :proceed
  end

  defp precheck(target, _ring, {:release, key_id}) do
    with {:ok, %{current: current, retained: retained}} <- S3.key_census(target.s3),
         true <- current != key_id and not is_map_key(retained, key_id),
         {:ok, %{objects: 0}} <- S3.orphans(target.s3) do
      {:done, {:ok, %{key: Keys.shown(key_id), epochs: []}}}
    else
      _ -> :proceed
    end
  rescue
    _ -> :proceed
  end

  defp precheck(_target, _ring, nil), do: :proceed

  defp other_material(id, ring_fp, recorded_fp),
    do:
      {:error,
       "s3 config: the ring's key #{Keys.shown(id)} (fingerprint #{ring_fp}) is not the key " <>
         "the database records under #{Keys.shown(id)} (fingerprint #{recorded_fp}): the " <>
         "ring holds other key material under that id"}

  # The last result, and those before it.
  defp retrying(target, ring, op, {attempts, backoff} = retries, earlier) do
    result = operation(fn -> op.(target, ring) end)
    n = length(earlier) + 1

    if retry?(result) and n < attempts do
      Process.sleep(backoff * Integer.pow(2, n - 1))
      retrying(target, ring, op, retries, earlier ++ [result])
    else
      {result, earlier}
    end
  end

  # The operation raises only past its own boundary, with a message that
  # passed it.
  defp operation(fun) do
    fun.()
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, _reason -> {:error, :exit}
  end

  # A caller's function failing is reported by its type only: its message
  # and stack frames may hold anything, keys included.
  defp callback(name, fun) do
    fun.()
  rescue
    exception -> {:error, {:callback_raised, name, exception.__struct__}}
  catch
    kind, _reason -> {:error, {:callback_raised, name, kind}}
  end

  # Runs `one` over the targets in workers, at most `max` at once; calls
  # on_result.(given, result) and stop?.({target, result}) in this process
  # as each finishes. Once stop? is true (or on_result or stop? raises, which
  # is re-raised once the workers running finish), no further target starts.
  #
  # The workers are monitored, never linked, and trap exits: a process
  # linked to one (a helper of around:'s) exiting abnormally can't stop it
  # mid-operation; the target fails with {:linked_exit, result} once its
  # operation and around: end. One that dies all the same (a kill) may
  # leave its native operation running, holding a dirty I/O scheduler, the
  # file and the lease, which its slot must not hand to the next target:
  # its target fails with {:crashed, why} and no further target starts. If
  # this process dies, the workers running still finish their target
  # (around: reopening its database), none further starting.
  defp collect(targets, max, one, on_result, stop?) do
    loop(targets, %{}, max, one, {on_result, stop?}, [], nil)
  end

  defp loop([target | rest], running, max, one, callbacks, done, nil)
       when map_size(running) < max do
    {pid, ref} = start(target, one)
    loop(rest, Map.put(running, pid, {ref, target}), max, one, callbacks, done, nil)
  end

  # The caller's exception, through the net, its frames without their
  # arguments (ours hold the targets).
  defp loop(_queue, running, _max, _one, _callbacks, _done, {kind, reason, stacktrace})
       when running == %{},
       do: :erlang.raise(kind, Keys.diagnostic(reason), Keys.scrub_stacktrace(stacktrace))

  defp loop(queue, running, _max, _one, _callbacks, done, stop)
       when running == %{} and (queue == [] or stop != nil),
       do: Enum.reverse(done)

  defp loop(queue, running, max, one, {on_result, stop?} = callbacks, done, stop) do
    {target, result, running, crashed} =
      receive do
        {__MODULE__, pid, {target, result}} when is_map_key(running, pid) ->
          {ref, _target} = Map.fetch!(running, pid)
          Process.demonitor(ref, [:flush])
          {target, result, Map.delete(running, pid), nil}

        # A worker that died (its own failures are results): its reason
        # isn't kept (it may hold anything).
        {:DOWN, _ref, :process, pid, _reason} when is_map_key(running, pid) ->
          {_ref, target} = Map.fetch!(running, pid)
          {target, {:error, {:crashed, @crashed}}, Map.delete(running, pid), :stop}
      end

    done = [{target, result} | done]
    stop = stop || after_result(target, result, on_result, stop?) || crashed
    loop(queue, running, max, one, callbacks, done, stop)
  end

  # A worker as Task.async/1 starts one (its $callers, for a sandbox's
  # allowances), monitored but not linked, trapping exits.
  defp start(target, one) do
    caller = self()
    callers = [caller | Process.get(:"$callers", [])]

    spawn_monitor(fn ->
      Process.flag(:trap_exit, true)
      Process.put(:"$callers", callers)
      {target, result} = one.(target)
      result = if linked_exit?(), do: {:error, {:linked_exit, result}}, else: result
      send(caller, {__MODULE__, self(), {target, result}})
    end)
  end

  # Whether a linked process exited abnormally meanwhile (its reason isn't
  # kept: it may hold anything).
  defp linked_exit? do
    receive do
      {:EXIT, _pid, reason} when reason != :normal -> true
    after
      0 -> false
    end
  end

  defp after_result(target, result, on_result, stop?) do
    if on_result, do: on_result.(target.given, result)
    if stop? && stop?.({target, result}), do: :stop
  catch
    kind, reason -> {kind, reason, __STACKTRACE__}
  end

  ## Arguments

  defp checked!(targets, opts, call, allowed) do
    opts = Keys.options!(opts, call)
    unknown_options!(opts, allowed, call)

    for name <- [:max_concurrency, :attempts], do: integer!(opts, name, 1, call)
    integer!(opts, :backoff, 0, call)
    for name <- [:around, :on_result, :stop_on], do: function!(opts, name, 2, call)
    function!(opts, :check, 1, call)

    for name <- [:force, :allow_reuse],
        Keyword.has_key?(opts, name),
        not is_boolean(opts[name]),
        do: raise(ArgumentError, "#{call}: #{inspect(name)} must be true or false")

    case opts[:encryption] do
      fun when is_function(fun, 1) ->
        :ok

      nil ->
        raise ArgumentError,
              "#{call} needs :encryption, a key ring or a function of the target returning one"

      # The same ring for every target: refused now, once, if malformed.
      ring ->
        Keys.normalize(ring)
    end

    {targets!(targets, call, true), opts}
  end

  defp unknown_options!(opts, allowed, call) do
    case Enum.find(Keyword.keys(opts), &(&1 not in allowed)) do
      nil -> :ok
      name -> raise ArgumentError, "#{call}: unknown option #{Keys.shown_value(name)}"
    end
  end

  defp integer!(opts, name, min, call) do
    case Keyword.fetch(opts, name) do
      {:ok, value} when is_integer(value) and value >= min -> :ok
      :error -> :ok
      _ -> raise ArgumentError, "#{call}: #{inspect(name)} must be an integer of at least #{min}"
    end
  end

  defp function!(opts, name, arity, call) do
    case Keyword.fetch(opts, name) do
      {:ok, fun} when is_function(fun, arity) ->
        :ok

      :error ->
        :ok

      _ ->
        raise ArgumentError, "#{call}: #{inspect(name)} must be a function of #{arity} arguments"
    end
  end

  # Each target as %{index:, id:, path:, s3:, given:}; `given` is what the
  # caller gave, passed back to its functions, every `:encryption` in it
  # hidden (Keys.hide_rings/1).
  defp targets!(targets, call, path?) do
    if Enumerable.impl_for(targets) == nil,
      do: raise(ArgumentError, "#{call}: the targets must be a list")

    targets
    |> Enum.with_index(1)
    |> Enum.map(fn {given, index} -> target!(given, index, call, path?) end)
  end

  defp target!(given, index, call, path?) do
    {id, path, s3} = parts!(given, index, call)

    unless is_binary(path) or (not path? and is_nil(path)),
      do: raise(ArgumentError, "#{call}: the path of target #{index} must be a string")

    unless is_list(s3) or is_map(s3),
      do:
        raise(
          ArgumentError,
          "#{call}: the S3 options of target #{index} must be a keyword list or a map"
        )

    %{index: index, id: id || index, path: path, s3: s3, given: Keys.hide_rings(given)}
  end

  defp parts!({path, s3}, _index, _call), do: {path, path, s3}

  defp parts!(%{s3: s3} = target, _index, _call),
    do: {Map.get(target, :id, Map.get(target, :path)), Map.get(target, :path), s3}

  defp parts!(_given, index, call),
    do:
      raise(
        ArgumentError,
        "#{call}: target #{index} must be {path, s3} or %{path: path, s3: s3}"
      )
end
