defmodule Sediment.S3.Many do
  @moduledoc false
  # Sediment.S3.rekey_many/2, release_key_many/3 and key_census_many/2: one
  # operation over many databases, a few at a time, never stopping at a
  # failure. See their docs in Sediment.S3.

  alias Sediment.S3
  alias Sediment.S3.Keys

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

    run(targets, opts, rekey, &rekey_outcome/1, [:rekeyed, :current, :absent])
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

    run(targets, opts, release, &release_outcome/1, [:released, :unchanged, :absent])
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
      collect(targets, Keyword.get(opts, :max_concurrency, 8), census, opts[:on_result], nil)

    acc = %{keys: %{}, absent: [], unencrypted: [], unreadable: []}

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

  @spec removable?(map(), String.t()) :: boolean()
  def removable?(%{keys: keys, unreadable: unreadable}, key_id) do
    id = to_string(key_id)

    unreadable == [] and
      case keys do
        %{^id => %{current: [], retained: []}} -> true
        %{^id => _} -> false
        _ -> true
      end
  end

  def removable?(_census, _key_id),
    do:
      raise(
        ArgumentError,
        "Sediment.S3.removable?/2 expects the result of key_census_many/2"
      )

  ## Outcomes

  defp rekey_outcome({:ok, :current}), do: :current
  defp rekey_outcome({:ok, %{to: _}}), do: :rekeyed
  defp rekey_outcome(result), do: absent_or_failed(result)

  defp release_outcome({:ok, %{epochs: []}}), do: :unchanged
  defp release_outcome({:ok, %{epochs: _}}), do: :released
  defp release_outcome(result), do: absent_or_failed(result)

  defp absent_or_failed({:error, "s3 config: no database at " <> _}), do: :absent
  defp absent_or_failed(_result), do: :failed

  # A8: after a non-ok result the operation may have happened or may still
  # happen; only a re-run resolves it. Those are retried; a refusal (the
  # database open, the lease held, a retired key, a missing key) isn't.
  defp retry?({:error, {:fenced, _}}), do: true
  defp retry?({:error, {:indeterminate, _}}), do: true
  defp retry?({:error, "s3 store: " <> _}), do: true
  defp retry?({:error, "s3 conflict: " <> _}), do: true
  defp retry?({:error, "s3 writer fenced: " <> _}), do: true
  defp retry?(_result), do: false

  defp tally(acc, id, {:ok, :no_database}), do: Map.update!(acc, :absent, &[id | &1])

  defp tally(acc, id, {:ok, %{current: nil, retained: retained}}) when retained == %{},
    do: Map.update!(acc, :unencrypted, &[id | &1])

  defp tally(acc, id, {:ok, %{current: current, retained: retained}}) do
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
    one = fn target -> {target, attempt(target, opts, op)} end
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
        case outcome.(result) do
          :failed -> Map.update!(acc, :failed, &[{target.id, reason(result)} | &1])
          kind -> Map.update!(acc, kind, &[target.id | &1])
        end
      end)

    acc
    |> Map.new(fn {kind, ids} -> {kind, Enum.reverse(ids)} end)
    |> Map.put(:skipped, for(target <- targets, target.index not in done, do: target.id))
  end

  defp reason({:error, reason}), do: reason
  defp reason(other), do: {:unexpected, other}

  # The caller's around: wraps all of the target's attempts. The result is
  # what the operation returned when around: ran it, else around:'s own
  # {:error, reason} (a refusal of its own), else {:error, :not_run}.
  defp attempt(target, opts, op) do
    case opts[:around] do
      nil ->
        attempts(target, opts, op)

      around ->
        key = {__MODULE__, :result}
        Process.delete(key)

        runner = fn ->
          result = attempts(target, opts, op)
          Process.put(key, result)
          result
        end

        returned = callback(:around, fn -> around.(target.given, runner) end)

        case {Process.get(key), returned} do
          {_ran, {:error, {:callback_raised, :around, _}} = raised} -> raised
          {nil, {:error, _} = refused} -> refused
          {nil, _other} -> {:error, :not_run}
          {result, _returned} -> result
        end
    end
  end

  defp attempts(target, opts, op) do
    with :ok <- check(target, opts[:check]),
         {:ok, ring} <- ring(target, opts[:encryption]) do
      retrying(
        target,
        ring,
        op,
        Keyword.get(opts, :attempts, 3),
        Keyword.get(opts, :backoff, 1_000),
        1
      )
    end
  end

  defp check(_target, nil), do: :ok

  defp check(target, check) do
    case callback(:check, fn -> check.(target.given) end) do
      {:error, _} = refused -> refused
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

  defp retrying(target, ring, op, attempts, backoff, n) do
    result = operation(fn -> op.(target, ring) end)

    if retry?(result) and n < attempts do
      Process.sleep(backoff * Integer.pow(2, n - 1))
      retrying(target, ring, op, attempts, backoff, n + 1)
    else
      result
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

  # Runs `one` over the targets in tasks, at most `max` at once; calls
  # on_result.(given, result) and stop?.({target, result}) in this process
  # as each finishes. Once stop? is true (or on_result or stop? raises, which
  # is re-raised once the tasks running finish), no further target starts.
  defp collect(targets, max, one, on_result, stop?) do
    loop(targets, %{}, max, one, {on_result, stop?}, [], nil)
  end

  defp loop([target | rest], running, max, one, callbacks, done, nil)
       when map_size(running) < max do
    task = Task.async(fn -> one.(target) end)
    loop(rest, Map.put(running, task.ref, task), max, one, callbacks, done, nil)
  end

  # The caller's exception, its frames without their arguments (ours hold
  # the targets).
  defp loop(_queue, running, _max, _one, _callbacks, _done, {kind, reason, stacktrace})
       when running == %{},
       do: :erlang.raise(kind, reason, Keys.scrub_stacktrace(stacktrace))

  defp loop(queue, running, _max, _one, _callbacks, done, stop)
       when running == %{} and (queue == [] or stop != nil),
       do: Enum.reverse(done)

  defp loop(queue, running, max, one, {on_result, stop?} = callbacks, done, stop) do
    receive do
      {ref, {target, result}} when is_map_key(running, ref) ->
        Process.demonitor(ref, [:flush])
        running = Map.delete(running, ref)
        done = [{target, result} | done]
        stop = stop || after_result(target, result, on_result, stop?)
        loop(queue, running, max, one, callbacks, done, stop)
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
  # caller gave, passed back to its functions.
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

    %{index: index, id: id || index, path: path, s3: s3, given: given}
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
