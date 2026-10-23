defmodule Sediment.KeyNetTest do
  # Every public function, enumerated from the docs, called with arguments
  # of wrong shapes that hold a synthetic key, in every position: the key
  # never shows in a result, an exception (message, inspect, stack trace),
  # an exit or a throw. Not async: it works in a directory of its own (a
  # key given as a path creates files there).
  use ExUnit.Case, async: false

  @key String.duplicate("7c", 32)
  @k128 String.duplicate("6e", 16)

  @modules [
    Sediment,
    Sediment.Engine,
    Sediment.S3,
    Sediment.Connection,
    Sediment.Basic,
    Sediment.Pragma,
    Sediment.CDC
  ]

  # Functions with no argument that could carry a key.
  @no_arguments [{Sediment.S3, :generate_key, 0}, {Sediment.S3, :request_counts, 0}]

  # Functions whose successful result is made of their arguments: given a
  # key as a path, Basic.open/1 opens a file of that name and names it;
  # child_spec/1 returns the pool's options (the caller's choice), with
  # `:encryption` and `:key` hidden. Their errors are checked like any.
  @echoing [{Sediment.Basic, :open, 1}, {Sediment, :child_spec, 1}]

  defp ring(key), do: [keys: [{"r1", key}], cipher: "aegis256"]

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
      [mode: [:readwrite, String.to_atom(key)]]
    ]
  end

  defp functions do
    for module <- @modules,
        {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(module),
        {{:function, name, arity}, _, _, doc, _} <- docs,
        doc != :hidden,
        do: {module, name, arity}
  end

  # For each position, each shape there, and the other positions either
  # nil or the same shape.
  defp calls(arity, key) do
    for position <- 0..(arity - 1),
        shape <- shapes(key),
        fill <- [nil, shape],
        uniq: true,
        do: List.duplicate(fill, arity) |> List.replace_at(position, shape)
  end

  # Everything a call shows.
  defp outcome(module, name, args) do
    result = apply(module, name, args)
    cleanup(result)

    case result do
      {:ok, _} when {module, name, length(args)} in @echoing -> []
      %{start: _} when {module, name, length(args)} in @echoing -> []
      result -> [inspect(result, limit: :infinity)]
    end
  rescue
    exception ->
      [
        Exception.format(:error, exception, __STACKTRACE__),
        inspect(exception, limit: :infinity),
        inspect(__STACKTRACE__, limit: :infinity)
      ]
  catch
    kind, reason ->
      [Exception.format(kind, reason, __STACKTRACE__), inspect(reason, limit: :infinity)]
  end

  defp cleanup({:ok, db}) when is_reference(db), do: Sediment.Engine.close(db)
  defp cleanup({:ok, pid}) when is_pid(pid), do: Supervisor.stop(pid)

  defp cleanup({:ok, %Sediment.Connection{} = state}),
    do: Sediment.Connection.disconnect(:normal, state)

  defp cleanup(_result), do: :ok

  defp windows(key), do: for(i <- 0..(byte_size(key) - 16), do: binary_part(key, i, 16))

  @tag timeout: 600_000
  test "no public function shows a key given in any argument, in any shape" do
    dir = Temp.path!()
    File.mkdir_p!(dir)
    functions = functions()
    assert {Sediment.Engine, :open, 2} in functions
    assert {Sediment.S3, :refresh, 2} in functions

    leaks =
      File.cd!(dir, fn ->
        ExUnit.CaptureLog.capture_log(fn ->
          for {module, name, arity} = function <- functions,
              function not in @no_arguments,
              key <- [@key, @k128],
              args <- calls(arity, key),
              text <- outcome(module, name, args),
              Enum.any?(windows(key), &String.contains?(text, &1)),
              do: send(self(), {:leak, function, args, text})
        end)

        receive_leaks([])
      end)

    assert leaks == [],
           leaks |> Enum.uniq_by(&elem(&1, 0)) |> Enum.map_join("\n\n", &inspect/1)

    # Every function of the modules is either called or listed as taking
    # no argument.
    for {m, f, 0} <- functions, do: assert({m, f, 0} in @no_arguments)
  end

  defp receive_leaks(acc) do
    receive do
      {:leak, function, args, text} ->
        # The arguments themselves hold the key: name them by shape only.
        receive_leaks([{function, Enum.map(args, &shape_of/1), String.slice(text, 0, 300)} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp shape_of(arg) when is_binary(arg), do: "a string"
  defp shape_of(arg) when is_atom(arg), do: "an atom"
  defp shape_of(arg) when is_map(arg), do: "a map"
  defp shape_of(arg) when is_tuple(arg), do: "a tuple"
  defp shape_of(arg) when is_list(arg), do: "a list"
end
