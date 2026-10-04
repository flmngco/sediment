defmodule Sediment.ParityTest do
  use ExUnit.Case, async: true

  # Keeps guides/exqlite_parity.md honest: every public exqlite function has
  # a row (needs an exqlite checkout, named by EXQLITE_SRC), and every row
  # marked "yes" is exported by our module (runs everywhere).
  @exqlite Path.join(Path.expand(System.get_env("EXQLITE_SRC", "")), "lib")
  @guide Path.expand("../../guides/exqlite_parity.md", __DIR__)

  @tag skip:
         (System.get_env("EXQLITE_SRC") in [nil, ""] or not File.dir?(@exqlite)) &&
           "set EXQLITE_SRC to an exqlite checkout"
  test "every exqlite function is listed in the parity guide" do
    rows = guide_rows()

    missing =
      for {module, name, arity} <- exqlite_functions(),
          not Map.has_key?(rows, {module, "#{name}/#{arity}"}),
          do: "#{module}.#{name}/#{arity}"

    assert missing == []
  end

  test "functions the guide marks as available are exported" do
    for {{_exqlite, function}, {ours, "yes"}} <- guide_rows() do
      [name, arity] = String.split(function, "/")
      module = Module.concat([ours])
      Code.ensure_loaded!(module)

      assert function_exported?(module, String.to_atom(name), String.to_integer(arity)),
             "#{ours}.#{function} is marked yes but not exported"
    end
  end

  defp guide_rows do
    for line <- @guide |> File.read!() |> String.split("\n"),
        [_, exqlite, ours, function, status | _] <- [String.split(line, "|")],
        exqlite = strip(exqlite),
        String.starts_with?(exqlite, "Exqlite"),
        into: %{},
        do: {{exqlite, strip(function)}, {strip(ours), strip(status)}}
  end

  defp strip(cell), do: cell |> String.trim() |> String.trim("`")

  defp exqlite_functions do
    for file <- Path.wildcard(Path.join(@exqlite, "**/*.ex")),
        {module, defs} <- modules(Code.string_to_quoted!(File.read!(file))),
        {name, arities} <- defs,
        arity <- arities,
        uniq: true,
        do: {module, name, arity}
  end

  defp modules(ast) do
    {_, acc} =
      Macro.prewalk(ast, [], fn
        {:defmodule, _, [{:__aliases__, _, parts}, [do: body]]}, acc ->
          {nil, [{Enum.join(parts, "."), defs(body)} | acc]}

        node, acc ->
          {node, acc}
      end)

    acc
  end

  # Public functions of one module body, with default arguments expanded;
  # protocol implementations and nested modules are not the module's API.
  defp defs(body) do
    {_, acc} =
      Macro.prewalk(body, [], fn
        {kind, _, _}, acc when kind in [:defmodule, :defimpl] ->
          {nil, acc}

        {:def, _, [head | _]}, acc ->
          {name, args} = name_and_args(head)
          defaults = Enum.count(args, &match?({:\\, _, _}, &1))
          {nil, [{name, (length(args) - defaults)..length(args)//1} | acc]}

        node, acc ->
          {node, acc}
      end)

    acc
  end

  defp name_and_args({:when, _, [head | _]}), do: name_and_args(head)
  defp name_and_args({name, _, args}) when is_list(args), do: {name, args}
  defp name_and_args({name, _, _}), do: {name, []}
end
