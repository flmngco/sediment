defmodule Sediment.SpecTest do
  use ExUnit.Case, async: true

  # Every public function has a @spec. Behaviour callbacks are specified by
  # their behaviour, and a spec on the highest arity covers the variants that
  # default arguments create.
  test "the public API is fully specified" do
    {:ok, modules} = :application.get_key(:sediment, :modules)

    missing =
      for module <- Enum.sort(modules),
          public?(module),
          {name, arity} <- module.__info__(:functions),
          not generated?(name),
          not specified?(module, name, arity),
          not callback?(module, name, arity),
          do: "#{inspect(module)}.#{name}/#{arity}"

    assert missing == []
  end

  defp public?(module) do
    Code.ensure_loaded!(module)
    not match?({:docs_v1, _, _, _, :hidden, _, _}, Code.fetch_docs(module))
  end

  defp generated?(name) do
    name in [:rustler_init, :load_rustler_precompiled] or
      String.starts_with?(Atom.to_string(name), "__")
  end

  defp specified?(module, name, arity) do
    case Code.Typespec.fetch_specs(module) do
      {:ok, specs} -> Enum.any?(specs, fn {{f, a}, _} -> f == name and a >= arity end)
      :error -> false
    end
  end

  defp callback?(module, name, arity) do
    behaviours = module.module_info(:attributes)[:behaviour] || []
    Enum.any?(behaviours, &({name, arity} in &1.behaviour_info(:callbacks)))
  end
end
