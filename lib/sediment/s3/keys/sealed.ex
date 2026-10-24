defmodule Sediment.S3.Keys.Sealed do
  @moduledoc false
  # Runs every public function of a module in the boundary,
  # Sediment.S3.Keys.scrubbed/1: `use Sediment.S3.Keys.Sealed` at the top of
  # the module. Each public function is overridden by one that calls the
  # original inside the boundary, so its arguments, whatever their shape,
  # never reach an exception (its function clause included), an exit or an
  # error result unscrubbed.
  #
  # Options:
  #
  #   * `:except` - functions left out, `[name: arity]`: a function running
  #     a caller's callback (whose exceptions are the caller's own) checks
  #     its arguments in the boundary itself;
  #   * `:only` - the functions sealed, instead of all of them.

  defmacro __using__(opts) do
    quote do
      @sealed_except Keyword.get(unquote(opts), :except, [])
      @sealed_only Keyword.get(unquote(opts), :only)
      @before_compile Sediment.S3.Keys.Sealed
    end
  end

  defmacro __before_compile__(env) do
    except = Module.get_attribute(env.module, :sealed_except)
    only = Module.get_attribute(env.module, :sealed_only)

    for {name, arity} = definition <- Module.definitions_in(env.module, :def),
        sealed?(env.module, definition, except, only) do
      args = Macro.generate_arguments(arity, __MODULE__)

      quote do
        defoverridable [{unquote(name), unquote(arity)}]

        def unquote(name)(unquote_splicing(args)) do
          Sediment.S3.Keys.scrubbed(fn -> super(unquote_splicing(args)) end)
        end
      end
    end
  end

  defp sealed?(module, {name, arity} = definition, except, only) do
    {:v1, _kind, meta, clauses} = Module.get_definition(module, definition)

    # A head generated for default arguments calls the full arity, which is
    # sealed.
    not default_head?(meta, clauses) and
      not String.starts_with?(Atom.to_string(name), "__") and
      {name, arity} not in except and
      (only == nil or {name, arity} in only)
  end

  # Elixir 1.20 marks the head's metadata; 1.18 only its body, a `super`
  # call tagged as a default's.
  defp default_head?(meta, clauses) do
    meta[:context] == :elixir_def or
      case clauses do
        [{_meta, _args, [], {:super, call, _args_passed}}] -> call[:default] == true
        _ -> false
      end
  end
end
