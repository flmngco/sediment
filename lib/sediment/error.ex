defmodule Sediment.Error do
  @moduledoc """
  The error emitted from turso or a general error with the library.

  `:statement` is the statement that failed, as it was given. The message
  and `inspect/2` show it with every run of 32 or more hex digits replaced
  by `<redacted>` (a key given in SQL by mistake never shows); the field
  itself is unchanged.
  """

  alias Sediment.S3.Keys

  defexception [:message, :statement]

  @type t :: %__MODULE__{
          message: String.t(),
          statement: String.t()
        }

  @impl true
  def message(%__MODULE__{message: message, statement: nil}), do: Keys.redact(message)

  def message(%__MODULE__{message: message, statement: statement}),
    do: Keys.redact("#{message}\n#{statement}")

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%{message: message, statement: statement}, opts) do
      statement = if statement, do: Keys.redact("#{statement}")
      fields = [message: Keys.redact(message), statement: statement]

      container_doc("%Sediment.Error{", fields, "}", opts, &field/2, separator: ",")
    end

    defp field({name, value}, opts), do: concat("#{name}: ", to_doc(value, opts))
  end
end
