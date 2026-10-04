defmodule Sediment.Stream do
  @moduledoc """
  An `Enumerable` over a query's results, fetched in chunks through a
  `DBConnection` cursor. Must be enumerated inside a transaction:

      Sediment.transaction(pool, fn conn ->
        %Sediment.Stream{conn: conn, query: "SELECT * FROM t", params: [], options: [max_rows: 500]}
        |> Enum.each(fn %Sediment.Result{rows: rows} -> process(rows) end)
      end)

  `query` is a SQL string or a prepared `Sediment.Query`. Each element is
  a `Sediment.Result` with up to `:max_rows` rows (default: the
  connection's `:chunk_size`).
  """
  defstruct [:conn, :query, :params, :options]
  @type t :: %Sediment.Stream{}

  defimpl Enumerable do
    def reduce(%Sediment.Stream{query: %Sediment.Query{} = query} = stream, acc, fun) do
      # Possibly need to pass a chunk size option along so that we can let
      # the NIF chunk it.
      %Sediment.Stream{conn: conn, params: params, options: opts} = stream

      stream = %DBConnection.Stream{
        conn: conn,
        query: query,
        params: params,
        opts: opts
      }

      DBConnection.reduce(stream, acc, fun)
    end

    def reduce(%Sediment.Stream{query: statement} = stream, acc, fun) do
      %Sediment.Stream{conn: conn, params: params, options: opts} = stream
      query = %Sediment.Query{name: "", statement: statement}

      stream = %DBConnection.PrepareStream{
        conn: conn,
        query: query,
        params: params,
        opts: opts
      }

      DBConnection.reduce(stream, acc, fun)
    end

    def member?(_, _), do: {:error, __MODULE__}

    def count(_), do: {:error, __MODULE__}

    def slice(_), do: {:error, __MODULE__}
  end
end
