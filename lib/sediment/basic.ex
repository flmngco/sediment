defmodule Sediment.Basic do
  @moduledoc """
  A very basic API for simple use cases.
  """

  alias Sediment.Connection
  alias Sediment.Engine
  alias Sediment.Error
  alias Sediment.Query
  alias Sediment.Result

  @doc """
  Opens a connection without a pool. See `Sediment.Connection.connect/1`.
  """
  @spec open(String.t()) :: {:ok, Connection.t()} | {:error, Exception.t()}
  def open(path) do
    Connection.connect(database: path)
  end

  @doc """
  Closes a connection opened with `open/1`.
  """
  @spec close(Connection.t()) :: :ok | {:error, Error.t()}
  def close(%Connection{} = conn) do
    case Engine.close(conn.db) do
      :ok -> :ok
      {:error, reason} -> {:error, %Error{message: to_string(reason)}}
    end
  end

  @doc """
  Runs `stmt` with `args` on the connection.
  """
  @spec exec(Connection.t(), iodata(), list()) ::
          {:ok, Query.t(), Result.t(), Connection.t()}
          | {:error | :disconnect, Error.t(), Connection.t()}
  def exec(%Connection{} = conn, stmt, args \\ []) do
    %Query{statement: stmt} |> Connection.handle_execute(args, [], conn)
  end

  @doc """
  Turns the result of `exec/3` into `{:ok, rows, columns}` or `{:error, message}`.
  """
  @spec rows(
          {:ok, Query.t(), Result.t(), Connection.t()}
          | {:error | :disconnect, Error.t(), Connection.t()}
        ) :: {:ok, [[term()]], [String.t()]} | {:error, String.t()}
  def rows(exec_result) do
    case exec_result do
      {:ok, %Query{}, %Result{rows: rows, columns: columns}, %Connection{}} ->
        {:ok, rows, columns}

      {status, %Error{message: message}, %Connection{}} when status in [:error, :disconnect] ->
        {:error, to_string(message)}
    end
  end

  @doc "Not supported by turso. Always returns `{:error, :not_supported}`."
  @spec load_extension(Connection.t(), String.t()) :: {:error, :not_supported}
  def load_extension(_conn, _path), do: {:error, :not_supported}

  @doc "Not supported by turso. Always returns `{:error, :not_supported}`."
  @spec enable_load_extension(Connection.t()) :: {:error, :not_supported}
  def enable_load_extension(conn), do: Engine.enable_load_extension(conn.db, true)

  @doc "Not supported by turso. Always returns `{:error, :not_supported}`."
  @spec disable_load_extension(Connection.t()) :: {:error, :not_supported}
  def disable_load_extension(conn), do: Engine.enable_load_extension(conn.db, false)
end
