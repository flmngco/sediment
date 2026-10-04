defmodule Sediment.Telemetry do
  @moduledoc """
  Telemetry events emitted by `sediment`.

  exqlite emits none. `DBConnection` emits `[:db_connection, :connected]`
  and `[:db_connection, :disconnected]` when you start the pool with a
  `DBConnection.TelemetryListener` in `:connection_listeners`, and Ecto
  emits its own query events. The events below add what only the driver
  knows.

  ## Queries

  `[:sediment, :query, :start | :stop | :exception]`, a
  `:telemetry.span/3` around every statement executed through
  `Sediment.Connection` (`Sediment.query/4`, `Sediment.execute/4`,
  Ecto queries).

    * Measurements: `:system_time` (start), `:duration` (stop, exception),
      in native time units.
    * Metadata: `:query` (the SQL), `:params`, `:database` (the database
      option); on stop also `:result`, `{:ok, %Sediment.Result{}}` or
      `{:error, exception}`.

  `[:sediment, :prepare, :start | :stop | :exception]` likewise around
  preparing a statement (`Sediment.prepare/4`, and the first step of
  `Sediment.query/4`), with `:query` and `:database`, and on stop
  `:result`, `{:ok, %Sediment.Query{}}` or `{:error, exception}`. A
  statement that fails to prepare (a syntax error, a missing table) only
  emits these.

  ## Disconnects

  `[:sediment, :connection, :disconnect]` when a pooled connection
  closes its database.

    * Measurements: `:system_time`.
    * Metadata: `:database`, `:error` (the exception DBConnection passed to
      `disconnect/2`) and `:reason`:
      * `:fenced` - an S3 writer lost its lease or an upload failed; the pool
        reconnects and restores from S3.
      * `:internal` - turso_core panicked; the statement failed and the
        connection was closed.
      * `:closed` - the handle was already closed, for example by a client
        timeout.
      * `:other` - anything else, such as a client timeout or pool shutdown.

  ## S3

  `[:sediment, :s3, operation, :start | :stop | :exception]` for
  `operation` in `:refresh`, `:snapshot`, `:restore`, `:import` and `:flush`
  (`Sediment.S3`), including the automatic refreshes of replica pools and
  the waits of `sync: true` commits (`:flush`, metadata `:timeout`; `:result`
  is `:ok` or `{:error, reason}`).

    * Measurements: as for queries.
    * Metadata: on stop `:result`, the function's return value; `:path`
      for restores and imports.

  `[:sediment, :export, :start | :stop | :exception]` for
  `Sediment.export_sqlite/3`, metadata `:source` and `:dest`.

  Commit uploads happen inside the native code; `Sediment.S3.info/1`
  reports their counts.

  ## Cost

  A span with no handler attached costs about a microsecond. A statement
  run through a pool emits two (prepare and query), a few percent of the
  cheapest pooled query (a point select, about 77 µs in `bench/RESULTS.md`)
  and negligible for anything larger.
  """

  @doc false
  @spec span([atom()], map(), (-> result)) :: result when result: var
  def span(event, metadata, fun) do
    :telemetry.span([:sediment | event], metadata, fn ->
      result = fun.()
      {result, Map.put(metadata, :result, result)}
    end)
  end

  @doc false
  @spec disconnect(Exception.t() | term(), term()) :: :ok
  def disconnect(error, database) do
    :telemetry.execute(
      [:sediment, :connection, :disconnect],
      %{system_time: System.system_time()},
      %{database: database, error: error, reason: reason(error)}
    )
  end

  defp reason(%{message: message}) when is_binary(message) do
    cond do
      String.contains?(message, "s3 writer fenced") -> :fenced
      String.starts_with?(message, "internal turso error:") -> :internal
      message == "connection_closed" -> :closed
      true -> :other
    end
  end

  defp reason(_error), do: :other
end
