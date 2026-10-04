defmodule Sediment.CDC do
  @moduledoc """
  Change data capture (turso extension).

  When capture is enabled on a connection, turso records every change that
  connection makes in a CDC table (`turso_cdc` by default), in the same
  transaction as the change. `changes/2` reads and decodes those records.

  Capture is a per-connection setting. With a pool, enable it on every
  connection through the connect options:

      Sediment.start_link(database: "app.db", custom_pragmas: [capture_data_changes_conn: "'full'"])

  or on a single low-level connection with `enable/3`:

      {:ok, db} = Sediment.Engine.open("app.db")
      :ok = Sediment.CDC.enable(db, :full)
      :ok = Sediment.Engine.execute(db, "INSERT INTO t VALUES (1, 'a')")
      {:ok, [%Sediment.CDC.Change{type: :insert, table: "t", row_id: 1}]} =
        Sediment.CDC.changes(db, tables: ["t"])

  `enable/3` does not support `Sediment.Engine` refs obtained through a
  pool; `changes/2` accepts both a `Sediment.Engine` ref and a
  `DBConnection` connection.

  ## Modes

    * `:id` - record the change type and row id only
    * `:before` - also record the row before the change
    * `:after` - also record the row after the change
    * `:full` - record the row before and after the change, plus the updated
      columns
  """

  alias Sediment.Engine

  @default_table "turso_cdc"

  defmodule Change do
    @moduledoc """
    A decoded CDC record.

    `type` is `:insert`, `:update`, `:delete` or `:commit` (the end of a
    transaction; its table and row fields are `nil`). `before` and `after`
    are maps of column name to value when the capture mode recorded them.
    """

    defstruct [:id, :time, :txn_id, :type, :table, :row_id, :before, :after]

    @type t :: %__MODULE__{
            id: integer(),
            time: integer(),
            txn_id: integer() | nil,
            type: :insert | :update | :delete | :commit,
            table: String.t() | nil,
            row_id: integer() | nil,
            before: map() | nil,
            after: map() | nil
          }
  end

  @type mode :: :id | :before | :after | :full

  @doc """
  Enables change data capture on a low-level connection.

  ## Options

    * `:table` - the CDC table name. Defaults to `"turso_cdc"`.
  """
  @spec enable(Engine.db(), mode(), keyword()) :: :ok | {:error, Engine.reason()}
  def enable(db, mode \\ :full, opts \\ []) when mode in [:id, :before, :after, :full] do
    table = Keyword.get(opts, :table, @default_table)
    Engine.execute(db, "PRAGMA capture_data_changes_conn('#{mode},#{escape(table)}')")
  end

  @doc """
  Disables change data capture on a low-level connection.
  """
  @spec disable(Engine.db()) :: :ok | {:error, Engine.reason()}
  def disable(db), do: Engine.execute(db, "PRAGMA capture_data_changes_conn('off')")

  @doc """
  Reads recorded changes in order.

  ## Options

    * `:since` - only return changes with an id greater than this. Defaults
      to `0`.
    * `:limit` - maximum number of changes to return.
    * `:tables` - only return changes to these tables. Commit records are
      only included when this option is not given.
    * `:table` - the CDC table name. Defaults to `"turso_cdc"`.
  """
  @spec changes(Engine.db() | DBConnection.conn(), keyword()) ::
          {:ok, [Change.t()]} | {:error, term()}
  def changes(conn, opts \\ []) do
    {sql, params} = changes_query(opts)

    with {:ok, rows} <- run(conn, sql, params) do
      {:ok, Enum.map(rows, &to_change/1)}
    end
  end

  defp changes_query(opts) do
    cdc_table = "\"#{String.replace(Keyword.get(opts, :table, @default_table), "\"", "\"\"")}\""

    {table_filter, table_params} =
      case Keyword.get(opts, :tables) do
        nil ->
          {"", []}

        tables ->
          placeholders = Enum.map_join(tables, ", ", fn _ -> "?" end)
          {" AND table_name IN (#{placeholders})", tables}
      end

    {limit, limit_params} =
      case Keyword.get(opts, :limit) do
        nil -> {"", []}
        limit -> {" LIMIT ?", [limit]}
      end

    sql = """
    SELECT change_id, change_time, change_txn_id, change_type, table_name, id,
      CASE WHEN before IS NULL OR table_name = 'sqlite_schema' THEN NULL
        ELSE bin_record_json_object(table_columns_json_array(table_name), before) END,
      CASE WHEN after IS NULL OR table_name = 'sqlite_schema' THEN NULL
        ELSE bin_record_json_object(table_columns_json_array(table_name), after) END
    FROM #{cdc_table}
    WHERE change_id > ?#{table_filter}
    ORDER BY change_id#{limit}
    """

    {sql, [Keyword.get(opts, :since, 0)] ++ table_params ++ limit_params}
  end

  defp run(db, sql, params) when is_reference(db) do
    with {:ok, stmt} <- Engine.prepare(db, sql) do
      try do
        with :ok <- Engine.bind(stmt, params), do: Engine.fetch_all(db, stmt)
      after
        Engine.release(db, stmt)
      end
    end
  end

  defp run(conn, sql, params) do
    with {:ok, %{rows: rows}} <- Sediment.query(conn, sql, params), do: {:ok, rows}
  end

  defp to_change([id, time, txn_id, type, table, row_id, before, after_]) do
    %Change{
      id: id,
      time: time,
      txn_id: txn_id,
      type: change_type(type),
      table: table,
      row_id: row_id,
      before: decode(before),
      after: decode(after_)
    }
  end

  defp change_type(1), do: :insert
  defp change_type(0), do: :update
  defp change_type(-1), do: :delete
  defp change_type(2), do: :commit

  defp decode(nil), do: nil
  defp decode(json), do: JSON.decode!(json)

  defp escape(value), do: String.replace(to_string(value), "'", "''")
end
