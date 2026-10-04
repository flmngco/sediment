defmodule Sediment do
  @moduledoc """
  Elixir driver for the Turso database engine.

  An `Exqlite`-compatible driver for [Turso](https://github.com/tursodatabase/turso),
  the SQLite-compatible database engine written in Rust. `Sediment.Connection`
  implements `DBConnection`; `Sediment.Engine` is the low-level API
  equivalent to `Exqlite.Sqlite3`.
  """

  alias Sediment.Connection
  alias Sediment.Error
  alias Sediment.Query
  alias Sediment.Result

  @doc """
  Starts a pool of connections. See `Sediment.Connection.connect/1` for the options.
  """
  @spec start_link([Connection.connection_opt()]) :: {:ok, pid()} | {:error, Error.t()}
  def start_link(opts) do
    DBConnection.start_link(Connection, with_s3_shutdown(opts))
  end

  # Closing an async S3 connection drains pending uploads; the pool must wait
  # for that (see Sediment.S3.shutdown_timeout/1).
  defp with_s3_shutdown(opts) do
    case opts[:s3] do
      nil -> opts
      s3 -> Keyword.put_new(opts, :shutdown, Sediment.S3.shutdown_timeout(s3))
    end
  end

  @doc """
  Prepares and runs `statement` with `params`, returning `{:ok, %Sediment.Result{}}`.

  With S3 durability, `sync: true` on a statement outside a transaction
  returns only once what it committed is durable in S3; a read returns at
  once (inside a transaction the option does nothing: pass it to the
  transaction); see `transaction/3`.
  """
  @spec query(DBConnection.conn(), iodata(), list(), list()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], opts \\ []) do
    query = %Query{name: "", statement: IO.iodata_to_binary(statement)}

    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _query, result} ->
        {:ok, result}

      otherwise ->
        otherwise
    end
  end

  @doc """
  Like `query/4`, but raises on error.
  """
  @spec query!(DBConnection.conn(), iodata(), list(), list()) :: Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Prepares a named query for later `execute/4` calls.
  """
  @spec prepare(DBConnection.conn(), iodata(), iodata(), list()) ::
          {:ok, Query.t()} | {:error, Exception.t()}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Like `prepare/4`, but raises on error.
  """
  @spec prepare!(DBConnection.conn(), iodata(), iodata(), list()) :: Query.t()
  def prepare!(conn, name, statement, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare!(conn, query, opts)
  end

  @doc """
  Prepares and executes a named query, returning the query and the result.
  """
  @spec prepare_execute(DBConnection.conn(), iodata(), iodata(), list(), list()) ::
          {:ok, Query.t(), Result.t()} | {:error, Error.t()}
  def prepare_execute(conn, name, statement, params, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Like `prepare_execute/5`, but raises on error.
  """
  @spec prepare_execute!(DBConnection.conn(), iodata(), iodata(), list(), list()) ::
          {Query.t(), Result.t()}
  def prepare_execute!(conn, name, statement, params, opts \\ []) do
    query = %Query{name: name, statement: statement}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query with `params`.
  """
  @spec execute(DBConnection.conn(), Query.t(), list(), list()) ::
          {:ok, DBConnection.Query.t(), Result.t()} | {:error, Error.t()}
  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  @doc """
  Like `execute/4`, but raises on error.
  """
  @spec execute!(DBConnection.conn(), Query.t(), list(), list()) :: Result.t()
  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
  end

  @doc """
  Closes a prepared query.
  """
  @spec close(DBConnection.conn(), Query.t(), list()) :: :ok | {:error, Exception.t()}
  def close(conn, query, opts \\ []) do
    with {:ok, _} <- DBConnection.close(conn, query, opts) do
      :ok
    end
  end

  @doc """
  Like `close/3`, but raises on error.
  """
  @spec close!(DBConnection.conn(), Query.t(), list()) :: :ok
  def close!(conn, query, opts \\ []) do
    DBConnection.close!(conn, query, opts)
    :ok
  end

  @doc """
  Runs `fun` in a transaction. See `DBConnection.transaction/3`; `mode:` selects the `BEGIN` flavour (`:deferred`, `:immediate`, `:exclusive`, `:concurrent`).

  With S3 durability, `sync: true` returns only once the transaction is
  durable in S3 (`:sync_timeout` bounds the wait, `:timeout` by default); a
  transaction that wrote nothing returns at once. If
  the wait fails the result is an error saying the transaction is committed
  locally but not known to be durable. On a database without S3 the option
  does nothing, and so it does on a transaction nested in another one (only
  the outermost commit can be made durable).
  """
  @spec transaction(DBConnection.conn(), (DBConnection.t() -> result), list()) ::
          {:ok, result} | {:error, any}
        when result: var
  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, opts)
  end

  @doc """
  Rolls back the current transaction with `reason`. See `DBConnection.rollback/2`.
  """
  @spec rollback(DBConnection.t(), term()) :: no_return()
  def rollback(conn, reason), do: DBConnection.rollback(conn, reason)

  @doc """
  A child spec for a connection pool, for supervision trees.
  """
  @spec child_spec([Connection.connection_opt()]) :: :supervisor.child_spec()
  def child_spec(opts) do
    opts = with_s3_shutdown(opts)
    spec = DBConnection.child_spec(Connection, opts)

    # The pool's supervisor must wait as long as the pool waits for its
    # connections, or it kills the pool while they are still closing.
    case opts[:shutdown] do
      nil -> spec
      shutdown -> Supervisor.child_spec(spec, shutdown: shutdown)
    end
  end

  @doc """
  Exports a database to a new plain SQLite file at `dest`, the way back
  from Sediment to SQLite (see the "Leaving Sediment" guide).

      # a local database file (encrypted: pass its key)
      {:ok, info} = Sediment.export_sqlite("/var/lib/app/app.db", "/tmp/app-sqlite.db")

      # the latest state of an S3 database (the prefix is only read)
      {:ok, info} =
        Sediment.export_sqlite(nil, "/tmp/app-sqlite.db",
          from_s3: s3_opts,
          encryption: [cipher: "aegis256", key: key]
        )

  The source is never written to: it is copied (or restored from S3) into
  a private (0700) staging directory next to `dest`, switched from MVCC to
  WAL there (turso_core 0.8.1's `VACUUM INTO` from MVCC writes a header
  SQLite can't read, resets `sqlite_sequence` and is quadratic for
  AUTOINCREMENT tables), and exported with `VACUUM INTO`; Turso FTS indexes
  are dropped only with `drop_fts: true`. Before the file appears at `dest`
  (mode 0600) it is checked: `PRAGMA integrity_check`, the row count of
  every table and `sqlite_sequence` against the source. For a symlink the
  WAL and MVCC log are found next to the link or its target (both is
  refused as ambiguous); a file with several hard links is refused.

  ## Options

    * `:encryption` - the source's key (`[cipher: ..., key: ...]`), or
      `false` for an unencrypted S3 database (with `:from_s3`, one of them is
      required, as for an open).
    * `:from_s3` - export the S3 database with these `:s3` options instead
      of a local file (`source` must then be `nil`).
    * `:drop_fts` - leave Turso FTS indexes (`CREATE INDEX ... USING fts`)
      out of the copy; without it, a database with any is refused, listing
      them, because SQLite can't read a file that has them. The indexed text
      stays.

  Returns `{:ok, %{rows: n, objects: n, sequences: [{table, seq}], dropped_fts:
  [name]}}` or `{:error, message}`: `dest` (or its `-wal`/`-journal`) exists,
  the source can't be read (a missing or wrong key), FTS indexes without
  `drop_fts: true`, or a check of the copy failed. On an error nothing is
  left at `dest`.

  turso's own tables (`__turso_internal_*`) stay in the copy: SQLite treats
  them as ordinary tables, and Sediment needs them if the file is opened
  with it again. The guide shows how to drop them.
  """
  @spec export_sqlite(Path.t() | nil, Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def export_sqlite(source, dest, opts \\ []) do
    source = source && to_string(source)
    dest = to_string(dest)

    from_s3 =
      case opts[:from_s3] do
        nil -> nil
        s3 -> Sediment.S3.with_encryption_choice(s3, opts[:encryption])
      end

    encryption =
      case opts[:encryption] do
        enc when is_list(enc) -> {to_string(enc[:cipher]), to_string(enc[:key])}
        _ -> nil
      end

    Sediment.Telemetry.span([:export], %{source: source, dest: dest}, fn ->
      Sediment.Native.export_sqlite(source, dest, from_s3, encryption, opts[:drop_fts] == true)
    end)
  end
end
