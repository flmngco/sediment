defmodule Sediment do
  @moduledoc """
  Elixir driver for the Turso database engine.

  An `Exqlite`-compatible driver for [Turso](https://github.com/tursodatabase/turso),
  the SQLite-compatible database engine written in Rust. `Sediment.Connection`
  implements `DBConnection`; `Sediment.Engine` is the low-level API
  equivalent to `Exqlite.Sqlite3`.
  """

  # Every public function runs in the key-scrubbing boundary
  # (Sediment.S3.Keys.Sealed).
  use Sediment.S3.Keys.Sealed, except: [transaction: 3]

  alias Sediment.Connection
  alias Sediment.Error
  alias Sediment.Query
  alias Sediment.Result

  # The options DBConnection and its pools read, checked with fixed messages
  # before DBConnection sees them: a value of the wrong type would otherwise
  # fail inside DBConnection (or in a pool's process, later), with the value
  # in the exception, its stack trace or a crash report. Others are the
  # connection's (it checks them) or ignored.
  @transaction_options %{
    timeout: :timeout,
    sync_timeout: :timeout,
    deadline: :integer_or_nil,
    queue: :boolean,
    sync: :boolean,
    pool: :pool,
    log: :log
  }

  @pool_options %{
    pool: :pool,
    pool_size: :pos_integer,
    queue_target: :non_neg_integer,
    queue_interval: :non_neg_integer,
    idle_interval: :non_neg_integer,
    idle_limit: :non_neg_integer,
    backoff_min: :non_neg_integer,
    backoff_max: :non_neg_integer,
    backoff_type: :backoff_type,
    max_restarts: :non_neg_integer,
    max_seconds: :pos_integer,
    timeout: :timeout,
    after_connect_timeout: :timeout,
    shutdown: :shutdown,
    show_sensitive_data_on_connection_error: :boolean
  }

  @doc """
  Starts a pool of connections. See `Sediment.Connection.connect/1` for the options.
  """
  @spec start_link([Connection.connection_opt()]) :: {:ok, pid()} | {:error, Error.t()}
  def start_link(opts) do
    # Its options hold keys: see Sediment.S3.Keys.scrubbed/1 (`:key` is left
    # to connect/1, which refuses it as an unsupported option).
    Sediment.S3.Keys.scrubbed(fn ->
      opts = Sediment.S3.Keys.options!(opts, "Sediment.start_link/1", [:key])
      checked_options!(opts, @pool_options, "Sediment.start_link/1")
      DBConnection.start_link(Connection, opts |> hide_keys() |> with_s3_shutdown())
    end)
  end

  # The pool's processes keep their options (and supervisors their child
  # specs): the keys in them are hidden from crash reports and inspection.
  # `:key` too: connect/1 refuses it, but the pool keeps it meanwhile.
  defp hide_keys(opts) do
    Enum.reduce([:encryption, :key], opts, fn option, opts ->
      case Keyword.fetch(opts, option) do
        {:ok, value} -> Keyword.put(opts, option, Sediment.S3.Keys.hide(value))
        :error -> opts
      end
    end)
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
    # Not sealed (see Sediment.S3.Keys.Sealed): DBConnection's setup and
    # completion run in the boundary, `fun` out of it (its exceptions,
    # exits, throws and rollbacks are the caller's own).
    Sediment.S3.Keys.around(fun, fn carried ->
      Sediment.S3.Keys.options!(opts, "Sediment.transaction/3")
      check_conn!(conn, "Sediment.transaction/3")

      unless is_function(fun, 1),
        do: raise(ArgumentError, "Sediment.transaction/3: expects a function of one argument")

      checked_options!(opts, @transaction_options, "Sediment.transaction/3")
      DBConnection.transaction(conn, carried, opts)
    end)
  end

  defp checked_options!(opts, table, call) do
    for {name, value} <- opts,
        type = Map.get(table, name),
        not type?(type, value),
        do: raise(ArgumentError, "#{call}: invalid #{inspect(name)} (#{type_name(type)})")

    opts
  end

  defp type?(:timeout, value), do: value == :infinity or (is_integer(value) and value >= 0)
  defp type?(:shutdown, value), do: value == :brutal_kill or type?(:timeout, value)
  defp type?(:integer_or_nil, value), do: is_nil(value) or is_integer(value)
  defp type?(:boolean, value), do: is_boolean(value)
  defp type?(:pos_integer, value), do: is_integer(value) and value > 0
  defp type?(:non_neg_integer, value), do: is_integer(value) and value >= 0
  defp type?(:backoff_type, value), do: value in [:stop, :exp, :rand, :rand_exp]

  defp type?(:pool, value),
    do: is_atom(value) and Code.ensure_loaded?(value) and function_exported?(value, :checkout, 3)

  defp type?(:log, value) do
    case value do
      nil -> true
      fun when is_function(fun, 1) -> true
      {m, f, a} -> is_atom(m) and is_atom(f) and is_list(a)
      _other -> false
    end
  end

  defp type_name(:timeout), do: "a non-negative integer or :infinity"
  defp type_name(:shutdown), do: "a non-negative integer, :infinity or :brutal_kill"
  defp type_name(:integer_or_nil), do: "an integer or nil"
  defp type_name(:boolean), do: "a boolean"
  defp type_name(:pos_integer), do: "a positive integer"
  defp type_name(:non_neg_integer), do: "a non-negative integer"
  defp type_name(:backoff_type), do: "one of :stop, :exp, :rand and :rand_exp"
  defp type_name(:pool), do: "a DBConnection pool module"
  defp type_name(:log), do: "a function of one argument, an {m, f, a} tuple or nil"

  # A pool, or the connection of a transaction or run, alive: DBConnection's
  # exits for a name with no process carry the name.
  defp check_conn!(%DBConnection{}, _call), do: :ok
  defp check_conn!(pid, _call) when is_pid(pid), do: :ok

  defp check_conn!(conn, call) do
    case GenServer.whereis(conn) do
      nil ->
        raise ArgumentError,
              "#{call}: no process #{Sediment.S3.Keys.shown_value(conn)}"

      _found ->
        :ok
    end
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
    Sediment.S3.Keys.scrubbed(fn ->
      opts = Sediment.S3.Keys.options!(opts, "Sediment.child_spec/1", [:key])
      checked_options!(opts, @pool_options, "Sediment.child_spec/1")
      opts = opts |> hide_keys() |> with_s3_shutdown()
      spec = DBConnection.child_spec(Connection, opts)

      # The pool's supervisor must wait as long as the pool waits for its
      # connections, or it kills the pool while they are still closing.
      case opts[:shutdown] do
        nil -> spec
        shutdown -> Supervisor.child_spec(spec, shutdown: shutdown)
      end
    end)
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
  WAL there (turso_core 0.8.3's `VACUUM INTO` from MVCC writes a header
  SQLite can't read, resets `sqlite_sequence` and is quadratic for
  AUTOINCREMENT tables), and exported with `VACUUM INTO`; Turso FTS indexes
  are dropped only with `drop_fts: true`. Before the file appears at `dest`
  (mode 0600) it is checked: `PRAGMA integrity_check`, the row count of
  every table and `sqlite_sequence` against the source. For a symlink the
  WAL and MVCC log are found next to the link or its target (both is
  refused as ambiguous); a file with several hard links is refused.

  ## Options

    * `:encryption` - the source's key (`[cipher: ..., key: ...]`) or key
      ring (an S3 source is read with its current key, a local file with
      the ring's active key), or `false` for an unencrypted S3 database
      (with `:from_s3`, one of them is required, as for an open).
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
    Sediment.S3.Keys.scrubbed(fn ->
      opts = Sediment.S3.Keys.options!(opts, "Sediment.export_sqlite/3")
      source = source && to_string(source)
      dest = to_string(dest)

      from_s3 =
        case opts[:from_s3] do
          nil -> nil
          s3 -> Sediment.S3.with_encryption_choice(s3, opts[:encryption])
        end

      ring = Sediment.S3.Keys.normalize(opts[:encryption])
      drop_fts = opts[:drop_fts] == true

      Sediment.Telemetry.span([:export], %{source: source, dest: dest}, fn ->
        export(source, dest, from_s3, ring, drop_fts)
      end)
    end)
  end

  defp export(source, dest, from_s3, ring, drop_fts) do
    ring = ring |> export_ring(from_s3) |> Sediment.S3.Keys.native()
    export = fn -> Sediment.Native.export_sqlite(source, dest, from_s3, ring, drop_fts) end
    Sediment.S3.Keys.scrubbed(export)
  end

  # An S3 source is read with its current key: a key_for ring resolves it
  # first (from the manifest, see Sediment.S3.key_census/1).
  defp export_ring(%{key_for: key_for} = ring, from_s3) when key_for != nil and from_s3 != nil do
    case Sediment.S3.key_census(from_s3) do
      {:ok, %{current: id}} when is_binary(id) -> Sediment.S3.Keys.with_key(ring, id)
      _ -> ring
    end
  end

  defp export_ring(ring, _from_s3), do: ring
end
