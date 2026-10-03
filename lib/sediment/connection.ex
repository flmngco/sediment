defmodule Sediment.Connection do
  @moduledoc """
  This module implements connection details as defined in DBProtocol.

  ## Attributes

  - `db` - The turso database reference.
  - `path` - The path that was used to open.
  - `transaction_status` - The status of the connection. Can be `:idle` or `:transaction`.

  Notes:
    - we closely follow the structure and naming conventions of
      `Exqlite.Connection`, so that code written against exqlite works here.
  """

  use DBConnection
  alias Sediment.Engine
  alias Sediment.Error
  alias Sediment.Native
  alias Sediment.Pragma
  alias Sediment.Query
  alias Sediment.Result
  alias Sediment.S3

  defstruct [
    :db,
    :default_transaction_mode,
    :directory,
    :path,
    :transaction_status,
    :status,
    :chunk_size,
    :before_disconnect,
    pending_begin: false,
    s3_generation: 0,
    generation_key: nil,
    stmt_cache: %{}
  ]

  @type t() :: %__MODULE__{
          db: Engine.db(),
          directory: String.t() | nil,
          path: String.t(),
          transaction_status: :idle | :transaction | :error,
          status: :idle | :busy,
          chunk_size: integer(),
          before_disconnect: (Exception.t(), t -> any) | {module, atom, [any]} | nil
        }

  @type journal_mode() :: :delete | :truncate | :persist | :memory | :wal | :off | :mvcc
  @type temp_store() :: :default | :file | :memory
  @type synchronous() :: :extra | :full | :normal | :off
  @type auto_vacuum() :: :none | :full | :incremental
  @type locking_mode() :: :normal | :exclusive
  @type transaction_mode() :: :deferred | :immediate | :exclusive | :concurrent

  @type connection_opt() ::
          {:database, String.t()}
          | {:default_transaction_mode, transaction_mode()}
          | {:mode, Engine.open_opt()}
          | {:journal_mode, journal_mode()}
          | {:temp_store, temp_store()}
          | {:synchronous, synchronous()}
          | {:foreign_keys, :on | :off}
          | {:cache_size, integer()}
          | {:cache_spill, :on | :off}
          | {:case_sensitive_like, boolean()}
          | {:auto_vacuum, auto_vacuum()}
          | {:locking_mode, locking_mode()}
          | {:secure_delete, :on | :off}
          | {:wal_auto_check_point, integer()}
          | {:busy_timeout, integer()}
          | {:progress_handler_steps, integer()}
          | {:chunk_size, integer()}
          | {:journal_size_limit, integer()}
          | {:soft_heap_limit, integer()}
          | {:hard_heap_limit, integer()}
          | {:encryption, [cipher: String.t(), key: String.t()] | false}
          | {:experimental, [atom()]}
          | {:s3, keyword() | map()}
          | {:custom_pragmas, [{keyword(), integer() | boolean() | String.t()}]}
          | {:before_disconnect, (Exception.t(), t -> any) | {module, atom, [any]} | nil}

  @impl true
  @doc """
  Initializes the connection.

  Options follow `Exqlite.Connection.connect/1`. Turso ignores or does not
  implement some SQLite pragmas; see the README section "Differences from
  exqlite".

  Allowed options:

    * `:database` - The path to the database. In memory is allowed. You can use
      `:memory` or `":memory:"` to designate that. `file:` URIs with a `mode`
      query parameter (`ro`, `rw`, `rwc`, `memory`) are accepted.
    * `:default_transaction_mode` - one of `deferred` (default), `immediate`,
      `exclusive` or `concurrent`. If a mode is not specified in a call to
      `Repo.transaction/2`, this will be the default transaction mode.
      `concurrent` issues `BEGIN CONCURRENT` and requires `journal_mode: :mvcc`.
      Turso refuses DDL inside `BEGIN CONCURRENT`, so when `:concurrent` is
      the default (not requested with `mode: :concurrent`), the transaction
      begins at its first statement, as `BEGIN IMMEDIATE` if that statement
      is DDL (`CREATE`, `ALTER`, `DROP`, `REINDEX`). Migrations therefore work
      with this default.
    * `:mode` - `[:readwrite, :create]` (default), `:readwrite`, `:readonly`
      or a list of these. See `Sediment.Engine.open/2`.
    * `:journal_mode` - Sets the journal mode. Turso stores data in WAL mode
      (`:wal`) or MVCC mode (`:mvcc`); `:mvcc` enables `BEGIN CONCURRENT` and
      is required for S3 durability. SQLite's other journal modes are accepted
      and have no effect. Switching an existing database that has
      `AUTOINCREMENT` tables to `:mvcc` is refused (see the README).
    * `:temp_store` - `:default`, `:file` or `:memory`.
    * `:synchronous` - Can be `:extra`, `:full`, `:normal`, or `:off`. Defaults
      to `:normal`.
    * `:foreign_keys` - Sets if foreign key checks should be enforced or not.
      Can be `:on` or `:off`. Default is `:on`.
    * `:cache_size` - Sets the cache size to be used for the connection.
      Default is `-2000`.
    * `:cache_spill` - `:on` (default) or `:off`.
    * `:auto_vacuum` - Defaults to `:none`. `:full` and `:incremental` require
      `experimental: [:autovacuum]`.
    * `:locking_mode` - Turso always locks the database file exclusively per
      process; `:normal` (default) is accepted and not applied.
    * `:busy_timeout` - Sets the busy timeout in milliseconds for a
      connection. Default is `2000` (`15_000` with `:s3`, where a writer
      holds the lock while each commit uploads). Set it to `0` to make lock
      contention fail immediately.
    * `:chunk_size` - The chunk size for bulk fetching. Defaults to `50`.
    * `:encryption` - `[cipher: "aegis256", key: "<hex key>"]` to open an
      encrypted database (turso extension). With `:s3` it is required: a
      key, or `false` to store the database unencrypted.
    * `:mvcc_checkpoint_threshold` - MVCC log bytes between automatic
      checkpoints; defaults to 256 KiB for non-S3 MVCC databases. See
      `Sediment.Engine.open/2`.
    * `:experimental` - experimental turso features to enable, see
      `Sediment.Engine.open/2`.
    * `:s3` - S3 durability configuration (sediment extension).
    * `:custom_pragmas` - A list of custom pragmas to set on the connection.
    * `:serialized` - A database which was previously serialized, to load into
      the database after connection.
    * `:before_disconnect` - A function to run before disconnect, either a
      2-arity fun or `{module, function, args}` with the close reason and
      `t:Sediment.Connection.t/0` prepended to `args` or `nil` (default: `nil`)

  The SQLite tuning pragmas `:case_sensitive_like`, `:secure_delete`,
  `:wal_auto_check_point`, `:journal_size_limit`, `:soft_heap_limit`,
  `:hard_heap_limit` and `:progress_handler_steps` are accepted for
  compatibility; turso does not implement them.

  `:load_extensions`, `:authorizer` and `:key` are not supported and make
  `connect/1` fail if given.

  ## Cancellation notes

  Connection teardown uses `Sediment.Engine.cancel/1`, so DBConnection
  timeouts and disconnects break out of both long-running statements and
  busy waits.
  """
  @spec connect([connection_opt()]) :: {:ok, t()} | {:error, Exception.t()}
  def connect(options) do
    database = Keyword.get(options, :database)

    options =
      Keyword.put_new(
        options,
        :chunk_size,
        Application.get_env(:sediment, :default_chunk_size, 50)
      )

    case database do
      nil ->
        {:error,
         %Error{
           message: """
           You must provide a :database to the database. \
           Example: connect(database: "./") or connect(database: :memory)\
           """
         }}

      :memory ->
        do_connect(":memory:", options)

      _ ->
        do_connect(database, options)
    end
  end

  @impl true
  def disconnect(err, %__MODULE__{db: db} = state) do
    Sediment.Telemetry.disconnect(err, state.path)

    if state.before_disconnect != nil do
      apply_before_disconnect(state.before_disconnect, err, state)
    end

    Engine.cancel(db)

    case Engine.close(db) do
      :ok -> :ok
      {:error, reason} -> {:error, %Error{message: to_string(reason)}}
    end
  end

  defp apply_before_disconnect({module, function, args}, err, state),
    do: apply(module, function, [err, state | args])

  defp apply_before_disconnect(fun, err, state), do: fun.(err, state)

  @impl true
  def checkout(%__MODULE__{status: :idle} = state) do
    {:ok, %{state | status: :busy}}
  end

  def checkout(%__MODULE__{status: :busy} = state) do
    {:disconnect, %Error{message: "Database is busy"}, state}
  end

  @impl true
  def ping(state) do
    with {:ok, _} <- Engine.transaction_status(state.db),
         :ok <- Native.s3_check(state.db) do
      {:ok, state}
    else
      {:error, reason} -> {:disconnect, %Error{message: to_string(reason)}, state}
    end
  end

  ##
  ## Handlers
  ##

  @impl true
  def handle_prepare(%Query{} = query, options, state) do
    metadata = %{query: query.statement, database: state.path}

    :telemetry.span([:sediment, :prepare], metadata, fn ->
      result = query |> prepare_cached(options, state) |> disconnect_if_fenced()
      {result, Map.put(metadata, :result, prepare_result(result))}
    end)
  end

  defp prepare_result({:ok, query, _state}), do: {:ok, query}
  defp prepare_result({_error_or_disconnect, error, _state}), do: {:error, error}

  @impl true
  def handle_execute(%Query{command: {:sediment_s3, op}} = query, _params, _options, state) do
    {result, state} = s3_operation(op, state)
    {:ok, query, Result.new(command: :execute, rows: [[result]], num_rows: 1), state}
  end

  def handle_execute(%Query{} = query, _params, _options, %{transaction_status: :error} = state) do
    # The transaction was already rolled back natively; only a ROLLBACK
    # issued as a plain query is accepted, to acknowledge it.
    if rollback_statement?(query.statement) do
      {:ok, query, Result.new(command: :execute, rows: [], num_rows: 0),
       %{state | transaction_status: :idle}}
    else
      {:error, aborted_error(query.statement), state}
    end
  end

  def handle_execute(%Query{} = query, params, options, state) do
    metadata = %{query: query.statement, params: params, database: state.path}

    :telemetry.span([:sediment, :query], metadata, fn ->
      result = do_handle_execute(query, params, options, state)
      {result, Map.put(metadata, :result, query_result(result))}
    end)
  end

  defp query_result({:ok, _query, result, _state}), do: {:ok, result}
  defp query_result({_error_or_disconnect, error, _state}), do: {:error, error}

  defp do_handle_execute(query, params, options, state) do
    result =
      with {:ok, state} <- maybe_refresh(state),
           {:ok, state} <- begin_pending(query.statement, state),
           {:ok, query, state} <- prepare_cached(query, options, state) do
        query
        |> execute_cached(params, options, state)
        |> reconcile_on_error()
      end

    # An autocommit statement asked for sync: true (inside a transaction the
    # transaction's own option applies at COMMIT)
    result |> sync_if_asked(options) |> disconnect_if_fenced()
  end

  # With async S3 durability a commit returns once it is in the local log;
  # `sync: true` waits until this statement's or transaction's own commit is
  # durable in S3 too (commits upload in order, so earlier ones are too). A
  # statement or transaction that committed nothing, a read, returns at once.
  defp sync_if_asked({:ok, _result, %{transaction_status: :idle} = state} = ok, options) do
    case s3_sync(state, options) do
      :ok -> ok
      {:error, error} -> {:error, error, state}
    end
  end

  defp sync_if_asked({:ok, query, _result, %{transaction_status: :idle} = state} = ok, options) do
    case s3_sync(state, options) do
      :ok -> ok
      {:error, error} -> {:error, %{error | statement: query.statement}, state}
    end
  end

  defp sync_if_asked(result, _options), do: result

  # Whether the database is S3-backed is the native side's to say: a plain
  # connection to a file whose S3 database is open here shares its storage.
  defp s3_sync(state, options) do
    if Keyword.get(options, :sync, false) do
      timeout = Keyword.get(options, :sync_timeout, Keyword.get(options, :timeout, 15_000))

      case S3.flush_commit(state.db, timeout) do
        :ok ->
          :ok

        {:error, reason} ->
          message = "committed locally, but not known to be durable in S3: #{reason}"
          {:error, %Error{message: message}}
      end
    else
      :ok
    end
  end

  # A cached statement finalized behind our back (deserialize/3 on the same
  # handle) is prepared again once.
  defp execute_cached(query, params, options, state) do
    case execute(:execute, query, params, state) do
      {:error, %Error{message: "invalid_statement"}, state} ->
        state = %{state | stmt_cache: Map.delete(state.stmt_cache, sql(query))}

        with {:ok, query, state} <- prepare_cached(query, options, state) do
          execute(:execute, query, params, state)
        end

      result ->
        result
    end
  end

  # Turso's prepare is expensive compared with SQLite's, so each connection
  # keeps its most recent statements, keyed by SQL. Streams (handle_declare)
  # prepare their own.
  @stmt_cache_size 64

  defp prepare_cached(query, options, state) do
    query = maybe_put_command(query, options)
    sql = sql(query)

    # A hit needs no cacheable?/1 check: uncacheable statements are never
    # stored.
    case state.stmt_cache do
      %{^sql => {_used, ref}} ->
        cache = Map.put(state.stmt_cache, sql, {used_now(), ref})
        {:ok, %{query | ref: ref}, %{state | stmt_cache: cache}}

      cache ->
        prepare_uncached(query, options, sql, cache, state)
    end
  end

  defp prepare_uncached(query, options, sql, cache, state) do
    if cacheable?(sql) do
      with {:ok, query} <- prepare(query, options, state) do
        cache = cache |> evict(state.db) |> Map.put(sql, {used_now(), query.ref})
        {:ok, query, %{state | stmt_cache: cache}}
      end
    else
      state = maybe_flush_stmt_cache(sql, state)
      with {:ok, query} <- prepare(query, options, state), do: {:ok, query, state}
    end
  end

  # turso_core compiles some connection settings into statements (CHECK
  # constraints are left out while ignore_check_constraints is on) without
  # re-preparing them when the setting changes, so a PRAGMA that sets a value
  # drops the cached statements.
  defp maybe_flush_stmt_cache(sql, state) do
    if String.contains?(sql, ["=", "("]) do
      Enum.each(state.stmt_cache, fn {_sql, {_used, ref}} -> Engine.release(state.db, ref) end)
      %{state | stmt_cache: %{}}
    else
      state
    end
  end

  defp used_now, do: System.unique_integer([:monotonic])

  # turso_core 0.8.1 computes PRAGMA results such as table_info when the
  # statement is prepared and doesn't re-prepare it after schema changes, so
  # a reused PRAGMA statement would return stale results.
  defp cacheable?(sql), do: first_keyword(sql) != "PRAGMA"

  # Least recently used goes first.
  defp evict(cache, db) when map_size(cache) >= @stmt_cache_size do
    {sql, {_used, ref}} = Enum.min_by(cache, fn {_sql, {used, _ref}} -> used end)
    Engine.release(db, ref)
    Map.delete(cache, sql)
  end

  defp evict(cache, _db), do: cache

  defp sql(%Query{statement: statement}), do: IO.iodata_to_binary(statement)

  defp cached?(state, ref), do: Enum.any?(state.stmt_cache, fn {_sql, {_used, r}} -> r == ref end)

  defp s3_operation(:info, state), do: {S3.info(state.db), state}
  defp s3_operation({:flush, timeout}, state), do: {S3.flush(state.db, timeout), state}
  defp s3_operation(:acknowledge_loss, state), do: {S3.acknowledge_loss(state.db), state}
  defp s3_operation(:snapshot, state), do: {S3.snapshot(state.db), state}

  defp s3_operation(:refresh, %{transaction_status: :idle} = state) do
    case S3.refresh(state.db) do
      {:ok, _info} = ok ->
        {ok, %{state | s3_generation: S3.bump_generation(state.path), stmt_cache: %{}}}

      error ->
        {error, state}
    end
  end

  defp s3_operation(:refresh, state),
    do: {{:error, "cannot refresh a replica inside a transaction"}, state}

  # Another connection's S3.refresh/1 asked every connection of the pool to
  # refresh before its next statement (outside a transaction). The generation
  # only counts as seen once the refresh succeeded, so a failed one is retried
  # by the next statement.
  defp maybe_refresh(%{transaction_status: :idle, s3_generation: seen} = state) do
    case S3.generation_at(state.generation_key) do
      generation when generation > seen -> refresh_to(generation, state)
      _ -> {:ok, state}
    end
  end

  defp maybe_refresh(state), do: {:ok, state}

  defp refresh_to(generation, state) do
    case S3.refresh(state.db) do
      {:ok, _} ->
        {:ok, %{state | s3_generation: generation, stmt_cache: %{}}}

      {:error, reason} ->
        error = %Error{message: "replica refresh failed: #{reason}"}

        # A refresh that failed after closing the old copy leaves no handle.
        case Engine.transaction_status(state.db) do
          {:ok, _} -> {:error, error, state}
          _ -> {:disconnect, error, state}
        end
    end
  end

  # A transaction begun in the default `:concurrent` mode starts at its first
  # statement: Turso refuses DDL in `BEGIN CONCURRENT`, so a transaction that
  # starts with DDL (such as a migration) begins with `BEGIN IMMEDIATE`.
  # Nothing ran before that statement, so this changes nothing else.
  defp begin_pending(statement, %{pending_begin: true} = state) do
    begin =
      if ddl?(statement),
        do: "BEGIN IMMEDIATE TRANSACTION",
        else: "BEGIN CONCURRENT TRANSACTION"

    case Engine.execute(state.db, begin) do
      :ok ->
        {:ok, %{state | pending_begin: false}}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: begin},
         %{state | pending_begin: false, transaction_status: :error}}
    end
  end

  defp begin_pending(_statement, state), do: {:ok, state}

  @ddl_keywords ~w(CREATE ALTER DROP REINDEX)

  defp ddl?(statement), do: first_keyword(statement) in @ddl_keywords

  defp first_keyword(statement) do
    statement
    |> IO.iodata_to_binary()
    |> strip_leading_comments()
    |> leading_letters("")
    |> String.upcase()
  end

  defp leading_letters(<<c, rest::binary>>, acc) when c in ?a..?z or c in ?A..?Z,
    do: leading_letters(rest, <<acc::binary, c>>)

  defp leading_letters(_rest, acc), do: acc

  defp strip_leading_comments(sql) do
    case String.trim_leading(sql) do
      "--" <> rest ->
        rest |> String.split("\n", parts: 2) |> Enum.at(1, "") |> strip_leading_comments()

      "/*" <> rest ->
        rest |> String.split("*/", parts: 2) |> Enum.at(1, "") |> strip_leading_comments()

      # Turso accepts empty statements before the first real one
      ";" <> rest ->
        strip_leading_comments(rest)

      trimmed ->
        trimmed
    end
  end

  defp rollback_statement?(statement) do
    statement
    |> IO.iodata_to_binary()
    |> String.trim_leading()
    |> String.upcase()
    |> String.starts_with?("ROLLBACK")
  end

  defp aborted_error(statement) do
    %Error{
      message:
        "the transaction was rolled back by the database (for example after a " <>
          "write-write conflict or ON CONFLICT ROLLBACK); roll it back before " <>
          "running more statements",
      statement: statement
    }
  end

  # Some errors end the native transaction (a write-write conflict under
  # MVCC, ON CONFLICT ROLLBACK). Mark the DBConnection transaction failed so
  # later statements don't silently run in autocommit mode.
  defp reconcile_on_error({:error, error, %{transaction_status: :transaction} = state}) do
    case Engine.transaction_status(state.db) do
      {:ok, :idle} -> {:error, error, %{state | transaction_status: :error}}
      _ -> {:error, error, state}
    end
  end

  defp reconcile_on_error(result), do: result

  # A fenced S3 writer (another writer took over, or an upload's outcome is
  # unknown) can't go on with this connection: disconnect, so DBConnection
  # reconnects. That restores from S3 once no pooled connection holds the
  # fenced storage anymore (idle ones notice on their next ping).
  #
  # Likewise after turso panicked: the NIF closed the connection.
  defp disconnect_if_fenced({:error, %Error{message: message} = error, state})
       when is_binary(message) do
    cond do
      String.contains?(message, "s3 writer fenced") or
          String.starts_with?(message, "internal turso error:") ->
        {:disconnect, error, state}

      # disconnect/2 closed the handle (the client timed out): the error may
      # be the interrupt, or a statement finalized with the connection.
      closed?(state) ->
        message = if message == "invalid_statement", do: "connection_closed", else: message
        {:disconnect, %{error | message: message}, state}

      true ->
        {:error, error, state}
    end
  end

  defp disconnect_if_fenced(result), do: result

  defp closed?(state), do: match?({:error, _}, Engine.transaction_status(state.db))

  @begin_statements %{
    deferred: "BEGIN TRANSACTION",
    transaction: "BEGIN TRANSACTION",
    immediate: "BEGIN IMMEDIATE TRANSACTION",
    exclusive: "BEGIN EXCLUSIVE TRANSACTION",
    concurrent: "BEGIN CONCURRENT TRANSACTION"
  }

  @doc """
  Begin a transaction.

  Note: default transaction mode is DEFERRED. `:concurrent` issues
  `BEGIN CONCURRENT` (MVCC journal mode only).
  """
  @impl true
  def handle_begin(options, %{transaction_status: :idle} = state) do
    case Keyword.fetch(options, :mode) do
      :error when state.default_transaction_mode == :concurrent ->
        {:ok, transaction_result(:begin),
         %{state | transaction_status: :transaction, pending_begin: true}}

      _ ->
        mode = Keyword.get(options, :mode, state.default_transaction_mode)
        handle_transaction(:begin, Map.fetch!(@begin_statements, mode), state)
    end
  end

  def handle_begin(options, %{transaction_status: :transaction} = state) do
    mode = Keyword.get(options, :mode, state.default_transaction_mode)

    if mode in [:deferred, :immediate, :exclusive, :concurrent, :savepoint] do
      with {:ok, state} <- begin_pending("SAVEPOINT", state) do
        handle_transaction(:begin, "SAVEPOINT sediment_savepoint", state)
      end
    else
      raise ArgumentError, "unsupported transaction mode inside a transaction: #{inspect(mode)}"
    end
  end

  def handle_begin(_options, %{transaction_status: :error} = state), do: {:error, state}

  @impl true
  def handle_commit(_options, %{transaction_status: :error} = state), do: {:error, state}

  def handle_commit(_options, %{pending_begin: true} = state),
    do:
      {:ok, transaction_result(:commit),
       %{state | transaction_status: :idle, pending_begin: false}}

  def handle_commit(options, %{transaction_status: transaction_status} = state) do
    case Keyword.get(options, :mode, :deferred) do
      :savepoint when transaction_status == :transaction ->
        handle_transaction(
          :commit_savepoint,
          "RELEASE SAVEPOINT sediment_savepoint",
          state
        )

      mode
      when mode in [:deferred, :immediate, :exclusive, :concurrent, :transaction] and
             transaction_status == :transaction ->
        state |> commit() |> sync_if_asked(options) |> disconnect_if_fenced()

      # A COMMIT or ROLLBACK run as a query already ended the transaction.
      _mode when transaction_status == :idle ->
        {:idle, state}
    end
  end

  # Under MVCC a write-write conflict can surface at COMMIT; the database has
  # then rolled the transaction back and the connection is still usable.
  defp commit(state) do
    state |> commit_once() |> disconnect_if_fenced()
  end

  defp commit_once(state) do
    case handle_transaction(:commit, "COMMIT", state) do
      {:disconnect, error, state} = disconnect ->
        case Engine.transaction_status(state.db) do
          {:ok, :idle} -> {:error, error, %{state | transaction_status: :idle}}
          _ -> disconnect
        end

      other ->
        other
    end
  end

  @impl true
  def handle_rollback(options, %{transaction_status: transaction_status} = state) do
    case Keyword.get(options, :mode, :deferred) do
      # The whole transaction is gone; the outer rollback acknowledges it.
      :savepoint when transaction_status == :error ->
        {:ok, transaction_result(:rollback_savepoint), state}

      :savepoint when transaction_status == :idle ->
        {:idle, state}

      :savepoint when transaction_status == :transaction ->
        with {:ok, _result, state} <-
               handle_transaction(
                 :rollback_savepoint,
                 "ROLLBACK TO SAVEPOINT sediment_savepoint",
                 state
               ) do
          handle_transaction(
            :rollback_savepoint,
            "RELEASE SAVEPOINT sediment_savepoint",
            state
          )
        end

      mode
      when mode in [:deferred, :immediate, :exclusive, :concurrent, :transaction] ->
        rollback(state)
    end
  end

  # A transaction that never ran a statement, or that the database aborted,
  # has nothing to roll back.
  defp rollback(%{pending_begin: true} = state),
    do:
      {:ok, transaction_result(:rollback),
       %{state | transaction_status: :idle, pending_begin: false}}

  defp rollback(state) do
    case Engine.transaction_status(state.db) do
      {:ok, :idle} -> {:ok, transaction_result(:rollback), %{state | transaction_status: :idle}}
      _ -> handle_transaction(:rollback, "ROLLBACK TRANSACTION", state)
    end
  end

  @doc """
  Close a query prepared by `handle_prepare/3` with the database. Return
  `{:ok, result, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @impl true
  def handle_close(query, _opts, state) do
    unless cached?(state, query.ref), do: Engine.release(state.db, query.ref)
    {:ok, nil, state}
  end

  @impl true
  def handle_declare(
        %Query{statement: statement},
        _params,
        _opts,
        %{transaction_status: :error} = state
      ),
      do: {:error, aborted_error(statement), state}

  def handle_declare(%Query{} = query, params, opts, state) do
    # Cursors are emulated with a prepared statement that is stepped through,
    # so the query ref is the cursor.
    result =
      with {:ok, state} <- maybe_refresh(state),
           {:ok, state} <- begin_pending(query.statement, state),
           {:ok, query} <- prepare(query, opts, state),
           {:ok, query} <- bind_params(query, params, state) do
        {:ok, query, query.ref, state}
      end

    disconnect_if_fenced(result)
  end

  @impl true
  def handle_deallocate(%Query{} = query, _cursor, _opts, state) do
    Engine.release(state.db, query.ref)
    {:ok, nil, state}
  end

  @impl true
  def handle_fetch(%Query{} = query, cursor, opts, state) do
    query |> fetch(cursor, opts, state) |> reconcile_on_error() |> disconnect_if_fenced()
  end

  defp fetch(%Query{statement: statement}, cursor, opts, state) do
    chunk_size = opts[:chunk_size] || opts[:max_rows] || state.chunk_size

    case Engine.multi_step(state.db, cursor, chunk_size) do
      {:done, rows} ->
        {:halt, %Result{rows: rows, command: :fetch, num_rows: length(rows)}, state}

      {:rows, rows} ->
        {:cont, %Result{rows: rows, command: :fetch, num_rows: chunk_size}, state}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}

      :busy ->
        {:error, %Error{message: "Database is busy", statement: statement}, state}
    end
  end

  @impl true
  def handle_status(_opts, state) do
    {state.transaction_status, state}
  end

  ### ----------------------------------
  #     Internal functions and helpers
  ### ----------------------------------

  defp set_pragma(db, pragma_name, value) do
    Engine.execute(db, "PRAGMA #{pragma_name} = #{value}")
  end

  defp get_pragma(db, pragma_name) do
    {:ok, statement} = Engine.prepare(db, "PRAGMA #{pragma_name}")

    case Engine.fetch_all(db, statement) do
      {:ok, [[value]]} -> {:ok, value}
      _ -> :error
    end
  end

  defp maybe_set_pragma(db, pragma_name, value) do
    case get_pragma(db, pragma_name) do
      {:ok, ^value} -> :ok
      _ -> set_pragma(db, pragma_name, value)
    end
  end

  defp set_custom_pragmas(db, options) do
    # we can't use maybe_set_pragma because some pragmas
    # are required to be set before the database is e.g. decrypted.
    case Keyword.fetch(options, :custom_pragmas) do
      {:ok, list} -> do_set_custom_pragmas(db, list)
      _ -> :ok
    end
  end

  defp do_set_custom_pragmas(db, list) do
    Enum.reduce_while(list, :ok, fn {key, value}, :ok ->
      case set_pragma(db, key, value) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp set_temp_store(db, options) do
    set_pragma(db, "temp_store", Pragma.temp_store(options))
  end

  defp set_synchronous(db, options) do
    set_pragma(db, "synchronous", Pragma.synchronous(options))
  end

  defp set_foreign_keys(db, options) do
    set_pragma(db, "foreign_keys", Pragma.foreign_keys(options))
  end

  defp set_cache_size(db, options) do
    maybe_set_pragma(db, "cache_size", Pragma.cache_size(options))
  end

  defp set_cache_spill(db, options) do
    set_pragma(db, "cache_spill", Pragma.cache_spill(options))
  end

  defp set_auto_vacuum(db, options) do
    maybe_set_pragma(db, "auto_vacuum", Pragma.auto_vacuum(options))
  end

  # Turso always holds an exclusive lock on the database file per process and
  # rejects `locking_mode = NORMAL`.
  defp set_locking_mode(db, options) do
    case Pragma.locking_mode(options) do
      "NORMAL" -> :ok
      mode -> set_pragma(db, "locking_mode", mode)
    end
  end

  defp set_busy_timeout(db, options) do
    Engine.set_busy_timeout(db, Pragma.busy_timeout(options))
  end

  defp deserialize(db, options) do
    case Keyword.get(options, :serialized, nil) do
      nil -> :ok
      serialized -> Engine.deserialize(db, serialized)
    end
  end

  @unsupported_options [
    load_extensions: "loadable extensions are not supported by turso",
    authorizer: "the authorizer is not supported by turso",
    key: "use the :encryption option (cipher and hex key) instead of :key"
  ]

  defp check_unsupported_options(options) do
    Enum.find_value(@unsupported_options, :ok, fn {option, message} ->
      if Keyword.get(options, option, []) not in [nil, []] do
        {:error, "unsupported option #{inspect(option)}: #{message}"}
      end
    end)
  end

  defp open_options(options) do
    # The threshold is per database, and every connection's open sets it:
    # open with the custom pragma's value so a later connection's open
    # can't undo an earlier connection's pragma.
    options =
      case Keyword.fetch(options[:custom_pragmas] || [], :mvcc_checkpoint_threshold) do
        {:ok, threshold} when is_integer(threshold) ->
          Keyword.put(options, :mvcc_checkpoint_threshold, threshold)

        _ ->
          options
      end

    options
    |> Keyword.take([
      :mode,
      :journal_mode,
      :encryption,
      :experimental,
      :s3,
      :mvcc_checkpoint_threshold
    ])
    |> Keyword.update(:journal_mode, nil, &Pragma.journal_mode(journal_mode: &1))
    |> Keyword.reject(fn {key, value} -> is_nil(value) and key != :mvcc_checkpoint_threshold end)
  end

  defp do_connect(database, options) do
    # A supervisor shutdown then runs disconnect/2, which closes the database
    # before this process exits, so an open right after doesn't race a
    # deferred cleanup. DBConnection discards the EXIT messages this lets in.
    # Only in DBConnection's connection processes: connect/1 called directly
    # (Sediment.Basic.open/1) runs in the caller, whose flags are its own.
    if Process.get(:"$initial_call") == {DBConnection.Connection, :init, 1},
      do: Process.flag(:trap_exit, true)

    with :ok <- check_unsupported_options(options),
         {:ok, path, uri_mode} <- resolve_database(database),
         directory = resolve_directory(path),
         :ok <- mkdir_p(directory),
         open_options = open_options(Keyword.put_new(options, :mode, uri_mode)),
         {:ok, db} <- Engine.open(path, open_options),
         # Closed when this process dies, even without disconnect/2: statements
         # cached elsewhere must not keep the database (or an S3 lease) alive.
         :ok <- Native.monitor_owner(db),
         :ok <- setup(db, options) do
      state = %__MODULE__{
        db: db,
        default_transaction_mode: Keyword.get(options, :default_transaction_mode, :deferred),
        directory: directory,
        path: database,
        transaction_status: :idle,
        status: :idle,
        chunk_size: Keyword.get(options, :chunk_size),
        s3_generation: S3.generation(database),
        generation_key: S3.generation_key(database),
        before_disconnect: Keyword.get(options, :before_disconnect, nil)
      }

      {:ok, state}
    else
      {:error, reason} ->
        {:error, %Error{message: to_string(reason)}}
    end
  end

  defp setup(db, options) do
    with :ok <- set_custom_pragmas(db, options),
         :ok <- set_temp_store(db, options),
         :ok <- set_synchronous(db, options),
         :ok <- set_foreign_keys(db, options),
         :ok <- set_cache_size(db, options),
         :ok <- set_cache_spill(db, options),
         :ok <- set_auto_vacuum(db, options),
         :ok <- set_locking_mode(db, options),
         :ok <- set_busy_timeout(db, options),
         :ok <- deserialize(db, options) do
      :ok
    else
      error ->
        Engine.close(db)
        error
    end
  end

  @doc false
  @spec maybe_put_command(Query.t(), keyword()) :: Query.t()
  def maybe_put_command(query, options) do
    case Keyword.get(options, :command) do
      nil -> query
      command -> %{query | command: command}
    end
  end

  defp prepare(%Query{statement: statement} = query, options, state) do
    query = maybe_put_command(query, options)

    case Engine.prepare(state.db, IO.iodata_to_binary(statement)) do
      {:ok, ref} ->
        {:ok, %{query | ref: ref}}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  @spec maybe_changes(Engine.db(), Query.t()) :: integer() | nil
  defp maybe_changes(db, %Query{command: command})
       when command in [:update, :insert, :delete] do
    case Engine.changes(db) do
      {:ok, total} -> total
      _ -> nil
    end
  end

  defp maybe_changes(_, _), do: nil

  # when we have an empty list of columns, that signifies that
  # there was no possible return tuple (e.g., update statement without RETURNING)
  # and in that case, we return nil to signify no possible result.
  defp maybe_rows([], []), do: nil
  defp maybe_rows(rows, _cols), do: rows

  # One native call binds, runs the statement and returns everything the
  # result needs. The step-by-step path below handles named parameters (a
  # map) and argument counts that differ from the statement's
  # (constant-folded parameters).
  defp execute(call, %Query{ref: ref, statement: statement} = query, params, state)
       when ref != nil and is_list(params) do
    case Engine.run_prepared(state.db, ref, params) do
      {:ok, columns, rows, changes, transaction_status} ->
        {:ok, query, result(call, query, columns, rows, changes),
         %{state | transaction_status: transaction_status}}

      {:error, :parameter_count} ->
        execute_stepwise(call, query, params, state)

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  rescue
    e in ArgumentError ->
      {:error, %Error{message: Exception.message(e), statement: statement}, state}
  end

  defp execute(call, query, params, state), do: execute_stepwise(call, query, params, state)

  defp result(call, %Query{command: command}, columns, rows, changes)
       when command in [:delete, :insert, :update],
       do: Result.new(command: call, num_rows: changes, rows: maybe_rows(rows, columns))

  defp result(call, _query, columns, rows, _changes),
    do: Result.new(command: call, columns: columns, rows: rows, num_rows: length(rows))

  defp execute_stepwise(call, %Query{} = query, params, state) do
    with {:ok, query} <- bind_params(query, params, state),
         # Rows first: after a schema change turso re-prepares the statement
         # at its first step, and column names read before that are stale.
         {:ok, rows} <- get_rows(query, state),
         {:ok, columns} <- get_columns(query, state),
         {:ok, transaction_status} <- Engine.transaction_status(state.db),
         changes <- maybe_changes(state.db, query) do
      case query.command do
        command when command in [:delete, :insert, :update] ->
          {
            :ok,
            query,
            Result.new(
              command: call,
              num_rows: changes,
              rows: maybe_rows(rows, columns)
            ),
            %{state | transaction_status: transaction_status}
          }

        _ ->
          {
            :ok,
            query,
            Result.new(
              command: call,
              columns: columns,
              rows: rows,
              num_rows: Enum.count(rows)
            ),
            %{state | transaction_status: transaction_status}
          }
      end
    else
      # The handle was closed under us, e.g. by a disconnect after the
      # client timed out.
      {:error, reason} when is_atom(reason) or is_binary(reason) ->
        {:error, %Error{message: to_string(reason), statement: query.statement}, state}

      error ->
        error
    end
  end

  defp bind_params(%Query{ref: ref, statement: statement} = query, params, state)
       when ref != nil do
    Engine.bind(ref, params)
  rescue
    e -> {:error, %Error{message: Exception.message(e), statement: statement}, state}
  else
    :ok ->
      {:ok, query}

    {:error, reason} ->
      {:error, %Error{message: to_string(reason), statement: statement}, state}
  end

  defp get_columns(%Query{ref: ref, statement: statement}, state) do
    case Engine.columns(state.db, ref) do
      {:ok, columns} ->
        {:ok, columns}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  defp get_rows(%Query{ref: ref, statement: statement}, state) do
    case Engine.fetch_all(state.db, ref, state.chunk_size) do
      {:ok, rows} ->
        {:ok, rows}

      {:error, reason} ->
        {:error, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  defp handle_transaction(call, statement, state) do
    with :ok <- Engine.execute(state.db, statement),
         {:ok, transaction_status} <- Engine.transaction_status(state.db) do
      {:ok, transaction_result(call), %{state | transaction_status: transaction_status}}
    else
      {:error, reason} ->
        {:disconnect, %Error{message: to_string(reason), statement: statement}, state}
    end
  end

  defp transaction_result(call), do: %Result{command: call, rows: [], columns: [], num_rows: 0}

  # Turso opens plain paths, so `file:` URIs are translated into a path and
  # an open mode here.
  defp resolve_database("file:" <> _ = uri) do
    %URI{path: path, query: query} = URI.parse(uri)
    params = URI.decode_query(query || "")

    case {path, params["mode"]} do
      {_, "memory"} -> {:ok, ":memory:", nil}
      {path, _} when path in [nil, ""] -> {:error, "No path in #{inspect(uri)}"}
      {path, "ro"} -> {:ok, path, :readonly}
      {path, "rw"} -> {:ok, path, :readwrite}
      {path, _} -> {:ok, path, nil}
    end
  end

  defp resolve_database(path), do: {:ok, path, nil}

  defp resolve_directory(":memory:"), do: nil
  defp resolve_directory(path), do: Path.dirname(path)

  # Opening with :create creates the database file but not missing
  # intermediate directories, so create those first.
  defp mkdir_p(nil), do: :ok
  defp mkdir_p(directory), do: File.mkdir_p(directory)
end
