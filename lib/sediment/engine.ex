defmodule Sediment.Engine do
  @moduledoc """
  The interface to the NIF implementation.

  This mirrors `Exqlite.Sqlite3`: same function names, arities and return
  shapes. Functions that turso cannot support return `{:error, :not_supported}`.
  See the README section "Differences from exqlite".
  """

  alias Sediment.Native

  @type db() :: reference()
  @type statement() :: reference()
  @type reason() :: atom() | String.t()
  @type row() :: list()
  @type open_mode :: :readwrite | :readonly | :nomutex | :create
  @type journal_mode :: :delete | :truncate | :persist | :memory | :wal | :off | :mvcc
  @type open_opt ::
          {:mode, :readwrite | :readonly | :create | [open_mode()]}
          | {:journal_mode, journal_mode() | String.t()}
          | {:encryption, [cipher: String.t(), key: String.t()] | false}
          | {:experimental, [atom()]}
          | {:s3, keyword() | map()}
          | {:mvcc_checkpoint_threshold, non_neg_integer() | nil}

  @doc """
  Opens a new turso database at the path provided.

  `path` can be `":memory:"` to keep the database in memory.

  ## Options

    * `:mode` - `:readwrite`, `:readonly`, `:create` or a list of them.
      Defaults to `[:readwrite, :create]` (opens for reading and writing and
      creates the file if it does not exist). `:readwrite` alone does not
      create the file. `:nomutex` is accepted for compatibility and ignored.

    * `:journal_mode` - journal mode applied right after opening, for
      example `:wal` or `:mvcc`. MVCC is required for `BEGIN CONCURRENT`
      and S3 durability. Switching an existing database that has
      `AUTOINCREMENT` tables to `:mvcc` is refused, because turso_core 0.8.1
      would reuse their ids and overwrite rows (see the README).

    * `:encryption` - `[cipher: "aegis256", key: "<hex key>"]` opens an
      encrypted database (turso extension). With `:s3` it is required:
      a key, or `false` to store the database unencrypted (see
      `Sediment.S3`).

    * `:experimental` - list of experimental turso features to enable, such
      as `:attach`, `:views`, `:vacuum`, `:generated_columns`,
      `:without_rowid`, `:index_method`, `:custom_types`, `:autovacuum`.
      `:attach` can't be combined with MVCC (see the Turso extensions guide).

    * `:s3` - S3 durability configuration (turso extension).

    * `:mvcc_checkpoint_threshold` - bytes of MVCC logical log after which
      turso checkpoints automatically. Defaults to `262_144` (256 KiB) for
      databases in MVCC mode that are not S3-backed: with turso_core's own
      default (about 4 MB) commits get noticeably slower as the log grows.
      `nil` keeps turso's default. S3-backed databases use the `:s3`
      option's `:checkpoint_threshold` instead.
  """
  @spec open(String.t(), [open_opt()]) :: {:ok, db()} | {:error, reason()}
  def open(path, opts \\ []) do
    mode = opts[:mode] || [:readwrite, :create]

    with {:ok, conn} <- Native.open(path, native_open_opts(mode, opts)) do
      if opts[:s3], do: flush_s3_at_exit()
      set_mvcc_checkpoint_threshold(conn, opts)
    end
  end

  # Elixir's CLI ends scripts, `mix run` and Mix tasks with System.halt/1,
  # which closes no connection: async S3 commits still queued would be lost.
  # The first S3 open in a VM registers a hook that uploads them first.
  defp flush_s3_at_exit do
    key = {__MODULE__, :flush_s3_at_exit}

    unless :persistent_term.get(key, false) do
      :persistent_term.put(key, true)

      System.at_exit(fn _status -> flush_s3_now() end)
    end
  end

  defp flush_s3_now do
    with {:error, failed} <- Native.s3_flush_all(30_000) do
      Enum.each(failed, &IO.warn("S3 commits not durable at exit: " <> &1, []))
    end
  end

  @default_mvcc_checkpoint_threshold 262_144

  defp set_mvcc_checkpoint_threshold(conn, opts) do
    threshold = Keyword.get(opts, :mvcc_checkpoint_threshold, @default_mvcc_checkpoint_threshold)

    with true <- is_integer(threshold) and is_nil(opts[:s3]),
         {:ok, [["mvcc"]]} <- query_all(conn, "PRAGMA journal_mode"),
         {:error, _} = error <- execute(conn, "PRAGMA mvcc_checkpoint_threshold = #{threshold}") do
      close(conn)
      error
    else
      _ -> {:ok, conn}
    end
  end

  defp query_all(conn, sql) do
    with {:ok, stmt} <- prepare(conn, sql) do
      result = fetch_all(conn, stmt)
      release(conn, stmt)
      result
    end
  end

  defp native_open_opts(mode, opts) do
    modes = modes_from_mode(mode)

    %{
      readonly: :readonly in modes,
      create: :create in modes,
      journal_mode: journal_mode(opts[:journal_mode]),
      encryption: encryption(opts[:encryption]),
      experimental: Enum.map(opts[:experimental] || [], &to_string/1),
      s3: Sediment.S3.with_encryption_choice(opts[:s3], opts[:encryption])
    }
  end

  defp journal_mode(nil), do: nil
  defp journal_mode(mode), do: mode |> to_string() |> String.downcase()

  defp encryption(nil), do: nil
  defp encryption(false), do: nil
  defp encryption(opts), do: %{cipher: to_string(opts[:cipher]), key: to_string(opts[:key])}

  defp modes_from_mode(:nomutex) do
    raise ArgumentError,
          "expected mode to be `:readwrite` or `:readonly`, can't use a single :nomutex mode"
  end

  defp modes_from_mode(mode) when mode in [:readwrite, :readonly, :create],
    do: validate_modes([mode])

  defp modes_from_mode([_ | _] = modes), do: validate_modes(modes)

  defp modes_from_mode(mode) do
    raise ArgumentError,
          "expected mode to be `:readwrite`, `:readonly` or list of modes, but received #{inspect(mode)}"
  end

  defp validate_modes(modes) do
    Enum.each(modes, fn
      mode when mode in [:readwrite, :readonly, :nomutex, :create] ->
        :ok

      mode ->
        raise ArgumentError,
              "expected mode to be `:readwrite`, `:readonly`, `:nomutex` or `:create`, but received #{inspect(mode)}"
    end)

    modes
  end

  @doc """
  Closes the database and releases any underlying resources.
  """
  @spec close(db() | nil) :: :ok | {:error, reason()}
  def close(nil), do: :ok
  def close(conn), do: Native.close(conn)

  @doc """
  Interrupt a long-running query.

  Interrupts the statement currently executing on the connection. Use
  `cancel/1` to also abort a statement waiting for a lock.
  """
  @spec interrupt(db() | nil) :: :ok | {:error, reason()}
  def interrupt(nil), do: :ok
  def interrupt(conn), do: Native.interrupt(conn)

  @doc """
  Set the busy timeout in milliseconds.

  A timeout of `0` makes lock contention fail immediately. Larger values keep
  retrying until the timeout expires or the wait is cancelled with
  `cancel/1`.
  """
  @spec set_busy_timeout(db(), integer()) :: :ok | {:error, reason()}
  def set_busy_timeout(conn, timeout_ms), do: Native.set_busy_timeout(conn, timeout_ms)

  @doc """
  Accepted for compatibility with `Exqlite.Sqlite3`.

  Turso's interrupt reaches the running statement directly, so there is no
  progress handler to tune. Returns `:ok` for an open connection.
  """
  @spec set_progress_handler_steps(db(), integer()) :: :ok | {:error, reason()}
  def set_progress_handler_steps(conn, steps),
    do: Native.set_progress_handler_steps(conn, steps)

  @doc """
  Cancel a running query: abort both a running statement and a busy wait.

  It also applies to an operation whose call is under way but hasn't
  reached the database yet (waiting for a dirty scheduler under load). The
  next operation started through this module clears a cancel that nothing
  used, so after a cancel the connection can be reused normally.
  """
  @spec cancel(db() | nil) :: :ok | {:error, reason()}
  def cancel(nil), do: :ok
  def cancel(conn), do: Native.cancel(conn)

  @doc """
  Executes an sql script. Multiple stanzas can be passed at once.
  """
  @spec execute(db(), String.t()) :: :ok | {:error, reason()}
  def execute(conn, sql) do
    with :ok <- check_utf8(sql) do
      :ok = start_operation(conn)
      conn |> Native.execute(sql) |> resume_execute(conn)
    end
  end

  # A cancel/1 issued before an operation starts doesn't apply to it; one
  # issued from here on does, also while its native call waits for a
  # scheduler. The waits below (`{:sleep, ...}`) resume without clearing.
  defp start_operation(conn), do: Native.clear_cancel(conn)

  # The native calls hand busy backoffs back (`{:sleep, ms}`) instead of
  # sleeping on a dirty scheduler thread, which the lock holder may need to
  # commit: the caller's process sleeps, then the call resumes where it
  # stopped (the busy timeout still bounds the whole wait).
  defp resume_execute({:sleep, ms}, conn) do
    Process.sleep(ms)
    conn |> Native.execute_resume() |> resume_execute(conn)
  end

  defp resume_execute(result, _conn), do: result

  @doc """
  Get the number of changes recently.

  If triggers are used, the count may be larger than expected.
  """
  @spec changes(db()) :: {:ok, integer()} | {:error, reason()}
  def changes(conn), do: Native.changes(conn)

  @doc """
  Get the total number of changes since the connection was opened.
  """
  @spec total_changes(db()) :: {:ok, integer()} | {:error, reason()}
  def total_changes(conn), do: Native.total_changes(conn)

  @doc """
  Prepares a single SQL statement.
  """
  @spec prepare(db(), String.t()) :: {:ok, statement()} | {:error, reason()}
  def prepare(conn, sql) do
    with :ok <- check_utf8(sql), do: Native.prepare(conn, sql)
  end

  defp check_utf8(sql) when is_binary(sql) do
    if String.valid?(sql), do: :ok, else: {:error, "SQL is not valid UTF-8"}
  end

  defp check_utf8(_sql), do: :ok

  @doc """
  Resets a prepared statement.
  """
  @spec reset(statement) :: :ok | {:error, reason()}
  def reset(stmt), do: Native.reset(stmt)

  @doc """
  Returns number of SQL parameters in a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?, ?")
      iex> Engine.bind_parameter_count(stmt)
      2

  """
  @spec bind_parameter_count(statement) :: non_neg_integer() | {:error, reason()}
  def bind_parameter_count(stmt), do: Native.bind_parameter_count(stmt)

  @type bind_value ::
          NaiveDateTime.t()
          | DateTime.t()
          | Date.t()
          | Time.t()
          | number
          | iodata
          | {:blob, iodata}
          | atom

  @doc """
  Resets a prepared statement and binds values to it.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?, ?, ?, ?, ?")
      iex> Engine.bind(stmt, [42, 3.14, "Alice", {:blob, <<0, 0, 0>>}, nil])
      iex> Engine.step(conn, stmt)
      {:row, [42, 3.14, "Alice", <<0, 0, 0>>, nil]}

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT :answer, @pi, $name, @blob, :null")
      iex> Engine.bind(stmt, %{":answer" => 42, "@pi" => 3.14, "$name" => "Alice", :"@blob" => {:blob, <<0, 0, 0>>}, ~c":null" => nil})
      iex> Engine.step(conn, stmt)
      {:row, [42, 3.14, "Alice", <<0, 0, 0>>, nil]}

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind(stmt, [42, 3.14, "Alice"])
      ** (ArgumentError) expected 1 arguments, got 3

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?, ?")
      iex> Engine.bind(stmt, [42])
      ** (ArgumentError) expected 2 arguments, got 1

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind(stmt, [:erlang.list_to_pid(~c"<0.0.0>")])
      ** (ArgumentError) unsupported type: #PID<0.0.0>

  """
  @spec bind(statement, [bind_value] | %{optional(String.t()) => bind_value} | nil) ::
          :ok | {:error, reason()}
  def bind(stmt, nil), do: bind(stmt, [])

  def bind(stmt, [_ | _] = args) do
    # One native call when the argument count matches, the usual case.
    case Native.bind_all(stmt, Enum.map(args, &normalize/1)) do
      {:error, :parameter_count} -> bind_checked(stmt, args)
      result -> raise_on_bind_error(result)
    end
  end

  def bind(stmt, args) when is_list(args), do: bind_checked(stmt, args)

  def bind(stmt, args) when is_map(args) do
    with params_count when is_integer(params_count) <- bind_parameter_count(stmt) do
      args_count = map_size(args)

      if args_count != params_count do
        raise ArgumentError,
              "expected #{params_count} named arguments, got #{args_count}: #{inspect(Map.keys(args))}"
      end

      :ok = Native.bind_all(stmt, [])
      Enum.each(args, fn {name, param} -> bind_named(stmt, name, param) end)
    end
  end

  defp bind_checked(stmt, args) do
    with params_count when is_integer(params_count) <- bind_parameter_count(stmt) do
      args = check_args_count(stmt, args, params_count)

      stmt
      |> Native.bind_all(Enum.map(args, &normalize/1))
      |> raise_on_bind_error()
    end
  end

  # Turso drops trailing parameters that constant folding eliminated
  # (`WHERE 0 AND x = ?`) from the statement's parameter count. Arguments for
  # them are accepted, and dropped, when the SQL text has exactly that many
  # parameters.
  defp check_args_count(stmt, args, params_count) do
    args_count = length(args)

    cond do
      args_count == params_count ->
        args

      args_count > params_count and Native.sql_parameter_count(stmt) == args_count ->
        Enum.take(args, params_count)

      true ->
        raise ArgumentError, "expected #{params_count} arguments, got #{args_count}"
    end
  end

  defp bind_named(stmt, name, param) do
    idx = Native.bind_parameter_index(stmt, to_string(name))

    if idx == 0 do
      raise ArgumentError, "unknown named parameter: #{inspect(name)}"
    end

    stmt |> Native.bind_value_at(idx, normalize(param)) |> raise_on_bind_error()
  end

  @doc false
  # For Sediment.Connection: binds, runs the statement to completion and
  # returns its columns, rows, changes and transaction status in one native
  # call. `{:error, :parameter_count}` when `params` doesn't match the
  # statement's parameter count (use bind/2 then).
  @spec run_prepared(db(), statement(), list()) ::
          {:ok, [String.t()], [row()], non_neg_integer(), :idle | :transaction}
          | {:error, :parameter_count | reason()}
  def run_prepared(conn, statement, params) do
    :ok = start_operation(conn)

    conn
    |> Native.run_prepared(statement, Enum.map(params, &normalize/1))
    |> resume_prepared(conn, statement, [])
  rescue
    e in ErlangError -> handle_nif_exception(e, __STACKTRACE__)
  end

  # `chunks`: the rows returned before each wait, latest first.
  defp resume_prepared({:sleep, ms, rows}, conn, statement, chunks) do
    Process.sleep(ms)

    conn
    |> Native.resume_prepared(statement)
    |> resume_prepared(conn, statement, [rows | chunks])
  end

  defp resume_prepared({:ok, columns, rows, changes, status}, _conn, _statement, chunks),
    do: {:ok, columns, Enum.concat(Enum.reverse([rows | chunks])), changes, status}

  defp resume_prepared(result, _conn, _statement, _chunks), do: result

  defp raise_on_bind_error(:ok), do: :ok
  defp raise_on_bind_error({:error, :invalid_statement} = error), do: error

  defp raise_on_bind_error({:error, message}),
    do: raise(Sediment.Error, message: message)

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp normalize(param) do
    case convert(param) do
      i when is_integer(i) -> i
      f when is_float(f) -> f
      b when is_binary(b) -> b
      b when is_list(b) -> IO.iodata_to_binary(b)
      nil -> nil
      :undefined -> nil
      a when is_atom(a) -> Atom.to_string(a)
      {:blob, b} when is_binary(b) -> {:blob, b}
      {:blob, b} when is_list(b) -> {:blob, IO.iodata_to_binary(b)}
      _other -> raise ArgumentError, "unsupported type: #{inspect(param)}"
    end
  end

  @doc """
  Returns the column names of a prepared statement.
  """
  @spec columns(db(), statement()) :: {:ok, [binary()]} | {:error, reason()}
  def columns(conn, statement) do
    Native.columns(conn, statement)
  rescue
    e -> handle_nif_exception(e, __STACKTRACE__)
  end

  @doc """
  Runs the statement until it produces a row (`{:row, values}`) or finishes
  (`:done`). Returns `:busy` when the database stayed locked for the busy
  timeout. A finished statement starts over on the next call.
  """
  @spec step(db(), statement()) :: :done | :busy | {:row, row()} | {:error, reason()}
  def step(conn, statement) do
    :ok = start_operation(conn)
    do_step(conn, statement)
  rescue
    e -> handle_nif_exception(e, __STACKTRACE__)
  end

  defp do_step(conn, statement) do
    case Native.step(conn, statement) do
      {:sleep, ms} ->
        Process.sleep(ms)
        do_step(conn, statement)

      result ->
        result
    end
  end

  @doc """
  Like `multi_step/3` with the `:default_chunk_size` (50).
  """
  @spec multi_step(db(), statement()) ::
          :busy | {:rows, [row()]} | {:done, [row()]} | {:error, reason()}
  def multi_step(conn, statement) do
    chunk_size = Application.get_env(:sediment, :default_chunk_size, 50)
    multi_step(conn, statement, chunk_size)
  end

  @doc """
  Steps up to `chunk_size` rows at once: `{:rows, rows}` when more remain,
  `{:done, rows}` when the statement finished.
  """
  @spec multi_step(db(), statement(), integer()) ::
          :busy | {:rows, [row()]} | {:done, [row()]} | {:error, reason()}
  def multi_step(conn, statement, chunk_size) do
    :ok = start_operation(conn)
    do_multi_step(conn, statement, chunk_size)
  rescue
    e -> handle_nif_exception(e, __STACKTRACE__)
  end

  defp do_multi_step(conn, statement, chunk_size) do
    case Native.multi_step(conn, statement, chunk_size) do
      {:sleep, ms, rows} ->
        Process.sleep(ms)
        more = do_multi_step(conn, statement, chunk_size - length(rows))
        prepend_rows(rows, more)

      result ->
        result
    end
  end

  defp prepend_rows(rows, {:rows, more}), do: {:rows, rows ++ more}
  defp prepend_rows(rows, {:done, more}), do: {:done, rows ++ more}
  defp prepend_rows(_rows, other), do: other

  @doc """
  Returns the rowid of the most recent successful insert on the connection.
  """
  @spec last_insert_rowid(db()) :: {:ok, integer()} | {:error, reason()}
  def last_insert_rowid(conn), do: Native.last_insert_rowid(conn)

  @doc """
  Returns `{:ok, :transaction}` inside an explicit transaction, `{:ok, :idle}` otherwise.
  """
  @spec transaction_status(db()) :: {:ok, :idle | :transaction} | {:error, reason()}
  def transaction_status(conn), do: Native.transaction_status(conn)

  @doc """
  Causes the database connection to free as much memory as it can.
  """
  @spec shrink_memory(db()) :: :ok | {:error, reason()}
  def shrink_memory(conn), do: execute(conn, "PRAGMA shrink_memory")

  @doc """
  Steps the statement to completion and returns all rows, fetched `chunk_size` at a time.
  """
  @spec fetch_all(db(), statement(), integer()) :: {:ok, [row()]} | {:error, reason()}
  def fetch_all(conn, statement, chunk_size) do
    {:ok, try_fetch_all(conn, statement, chunk_size)}
  catch
    :throw, {:error, _reason} = error -> error
  end

  defp try_fetch_all(conn, statement, chunk_size) do
    case multi_step(conn, statement, chunk_size) do
      {:done, rows} -> rows
      {:rows, rows} -> rows ++ try_fetch_all(conn, statement, chunk_size)
      {:error, _reason} = error -> throw(error)
      :busy -> throw({:error, "Database busy"})
    end
  end

  @doc """
  Like `fetch_all/3` with the `:default_chunk_size` (50).
  """
  @spec fetch_all(db(), statement()) :: {:ok, [row()]} | {:error, reason()}
  def fetch_all(conn, statement) do
    chunk_size = Application.get_env(:sediment, :default_chunk_size, 50)
    fetch_all(conn, statement, chunk_size)
  end

  @doc """
  Serialize the contents of the database to a binary.

  Implemented with `VACUUM INTO`, so the result is a compacted copy of the
  database.
  """
  @spec serialize(db(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def serialize(conn, database \\ "main") do
    Native.serialize(conn, database)
  end

  @doc """
  Disconnect from database and then reopen as an in-memory database based on
  the serialized binary.

  Only the `"main"` database can be replaced. Statements prepared before the
  call belong to the old database and must be prepared again.
  """
  @spec deserialize(db(), String.t(), binary()) :: :ok | {:error, reason()}
  def deserialize(conn, database \\ "main", serialized) do
    Native.deserialize(conn, database, serialized)
  end

  @doc """
  Once finished with the prepared statement, call this to release the underlying
  resources.
  """
  @spec release(db(), statement() | nil) :: :ok | {:error, reason()}
  def release(_conn, nil), do: :ok

  def release(conn, statement) do
    Native.release(conn, statement)
  rescue
    e -> handle_nif_exception(e, __STACKTRACE__)
  end

  @doc """
  Not supported by turso: loadable SQLite extensions are not available.
  Always returns `{:error, :not_supported}`.
  """
  @spec enable_load_extension(db(), boolean()) :: :ok | {:error, reason()}
  def enable_load_extension(_conn, _flag), do: {:error, :not_supported}

  @doc """
  Not supported by turso: there is no update hook. Always returns
  `{:error, :not_supported}`.
  """
  @spec set_update_hook(db(), pid()) :: :ok | {:error, reason()}
  def set_update_hook(_conn, _pid), do: {:error, :not_supported}

  @doc """
  Not supported by turso: there is no authorizer. Always returns
  `{:error, :not_supported}`.
  """
  @spec set_authorizer(db(), [atom()]) :: :ok | {:error, reason()}
  def set_authorizer(_conn, deny_list) when is_list(deny_list), do: {:error, :not_supported}

  @doc """
  Not supported by turso: there is no SQLite error log. Always returns
  `{:error, :not_supported}`.
  """
  @spec set_log_hook(pid()) :: :ok | {:error, reason()}
  def set_log_hook(_pid), do: {:error, :not_supported}

  @doc """
  Binds a text value to a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind_text(stmt, 1, "Alice")
      :ok

  """
  @spec bind_text(statement, non_neg_integer, String.t()) :: :ok
  def bind_text(stmt, index, text) when is_binary(text), do: bind_at(stmt, index, text)
  def bind_text(_stmt, _index, text), do: raise(ArgumentError, "argument error: #{inspect(text)}")

  @doc """
  Binds a blob value to a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind_blob(stmt, 1, <<0, 0, 0>>)
      :ok

  """
  @spec bind_blob(statement, non_neg_integer, binary) :: :ok
  def bind_blob(stmt, index, blob) when is_binary(blob), do: bind_at(stmt, index, {:blob, blob})
  def bind_blob(_stmt, _index, blob), do: raise(ArgumentError, "argument error: #{inspect(blob)}")

  @doc """
  Binds an integer value to a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind_integer(stmt, 1, 42)
      :ok

  """
  @spec bind_integer(statement, non_neg_integer, integer) :: :ok
  def bind_integer(stmt, index, integer) when is_integer(integer),
    do: bind_at(stmt, index, integer)

  def bind_integer(_stmt, _index, integer),
    do: raise(ArgumentError, "argument error: #{inspect(integer)}")

  @doc """
  Binds a float value to a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind_float(stmt, 1, 3.14)
      :ok

  """
  @spec bind_float(statement, non_neg_integer, float) :: :ok
  def bind_float(stmt, index, float) when is_float(float), do: bind_at(stmt, index, float)

  def bind_float(_stmt, _index, float),
    do: raise(ArgumentError, "argument error: #{inspect(float)}")

  @doc """
  Binds a null value to a prepared statement.

      iex> {:ok, conn} = Engine.open(":memory:")
      iex> {:ok, stmt} = Engine.prepare(conn, "SELECT ?")
      iex> Engine.bind_null(stmt, 1)
      :ok

  """
  @spec bind_null(statement, non_neg_integer) :: :ok
  def bind_null(stmt, index), do: bind_at(stmt, index, nil)

  defp bind_at(stmt, index, value) when is_reference(stmt) do
    case Native.bind_value_at(stmt, index, value) do
      :ok -> :ok
      {:error, :invalid_statement} -> raise Sediment.Error, message: "invalid statement"
      {:error, message} -> raise Sediment.Error, message: message
    end
  end

  defp bind_at(stmt, _index, _value), do: raise(ArgumentError, "argument error: #{inspect(stmt)}")

  defp convert(%Date{} = val), do: Date.to_iso8601(val)
  defp convert(%Time{} = val), do: Time.to_iso8601(val)
  defp convert(%NaiveDateTime{} = val), do: NaiveDateTime.to_iso8601(val)
  defp convert(%DateTime{time_zone: "Etc/UTC"} = val), do: NaiveDateTime.to_iso8601(val)

  defp convert(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  defp convert(val) do
    convert_with_type_extensions(type_extensions(), val)
  end

  defp convert_with_type_extensions(nil, val), do: val
  defp convert_with_type_extensions([], val), do: val

  defp convert_with_type_extensions([extension | other_extensions], val) do
    case extension.convert(val) do
      nil ->
        convert_with_type_extensions(other_extensions, val)

      {:ok, converted} ->
        converted

      {:error, reason} ->
        raise ArgumentError,
              "Failed conversion by TypeExtension #{extension}: #{inspect(val)}. Reason: #{inspect(reason)}."
    end
  end

  defp type_extensions do
    Application.get_env(:sediment, :type_extensions)
  end

  defp handle_nif_exception(%ErlangError{original: :cross_connection_call}, _) do
    raise(ArgumentError,
      message: "Statement was prepared for a different connection, which is illegal"
    )
  end

  defp handle_nif_exception(e, stacktrace), do: reraise(e, stacktrace)
end
