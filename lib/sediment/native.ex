defmodule Sediment.Native do
  @moduledoc """
  This is the module where all of the NIF entry points reside. Calling this directly
  should be avoided unless you are aware of what you are doing.
  """

  version = Mix.Project.config()[:version]

  # Precompiled NIFs come from the GitHub release of this version, checked
  # against the checksum file in the Hex package. Only the Hex package has that
  # file, so any other source tree (a git checkout, a git or path dependency)
  # builds from source. SEDIMENT_BUILD=1 or 0 overrides either way.
  force_build =
    case System.get_env("SEDIMENT_BUILD") do
      value when value in ["1", "true"] -> true
      value when value in ["0", "false"] -> false
      _ -> not File.exists?(Path.expand("../../checksum-Elixir.Sediment.Native.exs", __DIR__))
    end

  use RustlerPrecompiled,
    otp_app: :sediment,
    crate: "sediment_nif",
    base_url: "https://github.com/flmngco/sediment/releases/download/v#{version}",
    force_build: force_build,
    version: version,
    nif_versions: ["2.15"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
      x86_64-pc-windows-msvc
    )

  @type db() :: reference()
  @type statement() :: reference()
  @type reason() :: atom() | String.t()
  @type row() :: list()

  @spec open(String.t(), map()) :: {:ok, db()} | {:error, reason()}
  def open(_path, _opts), do: :erlang.nif_error(:not_loaded)

  @spec close(db()) :: :ok | {:error, reason()}
  def close(_conn), do: :erlang.nif_error(:not_loaded)

  @spec close_interrupting(db()) :: :ok | {:error, reason()}
  def close_interrupting(_conn), do: :erlang.nif_error(:not_loaded)

  @spec interrupt(db()) :: :ok | {:error, reason()}
  def interrupt(_conn), do: :erlang.nif_error(:not_loaded)

  @spec cancel(db()) :: :ok | {:error, reason()}
  def cancel(_conn), do: :erlang.nif_error(:not_loaded)

  @spec clear_cancel(db()) :: :ok
  def clear_cancel(_conn), do: :erlang.nif_error(:not_loaded)

  @spec set_busy_timeout(db(), integer()) :: :ok | {:error, reason()}
  def set_busy_timeout(_conn, _timeout_ms), do: :erlang.nif_error(:not_loaded)

  @spec set_progress_handler_steps(db(), integer()) :: :ok | {:error, reason()}
  def set_progress_handler_steps(_conn, _steps), do: :erlang.nif_error(:not_loaded)

  # `{:sleep, ms}`: a statement waits out a busy backoff; sleep, then
  # `execute_resume/1` (see Sediment.Engine.execute/2).
  @spec execute(db(), String.t()) :: :ok | {:sleep, pos_integer()} | {:error, reason()}
  def execute(_conn, _sql), do: :erlang.nif_error(:not_loaded)

  @spec execute_resume(db()) :: :ok | {:sleep, pos_integer()} | {:error, reason()}
  def execute_resume(_conn), do: :erlang.nif_error(:not_loaded)

  @spec changes(db()) :: {:ok, integer()} | {:error, reason()}
  def changes(_conn), do: :erlang.nif_error(:not_loaded)

  @spec total_changes(db()) :: {:ok, integer()} | {:error, reason()}
  def total_changes(_conn), do: :erlang.nif_error(:not_loaded)

  @spec prepare(db(), String.t()) :: {:ok, statement()} | {:error, reason()}
  def prepare(_conn, _sql), do: :erlang.nif_error(:not_loaded)

  @spec step(db(), statement()) ::
          :done | :busy | {:row, row()} | {:sleep, pos_integer()} | {:error, reason()}
  def step(_conn, _statement), do: :erlang.nif_error(:not_loaded)

  @spec run_prepared(db(), statement(), list()) ::
          {:ok, [String.t()], [[term()]], non_neg_integer(), :idle | :transaction}
          | {:sleep, pos_integer(), [[term()]]}
          | {:error, :parameter_count | reason()}
  def run_prepared(_conn, _statement, _params), do: :erlang.nif_error(:not_loaded)

  @spec resume_prepared(db(), statement()) ::
          {:ok, [String.t()], [[term()]], non_neg_integer(), :idle | :transaction}
          | {:sleep, pos_integer(), [[term()]]}
          | {:error, reason()}
  def resume_prepared(_conn, _statement), do: :erlang.nif_error(:not_loaded)

  @spec multi_step(db(), statement(), integer()) ::
          :busy
          | {:rows, [row()]}
          | {:done, [row()]}
          | {:sleep, pos_integer(), [row()]}
          | {:error, reason()}
  def multi_step(_conn, _statement, _chunk_size), do: :erlang.nif_error(:not_loaded)

  @spec columns(db(), statement()) :: {:ok, [binary()]} | {:error, reason()}
  def columns(_conn, _statement), do: :erlang.nif_error(:not_loaded)

  @spec last_insert_rowid(db()) :: {:ok, integer()} | {:error, reason()}
  def last_insert_rowid(_conn), do: :erlang.nif_error(:not_loaded)

  @spec transaction_status(db()) :: {:ok, :idle | :transaction} | {:error, reason()}
  def transaction_status(_conn), do: :erlang.nif_error(:not_loaded)

  @spec release(db(), statement()) :: :ok | {:error, reason()}
  def release(_conn, _statement), do: :erlang.nif_error(:not_loaded)

  @spec reset(statement()) :: :ok | {:error, reason()}
  def reset(_stmt), do: :erlang.nif_error(:not_loaded)

  @spec bind_parameter_count(statement()) :: non_neg_integer() | {:error, reason()}
  def bind_parameter_count(_stmt), do: :erlang.nif_error(:not_loaded)

  @spec sql_parameter_count(statement()) :: non_neg_integer() | {:error, reason()}
  def sql_parameter_count(_stmt), do: :erlang.nif_error(:not_loaded)

  @spec bind_parameter_index(statement(), String.t()) :: non_neg_integer()
  def bind_parameter_index(_stmt, _name), do: :erlang.nif_error(:not_loaded)

  @spec bind_value_at(statement(), pos_integer(), term()) :: :ok | {:error, reason()}
  def bind_value_at(_stmt, _index, _value), do: :erlang.nif_error(:not_loaded)

  @spec bind_all(statement(), [term()]) :: :ok | {:error, reason()}
  def bind_all(_stmt, _values), do: :erlang.nif_error(:not_loaded)

  @spec serialize(db(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def serialize(_conn, _database), do: :erlang.nif_error(:not_loaded)

  @spec deserialize(db(), String.t(), binary()) :: :ok | {:error, reason()}
  def deserialize(_conn, _database, _serialized), do: :erlang.nif_error(:not_loaded)

  @spec s3_info(db()) :: {:ok, map()} | {:error, reason()}
  def s3_info(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_flush_snapshot(db()) :: {:ok, boolean()} | {:error, reason()}
  def s3_flush_snapshot(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_flush(db(), non_neg_integer()) :: {:ok, map()} | {:error, reason()}
  def s3_flush(_conn, _timeout_ms), do: :erlang.nif_error(:not_loaded)

  @spec s3_flush_commit(db(), non_neg_integer()) :: {:ok, :ok | nil} | {:error, reason()}
  def s3_flush_commit(_conn, _timeout_ms), do: :erlang.nif_error(:not_loaded)

  @spec s3_acknowledge_loss(db()) :: {:ok, map() | nil} | {:error, reason()}
  def s3_acknowledge_loss(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_flush_all(non_neg_integer()) :: :ok | {:error, [String.t()]}
  def s3_flush_all(_timeout_ms), do: :erlang.nif_error(:not_loaded)

  @spec s3_request_counts() :: [{String.t(), String.t(), non_neg_integer()}]
  def s3_request_counts, do: :erlang.nif_error(:not_loaded)

  @spec s3_replica_info(db()) :: {:ok, map()} | {:error, reason()}
  def s3_replica_info(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_refresh(db()) :: {:ok, map()} | {:error, reason()}
  def s3_refresh(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_restore(
          String.t(),
          keyword() | map(),
          :latest | {:epoch | :time, non_neg_integer()},
          {String.t(), String.t()} | nil
        ) :: {:ok, map()} | {:error, reason()}
  def s3_restore(_path, _opts, _target, _encryption), do: :erlang.nif_error(:not_loaded)

  @spec s3_import(
          String.t(),
          keyword() | map(),
          :checksum | :restore,
          {String.t(), String.t()} | nil,
          {String.t(), String.t()} | nil
        ) :: {:ok, map()} | {:error, reason()}
  def s3_import(_path, _opts, _verify, _encryption, _source_encryption),
    do: :erlang.nif_error(:not_loaded)

  @spec s3_exists(keyword() | map()) :: {:ok, boolean()} | {:error, reason()}
  def s3_exists(_opts), do: :erlang.nif_error(:not_loaded)

  @spec s3_destroy(keyword() | map(), boolean()) :: {:ok, map()} | {:error, reason()}
  def s3_destroy(_opts, _force), do: :erlang.nif_error(:not_loaded)

  @spec export_sqlite(
          String.t() | nil,
          String.t(),
          keyword() | map() | nil,
          {String.t(), String.t()} | nil,
          boolean()
        ) :: {:ok, map()} | {:error, reason()}
  def export_sqlite(_source, _dest, _from_s3, _encryption, _drop_fts),
    do: :erlang.nif_error(:not_loaded)

  @spec monitor_owner(db()) :: :ok
  def monitor_owner(_conn), do: :erlang.nif_error(:not_loaded)

  @spec s3_check(db()) :: :ok | {:error, reason()}
  def s3_check(_conn), do: :erlang.nif_error(:not_loaded)

  @doc false
  # {live connection resources, live statement resources}, for leak checks.
  @spec resource_counts() :: {integer(), integer()}
  def resource_counts, do: :erlang.nif_error(:not_loaded)

  @doc false
  # Test hook: makes the next step on `conn` panic inside the NIF's guard.
  @spec debug_panic_next_step(db()) :: :ok
  def debug_panic_next_step(_conn), do: :erlang.nif_error(:not_loaded)
end
