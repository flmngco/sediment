defmodule Sediment.Pragma do
  @moduledoc """
  Handles parsing extra options for the turso connection
  """

  @doc "Returns `:busy_timeout` in milliseconds (default 2000) from the connection options."
  @spec busy_timeout(keyword() | nil) :: non_neg_integer()
  def busy_timeout(nil), do: busy_timeout([])

  def busy_timeout(options) do
    # An S3 writer holds the write lock while each commit uploads, so waits
    # for it are measured in S3 round trips, not local writes.
    default = if options[:s3], do: 15_000, else: 2000
    Keyword.get(options, :busy_timeout, default)
  end

  @doc "Returns the `:journal_mode` pragma value from the connection options."
  @spec journal_mode(keyword() | nil) :: String.t()
  def journal_mode(nil), do: journal_mode([])

  def journal_mode(options) do
    case Keyword.get(options, :journal_mode, :delete) do
      :delete -> "delete"
      :memory -> "memory"
      :off -> "off"
      :persist -> "persist"
      :truncate -> "truncate"
      :wal -> "wal"
      :mvcc -> "mvcc"
      _ -> raise ArgumentError, "invalid :journal_mode"
    end
  end

  @doc "Returns the `:temp_store` pragma value from the connection options."
  @spec temp_store(keyword() | nil) :: 0..2
  def temp_store(nil), do: temp_store([])

  def temp_store(options) do
    case Keyword.get(options, :temp_store, :default) do
      :file -> 1
      :memory -> 2
      :default -> 0
      _ -> raise ArgumentError, "invalid :temp_store"
    end
  end

  @doc "Returns the `:synchronous` pragma value from the connection options."
  @spec synchronous(keyword() | nil) :: 0..3
  def synchronous(nil), do: synchronous([])

  def synchronous(options) do
    case Keyword.get(options, :synchronous, :normal) do
      :extra -> 3
      :full -> 2
      :normal -> 1
      :off -> 0
      _ -> raise ArgumentError, "invalid :synchronous"
    end
  end

  @doc "Returns the `:foreign_keys` pragma value from the connection options."
  @spec foreign_keys(keyword() | nil) :: 0..1
  def foreign_keys(nil), do: foreign_keys([])

  def foreign_keys(options) do
    case Keyword.get(options, :foreign_keys, :on) do
      :off -> 0
      :on -> 1
      _ -> raise ArgumentError, "invalid :foreign_keys"
    end
  end

  @doc "Returns the `:cache_size` pragma value (default -2000) from the connection options."
  @spec cache_size(keyword() | nil) :: integer()
  def cache_size(nil), do: cache_size([])

  def cache_size(options) do
    Keyword.get(options, :cache_size, -2000)
  end

  @doc "Returns the `:cache_spill` pragma value from the connection options."
  @spec cache_spill(keyword() | nil) :: 0..1
  def cache_spill(nil), do: cache_spill([])

  def cache_spill(options) do
    case Keyword.get(options, :cache_spill, :on) do
      :off -> 0
      :on -> 1
      _ -> raise ArgumentError, "invalid :cache_spill"
    end
  end

  @doc "Returns the `:case_sensitive_like` pragma value from the connection options."
  @spec case_sensitive_like(keyword() | nil) :: 0..1
  def case_sensitive_like(nil), do: case_sensitive_like([])

  def case_sensitive_like(options) do
    case Keyword.get(options, :case_sensitive_like, :off) do
      :off -> 0
      :on -> 1
      _ -> raise ArgumentError, "invalid :case_sensitive_like"
    end
  end

  @doc "Returns the `:auto_vacuum` pragma value from the connection options."
  @spec auto_vacuum(keyword() | nil) :: 0..2
  def auto_vacuum(nil), do: auto_vacuum([])

  def auto_vacuum(options) do
    case Keyword.get(options, :auto_vacuum, :none) do
      :none -> 0
      :full -> 1
      :incremental -> 2
      _ -> raise ArgumentError, "invalid :auto_vacuum"
    end
  end

  @doc "Returns the `:locking_mode` pragma value from the connection options."
  @spec locking_mode(keyword() | nil) :: String.t()
  def locking_mode(nil), do: locking_mode([])

  def locking_mode(options) do
    case Keyword.get(options, :locking_mode, :normal) do
      :normal -> "NORMAL"
      :exclusive -> "EXCLUSIVE"
      _ -> raise ArgumentError, "invalid :locking_mode"
    end
  end

  @doc "Returns the `:secure_delete` pragma value from the connection options."
  @spec secure_delete(keyword() | nil) :: 0..1
  def secure_delete(nil), do: secure_delete([])

  def secure_delete(options) do
    case Keyword.get(options, :secure_delete, :off) do
      :off -> 0
      :on -> 1
      _ -> raise ArgumentError, "invalid :secure_delete"
    end
  end

  @doc "Returns the `:wal_auto_check_point` pragma value (default 1000) from the connection options."
  @spec wal_auto_check_point(keyword() | nil) :: integer()
  def wal_auto_check_point(nil), do: wal_auto_check_point([])

  def wal_auto_check_point(options) do
    Keyword.get(options, :wal_auto_check_point, 1000)
  end
end
