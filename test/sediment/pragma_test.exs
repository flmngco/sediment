defmodule Sediment.PragmaTest do
  use ExUnit.Case

  alias Sediment.Pragma

  test ".journal_mode/1" do
    assert Pragma.journal_mode(journal_mode: :truncate) == "truncate"
    assert Pragma.journal_mode(journal_mode: :persist) == "persist"
    assert Pragma.journal_mode(journal_mode: :memory) == "memory"
    assert Pragma.journal_mode(journal_mode: :wal) == "wal"
    assert Pragma.journal_mode(journal_mode: :off) == "off"
    assert Pragma.journal_mode(journal_mode: :delete) == "delete"
    assert Pragma.journal_mode([]) == "delete"
    assert Pragma.journal_mode(nil) == "delete"

    assert_raise(
      ArgumentError,
      "invalid :journal_mode",
      fn ->
        Pragma.journal_mode(journal_mode: :invalid)
      end
    )

    assert_raise(
      ArgumentError,
      "invalid :journal_mode",
      fn ->
        Pragma.journal_mode(journal_mode: "WAL")
      end
    )
  end

  test ".temp_store/1" do
    assert Pragma.temp_store(temp_store: :memory) == 2
    assert Pragma.temp_store(temp_store: :file) == 1
    assert Pragma.temp_store(temp_store: :default) == 0
    assert Pragma.temp_store([]) == 0
    assert Pragma.temp_store(nil) == 0

    assert_raise(
      ArgumentError,
      "invalid :temp_store",
      fn ->
        Pragma.temp_store(temp_store: :invalid)
      end
    )

    assert_raise(
      ArgumentError,
      fn ->
        Pragma.temp_store(temp_store: 1)
      end
    )
  end

  test ".synchronous/1" do
    assert Pragma.synchronous(synchronous: :extra) == 3
    assert Pragma.synchronous(synchronous: :full) == 2
    assert Pragma.synchronous(synchronous: :normal) == 1
    assert Pragma.synchronous(synchronous: :off) == 0
    assert Pragma.synchronous([]) == 1
    assert Pragma.synchronous(nil) == 1

    assert_raise(
      ArgumentError,
      "invalid :synchronous",
      fn ->
        Pragma.synchronous(synchronous: :invalid)
      end
    )
  end

  test ".foreign_keys/1" do
    assert Pragma.foreign_keys(foreign_keys: :on) == 1
    assert Pragma.foreign_keys(foreign_keys: :off) == 0
    assert Pragma.foreign_keys([]) == 1
    assert Pragma.foreign_keys(nil) == 1

    assert_raise(
      ArgumentError,
      "invalid :foreign_keys",
      fn ->
        Pragma.foreign_keys(foreign_keys: :invalid)
      end
    )
  end

  test ".cache_size/1" do
    assert Pragma.cache_size(cache_size: -64_000) == -64_000
    assert Pragma.cache_size([]) == -2_000
    assert Pragma.cache_size(nil) == -2_000
  end

  test ".cache_spill/1" do
    assert Pragma.cache_spill(cache_spill: :on) == 1
    assert Pragma.cache_spill(cache_spill: :off) == 0
    assert Pragma.cache_spill([]) == 1
    assert Pragma.cache_spill(nil) == 1

    assert_raise(
      ArgumentError,
      "invalid :cache_spill",
      fn ->
        Pragma.cache_spill(cache_spill: :invalid)
      end
    )
  end

  test ".case_sensitive_like/1" do
    assert Pragma.case_sensitive_like(case_sensitive_like: :on) == 1
    assert Pragma.case_sensitive_like(case_sensitive_like: :off) == 0
    assert Pragma.case_sensitive_like([]) == 0
    assert Pragma.case_sensitive_like(nil) == 0

    assert_raise(
      ArgumentError,
      "invalid :case_sensitive_like",
      fn ->
        Pragma.case_sensitive_like(case_sensitive_like: :invalid)
      end
    )
  end

  test ".auto_vacuum/1" do
    assert Pragma.auto_vacuum(nil) == 0
    assert Pragma.auto_vacuum(auto_vacuum: :full) == 1
    assert Pragma.auto_vacuum(auto_vacuum: :incremental) == 2
    assert_raise ArgumentError, fn -> Pragma.auto_vacuum(auto_vacuum: :invalid) end
  end

  test ".locking_mode/1" do
    assert Pragma.locking_mode(nil) == "NORMAL"
    assert Pragma.locking_mode(locking_mode: :exclusive) == "EXCLUSIVE"
    assert_raise ArgumentError, fn -> Pragma.locking_mode(locking_mode: :invalid) end
  end

  test ".secure_delete/1" do
    assert Pragma.secure_delete(nil) == 0
    assert Pragma.secure_delete(secure_delete: :on) == 1
    assert_raise ArgumentError, fn -> Pragma.secure_delete(secure_delete: :invalid) end
  end

  test ".busy_timeout/1 and .wal_auto_check_point/1" do
    assert Pragma.busy_timeout(nil) == 2000
    assert Pragma.busy_timeout(busy_timeout: 5) == 5
    assert Pragma.wal_auto_check_point(nil) == 1000
    assert Pragma.wal_auto_check_point(wal_auto_check_point: 0) == 0
  end

  test "nil options fall back to the defaults" do
    assert Pragma.journal_mode(nil) == "delete"
    assert Pragma.temp_store(nil) == 0
    assert Pragma.synchronous(nil) == 1
    assert Pragma.foreign_keys(nil) == 1
    assert Pragma.cache_size(nil) == -2000
    assert Pragma.cache_spill(nil) == 1
    assert Pragma.case_sensitive_like(nil) == 0
  end
end
