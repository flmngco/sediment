defmodule Sediment.S3RestoreTargetTest do
  # Sediment.S3.restore/3's targets: a target of another form, or an option
  # restore/3 doesn't know, is an error that restores nothing (never the
  # latest state instead); :epoch is the whole epoch, :epoch_start its
  # snapshot alone. mix test --only s3
  use ExUnit.Case, async: false

  alias Sediment.Engine
  alias Sediment.S3

  @moduletag :s3

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  setup do
    prefix = "restore-target/#{System.unique_integer([:positive])}-#{System.os_time()}"
    dir = Path.join(System.tmp_dir!(), "s3-target-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    s3 = [
      bucket: Sediment.S3Bucket.name(),
      prefix: prefix,
      endpoint: Sediment.S3Bucket.endpoint(),
      region: Sediment.S3Bucket.region(),
      access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
      secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
      encryption: false,
      owner: "restore-target",
      lease_ttl_ms: 5_000,
      durability: :sync,
      retain_epochs: 4
    ]

    %{s3: s3, dir: dir}
  end

  defp ids(path) do
    {:ok, db} = Engine.open(path)
    {:ok, stmt} = Engine.prepare(db, "SELECT id FROM t ORDER BY id")
    {:ok, rows} = Engine.fetch_all(db, stmt)
    :ok = Engine.release(db, stmt)
    :ok = Engine.close(db)
    List.flatten(rows)
  end

  defp epoch(db) do
    {:ok, %{epoch: epoch}} = S3.info(db)
    epoch
  end

  # Epoch a starts with [1] and ends with [1, 2]; epoch b (current) starts
  # with [1, 2] and holds [1, 2, 3].
  defp history(s3, dir) do
    {:ok, db} = Engine.open(Path.join(dir, "live.db"), s3: s3)
    :ok = Engine.execute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Engine.execute(db, "INSERT INTO t VALUES (1)")
    :ok = S3.snapshot(db)
    a = epoch(db)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (2)")
    :ok = S3.snapshot(db)
    b = epoch(db)
    :ok = Engine.execute(db, "INSERT INTO t VALUES (3)")
    :ok = Engine.close(db)
    refute a == b
    {a, b}
  end

  defp seq(epoch), do: epoch |> String.split("-") |> hd() |> String.to_integer()

  test "an undecodable target or an unknown option is an error that restores nothing",
       %{s3: s3, dir: dir} do
    {a, _b} = history(s3, dir)
    path = Path.join(dir, "r.db")

    for opts <- [
          [epoch: "not an epoch"],
          [epoch: "1-2-3"],
          [epoch: "#{2 ** 64}-1"],
          [epoch: 2 ** 64],
          [epoch: -1],
          [epoch: 1.5],
          [epoch: nil],
          [epoch_start: "x"],
          [target: {:epoch, seq(a)}],
          [at: "2026-10-25T00:00:00Z"],
          [at: 0],
          [epoch: seq(a), at: DateTime.utc_now()]
        ] do
      assert {:error, message} = S3.restore(path, s3, opts), inspect(opts)
      assert message =~ "Sediment.S3.restore/3"
      refute File.exists?(path), inspect(opts)
    end

    # A generation that isn't the retained epoch's: not that epoch.
    [seq, _generation] = String.split(a, "-")
    assert {:error, "s3 config: epoch " <> _} = S3.restore(path, s3, epoch: "#{seq}-999")
    refute File.exists?(path)
  end

  test "the NIF refuses any other target shape", %{s3: s3, dir: dir} do
    {a, _b} = history(s3, dir)
    path = Path.join(dir, "r.db")

    for target <- [{:epoch, -1}, {:epoch, 2 ** 64}, {:epoch, "1"}, {:other, 1}, :newest, {:time}] do
      assert {:error, "restore target must be" <> _} =
               Sediment.Native.s3_restore(path, s3, target, nil)

      refute File.exists?(path)
    end

    assert {:ok, %{epoch: ^a}} = Sediment.Native.s3_restore(path, s3, {:epoch, seq(a)}, nil)
  end

  test ":epoch is the whole epoch, :epoch_start its snapshot alone", %{s3: s3, dir: dir} do
    {a, b} = history(s3, dir)

    restore = fn name, opts ->
      {S3.restore(Path.join(dir, name), s3, opts), Path.join(dir, name)}
    end

    for {name, opts, expected, epoch} <- [
          {"a-start.db", [epoch_start: seq(a)], [1], a},
          {"a-whole.db", [epoch: seq(a)], [1, 2], a},
          {"a-printed.db", [epoch: a], [1, 2], a},
          {"a-start-printed.db", [epoch_start: a], [1], a},
          {"b-start.db", [epoch_start: seq(b)], [1, 2], b},
          {"b-whole.db", [epoch: seq(b)], [1, 2, 3], b},
          {"latest.db", [], [1, 2, 3], b}
        ] do
      {result, path} = restore.(name, opts)
      assert {:ok, %{epoch: ^epoch}} = result, name
      assert ids(path) == expected, name
    end

    assert {:ok, %{frames: 0}} = S3.restore(Path.join(dir, "frames.db"), s3, epoch_start: seq(b))
  end
end
