defmodule Sediment.EncryptionSharingTest do
  # turso shares one Database per file in the VM, decrypted pages included:
  # a second open of a file that is already open must make the same
  # encryption choice.
  use ExUnit.Case, async: true

  alias Sediment.Engine

  @key "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"
  @enc [cipher: "aegis256", key: @key]
  @other [cipher: "aegis256", key: String.duplicate("00", 32)]

  defp query(conn, sql) do
    {:ok, stmt} = Engine.prepare(conn, sql)
    {:ok, rows} = Engine.fetch_all(conn, stmt)
    :ok = Engine.release(conn, stmt)
    rows
  end

  defp open_with_rows(opts) do
    path = Temp.path!()
    {:ok, conn} = Engine.open(path, opts)
    :ok = Engine.execute(conn, "create table secrets (v text)")
    :ok = Engine.execute(conn, "insert into secrets values ('hidden')")
    {path, conn}
  end

  describe "an open encrypted file" do
    for {name, opts} <- [
          {"another key", [encryption: @other]},
          {"no key", []},
          {"encryption: false", [encryption: false]}
        ] do
      test "refuses #{name}" do
        {path, conn} = open_with_rows(encryption: @enc)

        assert {:error, "" <> message} = Engine.open(path, unquote(opts))
        assert message =~ "is already open in this VM with another :encryption choice"

        # The first connection is unaffected.
        assert [["hidden"]] = query(conn, "select v from secrets")
        :ok = Engine.close(conn)
      end
    end

    test "is shared with the same key, also spelled in upper case" do
      {path, conn} = open_with_rows(encryption: @enc)
      upper = [cipher: "aegis256", key: String.upcase(@key)]

      for opts <- [[encryption: @enc], [encryption: upper]] do
        {:ok, other} = Engine.open(path, opts)
        assert [["hidden"]] = query(other, "select v from secrets")
        :ok = Engine.close(other)
      end

      :ok = Engine.close(conn)
    end

    test "can be opened with another choice once every connection is closed" do
      {path, conn} = open_with_rows(encryption: @enc)
      :ok = Engine.close(conn)

      # turso's own checks apply again: the file can't be read without its key.
      assert {:error, "File is not a database"} = Engine.open(path)
    end

    test "refuses a DBConnection pool with another key" do
      {path, conn} = open_with_rows(encryption: @enc)

      assert {:error, %Sediment.Error{message: message}} =
               Sediment.Connection.connect(database: path, encryption: @other)

      assert message =~ "another :encryption choice"

      {:ok, pool} = Sediment.start_link(database: path, encryption: @enc, pool_size: 2)
      assert %{rows: [["hidden"]]} = Sediment.query!(pool, "select v from secrets")
      GenServer.stop(pool)
      :ok = Engine.close(conn)
    end
  end

  describe "an open unencrypted file" do
    test "refuses a key and is shared without one" do
      {path, conn} = open_with_rows([])

      assert {:error, "" <> message} = Engine.open(path, encryption: @enc)
      assert message =~ "another :encryption choice"

      for opts <- [[], [encryption: false]] do
        {:ok, other} = Engine.open(path, opts)
        assert [["hidden"]] = query(other, "select v from secrets")
        :ok = Engine.close(other)
      end

      :ok = Engine.close(conn)
    end
  end
end
