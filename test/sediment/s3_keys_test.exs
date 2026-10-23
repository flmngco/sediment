defmodule Sediment.S3KeysTest do
  # Key rings without a server: option forms, and no key in an exception.
  use ExUnit.Case, async: true

  alias Sediment.Engine
  alias Sediment.S3
  alias Sediment.S3.Keys

  @k1 String.duplicate("1a", 32)
  @k2 String.duplicate("2b", 32)
  @secret "S3-SECRET-ACCESS-KEY-VALUE"

  defp ring, do: [keys: [{"r2", @k2}, {"r1", @k1}], cipher: "aegis256"]

  defp s3 do
    [
      bucket: "b",
      prefix: "p",
      endpoint: "http://127.0.0.1:1",
      access_key_id: "id",
      secret_access_key: @secret
    ]
  end

  # The exception, its message and every frame of its stack trace.
  defp raised(fun) do
    fun.()
    flunk("expected an exception")
  rescue
    exception -> {exception, Exception.format(:error, exception, __STACKTRACE__), __STACKTRACE__}
  end

  defp refute_secrets({exception, formatted, stacktrace}) do
    for secret <- [@k1, @k2, String.slice(@k1, 0, 16), @secret] do
      refute formatted =~ secret
      refute inspect(stacktrace, limit: :infinity) =~ secret
      refute inspect(exception, limit: :infinity) =~ secret
    end

    exception
  end

  test "the option forms" do
    assert %{keys: [{"default", "aegis256", @k1}], active: nil, key_for: nil} =
             Keys.normalize(cipher: "aegis256", key: @k1)

    assert %{keys: [{"r2", "aegis256", @k2}, {"r1", "aes256gcm", @k1}]} =
             Keys.normalize(keys: [{"r2", @k2}, {"r1", @k1, cipher: "aes256gcm"}])

    fun = fn _ -> nil end
    assert %{keys: [], active: "r2", key_for: ^fun} = Keys.normalize(key_for: fun, active: "r2")
    assert Keys.normalize(false) == false
    assert Keys.normalize(nil) == nil

    assert %{resolvable: true, active: "r2"} =
             Keys.native(Keys.normalize(key_for: fun, active: "r2"))
  end

  test "a bad option is refused before the NIF, and no key or secret shows" do
    for {call, fun} <- [
          rekey: fn -> S3.rekey("/tmp/x.db", s3(), encryption: ring(), force: "true") end,
          reuse: fn -> S3.rekey("/tmp/x.db", s3(), encryption: ring(), allow_reuse: nil) end,
          release: fn -> S3.release_key("/tmp/x.db", s3(), "r1", encryption: ring(), force: 1) end,
          destroy: fn -> S3.destroy(s3(), force: "yes") end,
          bad_entry: fn -> Engine.open("/tmp/x.db", encryption: [keys: [{"r1", @k1, :aes}]]) end
        ] do
      assert %ArgumentError{} = fun |> raised() |> refute_secrets(), "#{call}"
    end
  end

  test "a NIF's badarg is raised without its arguments" do
    # A path the NIF can't decode: badarg, with the ring in the top frame
    # unless it is scrubbed.
    {exception, _formatted, stacktrace} =
      raised = raised(fn -> Engine.open(123, encryption: ring(), s3: s3()) end)

    refute_secrets(raised)
    assert %ArgumentError{} = exception
    assert [{Sediment.Native, :open, 2, _} | _] = stacktrace
  end

  test "an invalid key is named by its id, never shown" do
    for bad <- [String.slice(@k1, 0, 62), String.duplicate("zz", 32)] do
      assert {:error, message} =
               Engine.open(Temp.path!(), encryption: [keys: [{"r1", bad}]])

      assert message =~ "key r1"
      refute message =~ String.slice(bad, 0, 16)
    end
  end
end
