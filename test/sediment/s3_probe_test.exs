defmodule Sediment.S3ProbeTest do
  # The provider probe (conditional writes enforced?) must not mistake a
  # lost answer for a missing feature: a probe PUT that landed but whose
  # answer was lost is retried by the S3 client and then conflicts with
  # itself. Such an open used to fail with "s3 conflict" or claim
  # the provider "does not enforce If-Match".
  use ExUnit.Case, async: false

  alias Sediment.{Engine, TcpProxy}

  @moduletag :s3
  @moduletag skip: Sediment.S3Bucket.remote?() && "needs a local S3 server (TcpProxy)"

  setup_all do
    :ok = Sediment.S3Bucket.ensure()
  end

  # The probe's requests on its key: 1 create, 2 create again (must
  # conflict), 3 update with a stale ETag (must conflict), 4 update with the
  # current ETag.
  for {step, what} <- [{1, "the first create"}, {4, "the update with the current ETag"}] do
    test "a lost answer to #{what} doesn't fail the probe" do
      dir = Path.join(System.tmp_dir!(), "s3probe-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, proxy} =
        TcpProxy.start_link(URI.parse(Sediment.S3Bucket.endpoint()).port,
          lose_answer: {"/probe/", unquote(step)}
        )

      s3 = [
        bucket: Sediment.S3Bucket.name(),
        prefix: "elixir/probe/#{System.unique_integer([:positive])}-#{System.os_time()}",
        # its own endpoint, so this process hasn't verified it yet
        endpoint: "http://127.0.0.1:#{TcpProxy.port(proxy)}",
        region: "us-east-1",
        access_key_id: System.get_env("S3_TEST_ACCESS_KEY_ID", "any"),
        encryption: false,
        secret_access_key: System.get_env("S3_TEST_SECRET_ACCESS_KEY", "any"),
        owner: "probe"
      ]

      assert {:ok, db} = Engine.open(Path.join(dir, "w.db"), s3: s3)
      :ok = Engine.close(db)
    end
  end
end
