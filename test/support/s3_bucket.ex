defmodule Sediment.S3Bucket do
  @moduledoc false
  # The S3 test bucket (S3_TEST_BUCKET on S3_TEST_ENDPOINT). Every S3 test
  # module creates it in setup_all: a fresh server (CI) doesn't have it, and
  # modules run in any order.

  def name, do: System.get_env("S3_TEST_BUCKET", "sediment-tests")

  def endpoint, do: System.get_env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")

  def region, do: System.get_env("S3_TEST_REGION", "us-east-1")

  @doc """
  A remote provider over HTTPS: the bucket exists, and TcpProxy can't sit in
  front. S3_TEST_REMOTE=1 makes a local server behave the same.
  """
  def remote?,
    do: String.starts_with?(endpoint(), "https://") or System.get_env("S3_TEST_REMOTE") == "1"

  # SeaweedFS accepts unsigned requests, so the bucket is created on the fly;
  # servers that require signatures (403) must have it already.
  def ensure do
    if remote?(), do: :ok, else: create()
  end

  defp create do
    %URI{host: host, port: port} = URI.parse(endpoint())
    {:ok, socket} = :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "PUT /#{name()} HTTP/1.1\r\nHost: #{host}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
      )

    {:ok, "HTTP/1.1 " <> <<status::binary-size(3), _::binary>>} = :gen_tcp.recv(socket, 0)
    :gen_tcp.close(socket)
    if status in ["200", "403", "409"], do: :ok, else: {:error, status}
  end
end
