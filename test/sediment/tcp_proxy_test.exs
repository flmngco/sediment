defmodule Sediment.TcpProxyTest do
  # The fault proxy the S3 tests put in front of the server: a server that
  # closes its side after an answer (MinIO after a failed conditional PUT)
  # must not fail the client's next request on the kept-alive connection.
  use ExUnit.Case, async: true

  alias Sediment.TcpProxy

  # Answers each connection's first request with "ok <n>", then closes.
  defp start_server do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      for n <- Stream.iterate(1, &(&1 + 1)) do
        {:ok, socket} = :gen_tcp.accept(listen)
        {:ok, _request} = :gen_tcp.recv(socket, 0)
        :ok = :gen_tcp.send(socket, "ok #{n}")
        :gen_tcp.close(socket)
      end
    end)

    port
  end

  test "a request sent right after the server closed goes out on a new connection" do
    {:ok, proxy} = TcpProxy.start_link(start_server())

    {:ok, client} =
      :gen_tcp.connect(~c"127.0.0.1", TcpProxy.port(proxy), [:binary, active: false])

    :ok = :gen_tcp.send(client, "first")
    assert {:ok, "ok 1"} = :gen_tcp.recv(client, 0, 1_000)
    # Give the server's close time to reach the proxy, then reuse the
    # connection, as an HTTP client does with a kept-alive connection.
    Process.sleep(50)
    :ok = :gen_tcp.send(client, "second")
    assert {:ok, "ok 2"} = :gen_tcp.recv(client, 0, 1_000)
  end

  test "a server that closes without answering closes the client at once" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _request} = :gen_tcp.recv(socket, 0)
      :gen_tcp.close(socket)
    end)

    {:ok, proxy} = TcpProxy.start_link(port)

    {:ok, client} =
      :gen_tcp.connect(~c"127.0.0.1", TcpProxy.port(proxy), [:binary, active: false])

    :ok = :gen_tcp.send(client, "unanswered")
    # the client learns at once that no answer comes, and can retry
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
  end

  test "truncate_puts: a PUT reaches the server with its headers only, then a reset" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      send(parent, {:received, recv_all(socket, "")})
    end)

    {:ok, proxy} =
      TcpProxy.start_link(port, truncate_puts: 1.0, truncate_puts_matching: "manifest")

    {:ok, client} =
      :gen_tcp.connect(~c"127.0.0.1", TcpProxy.port(proxy), [:binary, active: false])

    head = "PUT /b/p/manifest.json HTTP/1.1\r\nContent-Length: 4\r\n\r\n"
    :ok = :gen_tcp.send(client, head <> "body")
    assert_receive {:received, ^head}, 1_000
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, _} -> acc
    end
  end

  test "cut/1 still drops the client's connections" do
    {:ok, proxy} = TcpProxy.start_link(start_server())

    {:ok, client} =
      :gen_tcp.connect(~c"127.0.0.1", TcpProxy.port(proxy), [:binary, active: false])

    :ok = :gen_tcp.send(client, "first")
    assert {:ok, "ok 1"} = :gen_tcp.recv(client, 0, 1_000)
    :ok = TcpProxy.cut(proxy)
    assert {:error, :closed} = :gen_tcp.recv(client, 0, 1_000)
  end
end
