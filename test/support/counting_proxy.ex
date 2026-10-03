defmodule Sediment.CountingProxy do
  @moduledoc false
  # An HTTP proxy in front of the S3 server that counts every request by S3
  # operation, independently of the driver (it checks the driver's
  # request meter). It parses only the request side (request line, headers,
  # and a Content-Length or chunked body, to find the next request on a
  # kept-alive connection) and forwards every byte unchanged.

  use GenServer

  def start_link(upstream_port), do: GenServer.start_link(__MODULE__, upstream_port)

  def port(proxy), do: GenServer.call(proxy, :port)

  @doc "Requests counted so far: %{operation => count}."
  def counts(proxy), do: GenServer.call(proxy, :counts)

  def reset(proxy), do: GenServer.call(proxy, :reset)

  @doc "Tigris class of an operation: :a, :b or :free."
  def class(op) when op in ~w(GetObject HeadObject HeadBucket), do: :b
  def class(op) when op in ~w(DeleteObject DeleteObjects AbortMultipartUpload), do: :free
  def class(_op), do: :a

  @impl true
  def init(upstream_port) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    owner = self()
    spawn_link(fn -> accept_loop(listen, upstream_port, owner) end)
    {:ok, %{listen: listen, port: port, counts: %{}}}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:counts, _from, state), do: {:reply, state.counts, state}
  def handle_call(:reset, _from, state), do: {:reply, :ok, %{state | counts: %{}}}

  @impl true
  def handle_info({:request, op}, state),
    do: {:noreply, %{state | counts: Map.update(state.counts, op, 1, &(&1 + 1))}}

  defp accept_loop(listen, upstream_port, owner) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        pid = spawn(fn -> connection(client, upstream_port, owner) end)
        :ok = :gen_tcp.controlling_process(client, pid)
        send(pid, :go)
        accept_loop(listen, upstream_port, owner)

      {:error, _} ->
        :ok
    end
  end

  defp connection(client, upstream_port, owner) do
    receive do: (:go -> :ok)
    {:ok, upstream} = :gen_tcp.connect(~c"127.0.0.1", upstream_port, [:binary, active: false])
    down = spawn_link(fn -> relay(upstream, client) end)
    :ok = :gen_tcp.controlling_process(upstream, down)
    requests(client, upstream, owner, <<>>)
  end

  # Responses go back unchanged.
  defp relay(from, to) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        :gen_tcp.send(to, data)
        relay(from, to)

      {:error, _} ->
        :gen_tcp.close(to)
    end
  end

  # Reads one request's head, counts it, forwards head and body.
  defp requests(client, upstream, owner, buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [head, rest] ->
        {op, body} = classify(head)
        send(owner, {:request, op})
        _ = :gen_tcp.send(upstream, [head, "\r\n\r\n"])
        rest = forward_body(client, upstream, body, rest)
        requests(client, upstream, owner, rest)

      [_incomplete] ->
        case :gen_tcp.recv(client, 0) do
          {:ok, data} ->
            requests(client, upstream, owner, buffer <> data)

          {:error, _} ->
            :gen_tcp.close(upstream)
        end
    end
  end

  # Forwards a body of `{:length, n}` or `:chunked` bytes; returns what
  # follows it.
  defp forward_body(client, upstream, {:length, n}, rest) do
    if byte_size(rest) >= n do
      <<body::binary-size(^n), after_body::binary>> = rest
      _ = :gen_tcp.send(upstream, body)
      after_body
    else
      _ = :gen_tcp.send(upstream, rest)
      data = recv_or_stop(client, upstream)
      forward_body(client, upstream, {:length, n - byte_size(rest)}, data)
    end
  end

  defp forward_body(client, upstream, :chunked, rest) do
    case :binary.split(rest, "\r\n") do
      [size_line, after_size] ->
        [hex | _] = String.split(size_line, ";")
        size = String.to_integer(String.trim(hex), 16)
        _ = :gen_tcp.send(upstream, [size_line, "\r\n"])

        if size == 0 do
          # the terminating chunk, then (no trailers) a blank line
          forward_body(client, upstream, {:length, 2}, after_size)
        else
          after_chunk = forward_body(client, upstream, {:length, size + 2}, after_size)
          forward_body(client, upstream, :chunked, after_chunk)
        end

      [_incomplete] ->
        data = recv_or_stop(client, upstream)
        forward_body(client, upstream, :chunked, rest <> data)
    end
  end

  defp forward_body(_client, _upstream, :none, rest), do: rest

  # A client that goes away mid-request (a reset, a timeout) ends this
  # connection quietly.
  defp recv_or_stop(client, upstream) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        data

      {:error, _} ->
        :gen_tcp.close(upstream)
        exit(:normal)
    end
  end

  @doc false
  # {operation, body} for a request head (path-style addressing).
  def classify(head) do
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, target | _] = String.split(request_line, " ")
    headers = Map.new(header_lines, &header/1)
    {path, query} = split_target(target)
    bucket_level? = path |> String.trim("/") |> String.split("/", parts: 2) |> length() == 1
    {operation(method, bucket_level?, query, headers), body(headers)}
  end

  defp header(line) do
    [name, value] = String.split(line, ":", parts: 2)
    {String.downcase(name), String.trim(value)}
  end

  defp split_target(target) do
    case String.split(target, "?", parts: 2) do
      [path, query] -> {path, URI.decode_query(query)}
      [path] -> {path, %{}}
    end
  end

  defp body(%{"transfer-encoding" => te}) when te != "", do: :chunked
  defp body(%{"content-length" => n}), do: {:length, String.to_integer(n)}
  defp body(_headers), do: :none

  defp operation("GET", _b, %{"uploads" => _}, _h), do: "ListMultipartUploads"
  defp operation("GET", _b, %{"uploadId" => _}, _h), do: "ListParts"
  defp operation("GET", true, _q, _h), do: "ListObjectsV2"
  defp operation("GET", false, _q, _h), do: "GetObject"
  defp operation("HEAD", true, _q, _h), do: "HeadBucket"
  defp operation("HEAD", false, _q, _h), do: "HeadObject"
  defp operation("PUT", true, _q, _h), do: "CreateBucket"
  defp operation("PUT", false, %{"partNumber" => _}, _h), do: "UploadPart"
  defp operation("PUT", false, _q, %{"x-amz-copy-source" => _}), do: "CopyObject"
  defp operation("PUT", false, _q, _h), do: "PutObject"
  defp operation("POST", _b, %{"uploads" => _}, _h), do: "CreateMultipartUpload"
  defp operation("POST", _b, %{"uploadId" => _}, _h), do: "CompleteMultipartUpload"
  defp operation("POST", true, %{"delete" => _}, _h), do: "DeleteObjects"
  defp operation("DELETE", false, %{"uploadId" => _}, _h), do: "AbortMultipartUpload"
  defp operation("DELETE", false, _q, _h), do: "DeleteObject"
  defp operation(method, _b, _q, _h), do: "Other:" <> method
end
