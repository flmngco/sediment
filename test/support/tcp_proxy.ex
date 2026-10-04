defmodule Sediment.TcpProxy do
  @moduledoc false
  # A TCP proxy in front of the local S3 server whose connectivity tests can
  # cut and restore, to simulate S3 outages. With `faults`, every request and
  # every response (a change of direction) is delayed by up to
  # `max_delay_ms`, and a connection is reset with
  # probability `drop`, within its first few chunks (before or after a
  # request reached the server, so its answer may be lost). With
  # `truncate_puts: p`, a PUT (whose request line contains
  # `truncate_puts_matching`, if given) reaches the server with its headers
  # but no body, and the connection is reset: some servers store an empty
  # object then. With `lose_answer: {matching, k}`, the k-th request
  # (through any connection of this proxy) whose request line contains
  # `matching` reaches the server, but its answer is dropped and the
  # connection reset: the request landed, and a client retry sees its
  # effect. `drop_budget: n` caps the number of connections `drop`
  # resets over the proxy's life, so a client with more retries than that
  # always gets through.

  use GenServer

  def start_link(upstream_port, faults \\ []),
    do: GenServer.start_link(__MODULE__, {upstream_port, faults})

  @doc "Replaces the faults for new connections."
  def set_faults(proxy, faults), do: GenServer.call(proxy, {:faults, faults})

  def port(proxy), do: GenServer.call(proxy, :port)

  @doc "Drops every connection and refuses new ones until `resume/1`."
  def cut(proxy), do: GenServer.call(proxy, :cut)

  def resume(proxy), do: GenServer.call(proxy, :resume)

  @impl true
  def init({upstream_port, faults}) do
    faults = counters(faults)

    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    state = %{
      listen: listen,
      port: port,
      upstream: upstream_port,
      up: true,
      pipes: [],
      faults: faults
    }

    {:ok, start_acceptor(state)}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def handle_call(:cut, _from, state) do
    Enum.each(state.pipes, &Process.exit(&1, :kill))
    {:reply, :ok, %{state | up: false, pipes: []}}
  end

  def handle_call(:resume, _from, state), do: {:reply, :ok, %{state | up: true}}

  def handle_call({:faults, faults}, _from, state),
    do: {:reply, :ok, %{state | faults: counters(faults)}}

  @impl true
  def handle_info({:accepted, client}, %{up: false} = state) do
    :gen_tcp.close(client)
    {:noreply, state}
  end

  def handle_info({:accepted, client}, state) do
    faults = state.faults
    pipe = spawn(fn -> pipe(client, state.upstream, faults) end)
    :ok = :gen_tcp.controlling_process(client, pipe)
    send(pipe, :go)
    {:noreply, %{state | pipes: [pipe | state.pipes]}}
  end

  defp start_acceptor(state) do
    owner = self()

    spawn_link(fn -> accept_loop(state.listen, owner) end)
    state
  end

  defp accept_loop(listen, owner) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        :ok = :gen_tcp.controlling_process(client, owner)
        send(owner, {:accepted, client})
        accept_loop(listen, owner)

      {:error, _} ->
        :ok
    end
  end

  defp pipe(client, upstream_port, faults) do
    receive do: (:go -> :ok)
    {:ok, upstream} = :gen_tcp.connect(~c"127.0.0.1", upstream_port, [:binary, active: true])
    :ok = :inet.setopts(client, active: true)
    drop = Keyword.get(faults, :drop, 0)

    reset_after =
      if :rand.uniform() < drop and take_drop_budget(faults[:drop_budget]),
        do: :rand.uniform(8),
        else: :never

    faults = Keyword.merge(faults, reset_after: reset_after, upstream_port: upstream_port)
    relay(client, upstream, faults)
  end

  # When the server closes an idle connection (it answered everything sent
  # so far), the client connection stays open and its next request goes out
  # on a new server connection, as through a load balancer. Closing the
  # client instead raced with a client about to send its next request on the
  # kept-alive connection (MinIO closes after answering a failed conditional
  # PUT): that request then failed, where a direct connection sees the close
  # in time. A close while a request awaits its answer still closes
  # the client, which then retries.
  defp take_drop_budget(nil), do: true
  defp take_drop_budget(budget), do: :atomics.sub_get(budget, 1, 1) >= 0

  defp relay(client, upstream, faults) do
    receive do
      {:tcp, ^client, data} when upstream == :closed ->
        {:ok, upstream} =
          :gen_tcp.connect(~c"127.0.0.1", faults[:upstream_port], [:binary, active: true])

        forward(upstream, :up, data, client, upstream, faults)

      {:tcp, ^client, data} ->
        forward(upstream, :up, data, client, upstream, faults)

      {:tcp, ^upstream, data} ->
        forward(client, :down, data, client, upstream, faults)

      {:tcp_closed, ^upstream} ->
        if faults[:awaiting] do
          :gen_tcp.close(client)
        else
          relay(client, :closed, faults)
        end

      {:tcp_closed, ^client} ->
        if upstream != :closed, do: :gen_tcp.close(upstream)
    end
  end

  defp forward(upstream, :up, "PUT " <> _ = data, client, upstream, faults) do
    faults = mark_lost_answer(data, faults)

    with p when is_number(p) <- Keyword.get(faults, :truncate_puts),
         true <- :rand.uniform() < p,
         [line | _] = String.split(data, "\r\n", parts: 2),
         true <- String.contains?(line, Keyword.get(faults, :truncate_puts_matching, "")),
         [head, _body] <- :binary.split(data, "\r\n\r\n") do
      :gen_tcp.send(upstream, [head, "\r\n\r\n"])
      Process.sleep(50)
      :gen_tcp.close(client)
      :gen_tcp.close(upstream)
    else
      _ -> forward_data(upstream, :up, data, client, upstream, faults)
    end
  end

  defp forward(upstream, :up, data, client, upstream, faults),
    do: forward_data(upstream, :up, data, client, upstream, mark_lost_answer(data, faults))

  defp forward(to, direction, data, client, upstream, faults) do
    if direction == :down and faults[:drop_answer] do
      :gen_tcp.close(client)
      :gen_tcp.close(upstream)
    else
      forward_data(to, direction, data, client, upstream, faults)
    end
  end

  # The shared counters of `drop_budget` and `lose_answer` (also for set_faults/2).
  defp counters(faults) do
    faults =
      case faults[:drop_budget] do
        nil ->
          faults

        n ->
          Keyword.put(faults, :drop_budget, :atomics.new(1, []) |> tap(&:atomics.put(&1, 1, n)))
      end

    case faults[:lose_answer] do
      {matching, k} -> Keyword.put(faults, :lose_answer, {matching, k, :atomics.new(1, [])})
      _ -> faults
    end
  end

  # `lose_answer`: marks the connection when this chunk starts the k-th
  # matching request, so its answer is dropped.
  defp mark_lost_answer(data, faults) do
    with {matching, k, seen} <- faults[:lose_answer],
         [line | _] <- String.split(data, "\r\n", parts: 2),
         true <- String.contains?(line, " HTTP/1.1") and String.contains?(line, matching),
         ^k <- :atomics.add_get(seen, 1, 1) do
      Keyword.put(faults, :drop_answer, true)
    else
      _ -> faults
    end
  end

  defp forward_data(to, direction, data, client, upstream, faults) do
    max_delay = Keyword.get(faults, :max_delay_ms, 0)

    if max_delay > 0 and Keyword.get(faults, :direction) != direction,
      do: Process.sleep(:rand.uniform(max_delay + 1) - 1)

    # client bytes sent and no answer yet
    faults = Keyword.merge(faults, direction: direction, awaiting: direction == :up)

    case Keyword.get(faults, :reset_after, :never) do
      1 ->
        :gen_tcp.close(client)
        :gen_tcp.close(upstream)

      n ->
        :gen_tcp.send(to, data)
        faults = if n == :never, do: faults, else: Keyword.put(faults, :reset_after, n - 1)
        relay(client, upstream, faults)
    end
  end
end
