defmodule Sediment.TelemetryTest do
  use ExUnit.Case, async: true

  alias Sediment.Connection

  defp attach(events) do
    id = make_ref()
    parent = self()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _ -> send(parent, {event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  test "queries emit start and stop with the result" do
    attach([
      [:sediment, :query, :start],
      [:sediment, :query, :stop],
      [:sediment, :prepare, :stop]
    ])

    path = Temp.path!()
    {:ok, pool} = Sediment.start_link(database: path, pool_size: 1)

    Sediment.query!(pool, "select ? + 1", [41])

    assert_receive {[:sediment, :query, :start], %{system_time: _},
                    %{query: "select ? + 1", params: [41], database: ^path}}

    assert_receive {[:sediment, :query, :stop], %{duration: duration},
                    %{database: ^path, result: {:ok, %Sediment.Result{rows: [[42]]}}}}

    assert duration >= 0

    {:error, _} = Sediment.query(pool, "select * from nope")

    assert_receive {[:sediment, :prepare, :stop], _,
                    %{
                      database: ^path,
                      result: {:error, %Sediment.Error{message: "no such table: nope"}}
                    }}
  end

  test "disconnects report why" do
    attach([[:sediment, :connection, :disconnect]])
    path = Temp.path!()
    {:ok, state} = Connection.connect(database: path)
    :ok = Sediment.Native.debug_panic_next_step(state.db)

    {:disconnect, error, state} =
      Connection.handle_execute(%Sediment.Query{statement: "select 1"}, [], [], state)

    Connection.disconnect(error, state)

    assert_receive {[:sediment, :connection, :disconnect], %{system_time: _},
                    %{database: ^path, reason: :internal, error: ^error}}
  end

  test "a pool emits a disconnect when a client times out" do
    attach([[:sediment, :connection, :disconnect]])
    path = Temp.path!()
    {:ok, pool} = Sediment.start_link(database: path, pool_size: 1)

    ExUnit.CaptureLog.capture_log(fn ->
      Sediment.query(
        pool,
        "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c",
        [],
        timeout: 50
      )

      assert_receive {[:sediment, :connection, :disconnect], _,
                      %{database: ^path, error: %DBConnection.ConnectionError{}, reason: :other}},
                     5_000
    end)
  end

  test "DBConnection.TelemetryListener works with sediment pools" do
    attach([[:db_connection, :connected]])
    {:ok, listener} = DBConnection.TelemetryListener.start_link()

    {:ok, _pool} =
      Sediment.start_link(
        database: Temp.path!(),
        pool_size: 1,
        connection_listeners: {[listener], :my_tag}
      )

    assert_receive {[:db_connection, :connected], %{count: 1}, %{tag: :my_tag}}, 5_000
  end
end
