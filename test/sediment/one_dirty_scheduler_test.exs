defmodule Sediment.OneDirtySchedulerTest do
  # Cancel and disconnect interleavings that need a native call to stay
  # queued for a dirty scheduler: run in a VM with only one (+SDio 1), see
  # test/support/one_dirty_scheduler.exs for what each exit status means.
  use ExUnit.Case, async: true

  test "cancels and disconnects reach queued and running calls" do
    paths =
      Enum.flat_map(
        Path.wildcard(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
        &["-pa", &1]
      )

    script = Path.expand("../support/one_dirty_scheduler.exs", __DIR__)

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["--erl", "+SDio 1 +S 2:2"] ++ paths ++ [script]
      ])

    {:os_pid, pid} = Port.info(port, :os_pid)
    {status, output} = collect(port, "")
    if status == :timeout, do: System.cmd("kill", ["-9", "#{pid}"])
    assert {status, output} == {0, "all passed\n"}
  end

  defp collect(port, output) do
    receive do
      {^port, {:data, data}} -> collect(port, output <> data)
      {^port, {:exit_status, status}} -> {status, output}
    after
      60_000 -> {:timeout, output}
    end
  end
end
