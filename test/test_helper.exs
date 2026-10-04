# Every temporary file of a test run (Temp.path!/0, System.tmp_dir!/0 and the
# NIF's serialize scratch files) goes to a per-run directory removed at the end.
# Runs killed before after_suite leave theirs behind: remove those whose OS
# process is gone (Linux, where /proc tells).
if File.dir?("/proc") do
  for dir <- Path.wildcard(Path.join(System.tmp_dir!(), "sediment_test_*_*")),
      pid = dir |> String.split("_") |> List.last(),
      not File.exists?("/proc/#{pid}"),
      do: File.rm_rf(dir)
end

run_tmp_dir =
  Path.join(System.tmp_dir!(), "sediment_test_#{System.os_time()}_#{System.pid()}")

File.mkdir_p!(run_tmp_dir)
System.put_env("TMPDIR", run_tmp_dir)
ExUnit.after_suite(fn _ -> File.rm_rf(run_tmp_dir) end)

ExUnit.start(capture_log: true, timeout: 120_000, exclude: [:slow_test, :s3, :soak, :torture])
