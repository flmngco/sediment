# Checks the checksum file that `mix rustler_precompiled.download --all` wrote
# from the GitHub release against the tarballs the release workflow built:
# the same files (so every target is covered) with the same SHA-256.
#
#   elixir scripts/check-nif-checksums.exs checksum-Elixir.Sediment.Native.exs nif-dist
[checksum_file, dist] = System.argv()
{checksums, _} = Code.eval_file(checksum_file)

built =
  for path <- Path.wildcard(Path.join(dist, "*.tar.gz")), into: %{} do
    hash = :crypto.hash(:sha256, File.read!(path)) |> Base.encode16(case: :lower)
    {Path.basename(path), "sha256:" <> hash}
  end

missing = Map.keys(built) -- Map.keys(checksums)
unexpected = Map.keys(checksums) -- Map.keys(built)
mismatched = for {file, sum} <- built, Map.get(checksums, file, sum) != sum, do: file

if built == %{} or missing != [] or unexpected != [] or mismatched != [] do
  IO.puts(:stderr, """
  checksum file and built NIFs disagree
    built: #{map_size(built)}, in the checksum file: #{map_size(checksums)}
    not in the checksum file: #{inspect(missing)}
    not built: #{inspect(unexpected)}
    different contents: #{inspect(mismatched)}
  """)

  System.halt(1)
end

IO.puts("#{map_size(built)} NIFs match #{checksum_file}")
