#!/bin/sh
# Builds the NIF for one target and packages it the way RustlerPrecompiled
# downloads it (NIF version 2.15):
#
#   scripts/build-nif.sh x86_64-unknown-linux-gnu [out-dir]
#
# The target must be the host's or installed with `rustup target add`. The
# tarball lands in out-dir (default: nif-dist).
set -eu
target=$1
out=${2:-nif-dist}
nif_version=2.15
version=$(sed -n 's/^  @version "\(.*\)"$/\1/p' mix.exs)
[ -n "$version" ] || { echo "no @version in mix.exs" >&2; exit 1; }

cargo build --release --locked -p sediment_nif --target "$target"

case "$target" in
  *windows*) built=sediment_nif.dll; name=sediment_nif-v$version-nif-$nif_version-$target.dll ;;
  *darwin*) built=libsediment_nif.dylib; name=libsediment_nif-v$version-nif-$nif_version-$target.so ;;
  *) built=libsediment_nif.so; name=libsediment_nif-v$version-nif-$nif_version-$target.so ;;
esac

mkdir -p "$out"
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
cp "target/$target/release/$built" "$staging/$name"
tar -czf "$out/$name.tar.gz" -C "$staging" "$name"
echo "$out/$name.tar.gz"
