#!/bin/sh
# Type-checks the NIF crate for another platform from Linux, with zig as the C
# cross compiler for the C dependencies (zstd-sys, aws-lc-sys):
#
#   scripts/cross-check.sh x86_64-pc-windows-gnu
#   scripts/cross-check.sh aarch64-apple-darwin
#
# Needs `rustup target add <target>` and zig. `cargo check` doesn't link, so
# no platform SDK is needed.
set -eu
target=${1:-x86_64-pc-windows-gnu}
case "$target" in
  x86_64-pc-windows-gnu) zig_target=x86_64-windows-gnu ;;
  aarch64-apple-darwin) zig_target=aarch64-macos ;;
  x86_64-apple-darwin) zig_target=x86_64-macos ;;
  *) echo "unsupported target $target" >&2; exit 1 ;;
esac
shift $(( $# > 0 ? 1 : 0 ))
tools=$(mktemp -d)
trap 'rm -rf "$tools"' EXIT
for pair in cc:cc cxx:c++; do
  name=${pair%%:*}
  lang=${pair##*:}
  # The cc crate passes a Rust-style --target that zig doesn't parse.
  cat > "$tools/$name" <<WRAPPER
#!/bin/sh
for arg in "\$@"; do
  shift
  case "\$arg" in --target=*) ;; *) set -- "\$@" "\$arg" ;; esac
done
exec zig $lang -target $zig_target "\$@"
WRAPPER
  chmod +x "$tools/$name"
done
printf '#!/bin/sh\nexec zig ar "$@"\n' > "$tools/ar"
chmod +x "$tools/ar"

var=$(echo "$target" | tr '-' '_')
env "CC_$var=$tools/cc" "CXX_$var=$tools/cxx" "AR_$var=$tools/ar" \
  cargo check --target "$target" -p sediment_nif --all-targets "$@"
