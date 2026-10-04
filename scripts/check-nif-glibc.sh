#!/bin/sh
# Fails if a packaged gnu NIF needs a newer glibc than the documented floor:
#
#   scripts/check-nif-glibc.sh 2.28 nif-dist/*.tar.gz
set -eu
floor=$1
shift
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
status=0
for tarball in "$@"; do
  rm -rf "${work:?}"/*
  tar -xzf "$tarball" -C "$work"
  needed=$(objdump -T "$work"/*.so | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -Vu | tail -1)
  if [ -z "$needed" ]; then
    echo "$tarball: no versioned glibc symbols found" >&2; status=1
  elif [ "$(printf '%s\n%s\n' "$floor" "$needed" | sort -V | tail -1)" != "$floor" ]; then
    echo "$tarball needs glibc $needed, newer than $floor" >&2; status=1
  else
    echo "$tarball: glibc $needed (floor $floor)"
  fi
done
exit $status
