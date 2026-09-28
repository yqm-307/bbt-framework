#!/usr/bin/env bash
# Reproduce infra's locked protobuf prefix without installing host packages.
set -euo pipefail

prefix="${1:?usage: prepare_protobuf.sh <empty-prefix-path>}"
if [ -e "$prefix" ]; then
    echo "[protobuf] FATAL: prefix already exists: $prefix" >&2
    exit 2
fi
if [ "$(dpkg --print-architecture)" != amd64 ]; then
    echo '[protobuf] FATAL: locked package set is amd64 only' >&2
    exit 2
fi
mkdir -p "$(dirname "$prefix")"
staging="$(mktemp -d "${prefix}.tmp.XXXXXX")"
trap 'rm -rf "$staging"' EXIT
mkdir -p "$staging/debs"

# Exact Ubuntu package SHA256 values from infra's protobuf-3.21.12.provenance.md.
while read -r component package sha; do
    file="$staging/debs/$package"
    curl --fail --location --retry 2 --silent --show-error \
        "https://archive.ubuntu.com/ubuntu/pool/$component/p/protobuf/$package" -o "$file"
    printf '%s  %s\n' "$sha" "$file" | sha256sum --check --status || {
        echo "[protobuf] FATAL: package checksum mismatch: $package" >&2
        exit 3
    }
    dpkg-deb --extract "$file" "$staging"
done <<'PACKAGES'
main libprotobuf32t64_3.21.12-15ubuntu1_amd64.deb adab0eb28161aea2101e33d8b4c8a336ad62c34c70e01bd3d7c1026279ca5757
main libprotobuf-dev_3.21.12-15ubuntu1_amd64.deb 259c9f383dee92f18da256ba31f6709a2eca835f50f7b5befdcbc1ea02fb05b1
main libprotoc32t64_3.21.12-15ubuntu1_amd64.deb 878419d40eaae00845382d79896d20b446aa12b0550a9e12db381895b05d86c0
universe protobuf-compiler_3.21.12-15ubuntu1_amd64.deb 2501bf552ae297d415eb31d40da8768c53d9810fc082fe5e7c93271e24d37f7a
PACKAGES

libdir="$staging/usr/lib/x86_64-linux-gnu"
version="$(LD_LIBRARY_PATH="$libdir" "$staging/usr/bin/protoc" --version)"
[ "$version" = 'libprotoc 3.21.12' ] || {
    echo "[protobuf] FATAL: unexpected protoc version: $version" >&2
    exit 4
}
printf '%s  %s\n' \
    '3e3e4ae1f24b2dd52e8104e7af229918a2d504274a08f7d660aaff59bae7a2de' \
    "$libdir/libprotobuf.a" | sha256sum --check --status
[ -f "$staging/usr/include/google/protobuf/stubs/common.h" ]
[ -f "$libdir/pkgconfig/protobuf.pc" ]
rm -rf "$staging/debs"
mv "$staging" "$prefix"
trap - EXIT
printf '[protobuf] locked %s prefix ready: %s\n' "$version" "$prefix"
