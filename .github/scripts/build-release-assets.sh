#!/usr/bin/env bash
# Build the six platform archives and checksums.txt used by the v3 release workflow.
#
# Layout matches the assets shipped on SisyphusSQ/go-oak-chunk v3.5.0 and v3.5.1:
#   goc_<version>_<os>_<arch>.tar.gz  contains ./goc and ./LICENSE
#   goc_<version>_windows_<arch>.zip  contains goc.exe and LICENSE
#   checksums.txt                     is the output of sha256sum on those six files
#
# ldflags follow the variables actually injected into those release binaries
# (Makefile BUILD_FLAGS, with the two differences those binaries show):
#   - AppVersion is not injected; vars/vars.go keeps the source value
#     (the Makefile line that would set it is commented out)
#   - BuildTime is UTC RFC3339 to the second, as in the shipped binaries
#     (2026-10-23T02:01:30Z), not Makefile's local "YYYY-MM-DD HH:MM:SS"
#   - GitRemote is the canonical upstream URL embedded in both releases,
#     not `git config remote.origin.url` (that can be the fork, or a token)
# -trimpath, CGO_ENABLED=0 and Go 1.26.2 match the v3.5.1 build info.
# -buildvcs=false matches those binaries, which carry no vcs.* build settings.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <version> <out-dir>" >&2
  exit 2
fi

VERSION=$1
OUT=$2

if [[ ! "$VERSION" =~ ^[A-Za-z0-9._+-]+$ ]]; then
  echo "invalid version: ${VERSION}" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

export GOTOOLCHAIN=local
export CGO_ENABLED=0
export LC_ALL=C

GO_VERSION_LINE=$(go version)
case "$GO_VERSION_LINE" in
  "go version go1.26.2 "*) ;;
  *)
    echo "需要 Go 1.26.2，当前是: ${GO_VERSION_LINE}" >&2
    exit 1
    ;;
esac

SHA=$(git rev-parse HEAD)
BUILDTIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
PKG=github.com/SisyphusSQ/go-oak-chunk/v3/vars
GIT_REMOTE=https://github.com/SisyphusSQ/go-oak-chunk.git

LDFLAGS="-X '${PKG}.AppName=goc'"
LDFLAGS+=" -X '${PKG}.GoVersion=${GO_VERSION_LINE}'"
LDFLAGS+=" -X '${PKG}.BuildTime=${BUILDTIME}'"
LDFLAGS+=" -X '${PKG}.GitCommit=${SHA}'"
LDFLAGS+=" -X '${PKG}.GitRemote=${GIT_REMOTE}'"

mkdir -p "$OUT"
shopt -s nullglob
rm -f "$OUT"/goc_"${VERSION}"_*.tar.gz "$OUT"/goc_"${VERSION}"_*.zip "$OUT"/checksums.txt
shopt -u nullglob

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

build_zip() {
  local archive=$1 binname=$2
  python3 - "$archive" "$binname" "$STAGE/$binname" "$STAGE/LICENSE" <<'PY'
import os
import sys
import time
import zipfile

archive, exe_name, exe_path, license_path = sys.argv[1:]
specs = (
    (exe_name, exe_path, 0o100755),
    ("LICENSE", license_path, 0o100644),
)
with zipfile.ZipFile(archive, "w") as zf:
    for arcname, path, mode in specs:
        info = zipfile.ZipInfo(arcname, time.localtime(os.stat(path).st_mtime)[:6])
        info.compress_type = zipfile.ZIP_DEFLATED
        info.create_system = 3
        info.external_attr = mode << 16
        with open(path, "rb") as fh:
            zf.writestr(info, fh.read())
PY
}

while read -r goos goarch kind; do
  binname=goc
  if [[ "$goos" == "windows" ]]; then
    binname=goc.exe
  fi
  rm -rf "$STAGE"
  mkdir -p "$STAGE"
  GOOS="$goos" GOARCH="$goarch" \
    go build -trimpath -buildvcs=false -ldflags "$LDFLAGS" \
    -o "$STAGE/$binname" ./cmd/go-oak-chunk
  chmod 0755 "$STAGE/$binname"
  install -m 0644 "$ROOT/LICENSE" "$STAGE/LICENSE"

  archive="$OUT/goc_${VERSION}_${goos}_${goarch}.${kind}"
  if [[ "$kind" == "tar.gz" ]]; then
    tar --format=gnu -C "$STAGE" -czf "$archive" "$binname" LICENSE
  else
    build_zip "$archive" "$binname"
  fi
done <<'EOF'
linux amd64 tar.gz
linux arm64 tar.gz
darwin amd64 tar.gz
darwin arm64 tar.gz
windows amd64 zip
windows arm64 zip
EOF

(
  cd "$OUT"
  sha256sum \
    "goc_${VERSION}_darwin_amd64.tar.gz" \
    "goc_${VERSION}_darwin_arm64.tar.gz" \
    "goc_${VERSION}_linux_amd64.tar.gz" \
    "goc_${VERSION}_linux_arm64.tar.gz" \
    "goc_${VERSION}_windows_amd64.zip" \
    "goc_${VERSION}_windows_arm64.zip" \
    > checksums.txt
)

python3 - "$OUT" "$VERSION" "$ROOT/LICENSE" <<'PY'
import hashlib
import pathlib
import sys
import tarfile
import zipfile

out = pathlib.Path(sys.argv[1])
version = sys.argv[2]
license_bytes = pathlib.Path(sys.argv[3]).read_bytes()
expected = []
for goos in ("darwin", "linux", "windows"):
    for arch in ("amd64", "arm64"):
        ext = "zip" if goos == "windows" else "tar.gz"
        expected.append(f"goc_{version}_{goos}_{arch}.{ext}")

files = sorted(p.name for p in out.iterdir())
if files != sorted(expected + ["checksums.txt"]):
    raise SystemExit(f"unexpected outputs: {files}")

lines = (out / "checksums.txt").read_text().splitlines()
if [line.split("  ", 1)[1] for line in lines] != expected:
    raise SystemExit(f"checksum order: {lines}")
for line in lines:
    digest, name = line.split("  ", 1)
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise SystemExit(f"bad checksum line: {line!r}")
    got = hashlib.sha256((out / name).read_bytes()).hexdigest()
    if got != digest:
        raise SystemExit(f"checksum mismatch for {name}")

for name in expected:
    path = out / name
    if name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as tf:
            members = [m for m in tf.getmembers() if m.name]
            names = [m.name for m in members]
            if names != ["goc", "LICENSE"]:
                raise SystemExit(f"{name} members {names}")
            modes = {m.name: m.mode & 0o777 for m in members}
            if modes != {"goc": 0o755, "LICENSE": 0o644}:
                raise SystemExit(f"{name} modes {modes}")
            if tf.extractfile("LICENSE").read() != license_bytes:
                raise SystemExit(f"{name} LICENSE bytes differ")
    else:
        with zipfile.ZipFile(path) as zf:
            infos = zf.infolist()
            names = [i.filename for i in infos]
            if names != ["goc.exe", "LICENSE"]:
                raise SystemExit(f"{name} members {names}")
            for info, mode in zip(infos, (0o100755, 0o100644)):
                if info.create_system != 3 or info.external_attr >> 16 != mode:
                    raise SystemExit(
                        f"{name} {info.filename} system={info.create_system} "
                        f"mode={info.external_attr >> 16:#o}"
                    )
                if "/" in info.filename:
                    raise SystemExit(f"{name} nested {info.filename}")
            if zf.read("LICENSE") != license_bytes:
                raise SystemExit(f"{name} LICENSE bytes differ")

print(f"release assets ok: {version}")
PY
