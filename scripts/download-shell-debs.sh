#!/bin/sh
# Download pre-built Debian .deb packages for offline Docker builds.
# Run this on a machine with public internet access.
#
# Usage:
#   ./scripts/download-shell-debs.sh                # shells only (zsh, tcsh)
#   ./scripts/download-shell-debs.sh --python       # also download python3.13
#   ./scripts/download-shell-debs.sh arm64          # force arch
#   ./scripts/download-shell-debs.sh amd64 --python
#
# Output:
#   .docker-debs/         — shell debs (used by Dockerfile)
#   .docker-debs-python/  — python3.13 debs (used by Dockerfile.python)

set -e

ARCH=""
PYTHON=0

for arg in "$@"; do
  case "$arg" in
    --python) PYTHON=1 ;;
    arm64|aarch64) ARCH="arm64" ;;
    amd64|x86_64)  ARCH="amd64" ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

if [ -z "$ARCH" ]; then
  HOST=$(uname -m)
  case "$HOST" in
    arm64|aarch64) ARCH="arm64" ;;
    x86_64|amd64)  ARCH="amd64" ;;
    *) echo "Unsupported arch: $HOST" >&2; exit 1 ;;
  esac
fi

BASE="http://snapshot.debian.org/archive/debian/20260505T000000Z"
ROOT="$(dirname "$0")/.."

echo "==> Fetching Debian trixie package index for $ARCH..."
PKG_DATA=$(curl -sL "$BASE/dists/trixie/main/binary-$ARCH/Packages.gz" | gunzip)

download() {
  pkg="$1"; dest="$2"
  path=$(echo "$PKG_DATA" | awk "/^Package: $pkg$/{found=1} found && /^Filename:/{print \$2; exit}")
  if [ -z "$path" ]; then echo "  WARNING: $pkg not found in index"; return; fi
  fname=$(basename "$path")
  printf "  %-55s" "$fname"
  curl -sL "$BASE/$path" -o "$dest/$fname" && du -sh "$dest/$fname" | cut -f1 || echo "FAILED"
}

# ── Shells ──────────────────────────────────────────────────────────────────────
SHELL_DEST="$ROOT/.docker-debs"
mkdir -p "$SHELL_DEST"
echo "==> Downloading shell packages → $SHELL_DEST"
for pkg in zsh zsh-common tcsh libncursesw6 libtinfo6; do
  download "$pkg" "$SHELL_DEST"
done

# ── Python ──────────────────────────────────────────────────────────────────────
if [ "$PYTHON" = "1" ]; then
  PY_DEST="$ROOT/.docker-debs-python"
  mkdir -p "$PY_DEST"
  echo "==> Downloading Python 3.13 packages → $PY_DEST"
  for pkg in \
    libbz2-1.0 libdb5.3t64 libexpat1 libffi8 \
    libpython3-stdlib libpython3.13-minimal libpython3.13-stdlib \
    libreadline8t64 libsqlite3-0 libssl3t64 libuuid1 \
    media-types openssl-provider-legacy \
    python3 python3-minimal python3.13 python3.13-minimal \
    readline-common; do
    download "$pkg" "$PY_DEST"
  done
fi

echo ""
echo "==> Done."
echo "    .docker-debs/:        $(ls "$SHELL_DEST"/*.deb 2>/dev/null | wc -l | tr -d ' ') files"
if [ "$PYTHON" = "1" ]; then
  echo "    .docker-debs-python/: $(ls "$PY_DEST"/*.deb 2>/dev/null | wc -l | tr -d ' ') files"
fi
