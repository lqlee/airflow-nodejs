#!/bin/sh
# Download pre-built Debian shell .deb packages for offline Docker builds.
# Run this on a machine with public internet access, then commit .docker-debs/.
#
# Usage: ./scripts/download-shell-debs.sh [arm64|amd64]
# Default arch: auto-detected from host.

set -e

ARCH=${1:-$(uname -m)}
case "$ARCH" in
  arm64|aarch64) ARCH="arm64" ;;
  x86_64|amd64)  ARCH="amd64" ;;
  *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

BASE="http://snapshot.debian.org/archive/debian/20260505T000000Z"
DEST="$(dirname "$0")/../.docker-debs"
mkdir -p "$DEST"

echo "==> Fetching Debian trixie package index for $ARCH..."
PKG_DATA=$(curl -sL "$BASE/dists/trixie/main/binary-$ARCH/Packages.gz" | gunzip)

for pkg in zsh zsh-common tcsh libncursesw6 libtinfo6; do
  path=$(echo "$PKG_DATA" | awk "/^Package: $pkg$/{found=1} found && /^Filename:/{print \$2; exit}")
  fname=$(basename "$path")
  echo "  Downloading $fname..."
  curl -sL "$BASE/$path" -o "$DEST/$fname"
done

echo "==> Done. Files in $DEST:"
ls -lh "$DEST"
