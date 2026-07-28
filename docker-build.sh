#!/bin/sh
# Build the airflow-nodejs Docker image.
#
# Usage:
#   ./docker-build.sh                               # base image (sh/bash/zsh/tcsh)
#   ./docker-build.sh --variant python              # python variant (adds python3.13)
#   ./docker-build.sh --variant java                # java variant (python3 + JRE 21)
#   ./docker-build.sh --variant java --jdk 25       # java variant with JDK 25
#   ./docker-build.sh --platform linux/amd64        # build for x86 servers
#   ./docker-build.sh --platform linux/arm64        # build for ARM (Apple Silicon, AWS Graviton)
#
# Supported JDK versions: 21 (LTS, default), 25 (latest)
# Note: JDK 8/11/17 are not in Debian trixie.
#
# Prerequisites:
#   - dist/ must be compiled (npm run build — handled automatically)
#   - .docker-debs/ must exist       (run ./scripts/download-shell-debs.sh on public WiFi)
#   - .docker-debs-python/ required  for --variant python|java
#   - .docker-debs-java/ required    for --variant java
#   - Docker running
#
set -e

# ── Parse args ─────────────────────────────────────────────────────────────────
PLATFORM=""
VARIANT=""
JDK_VER="21"

while [ $# -gt 0 ]; do
  case "$1" in
    --platform)     PLATFORM="$2";  shift 2 ;;
    --platform=*)   PLATFORM="${1#--platform=}"; shift ;;
    --variant)      VARIANT="$2";   shift 2 ;;
    --variant=*)    VARIANT="${1#--variant=}"; shift ;;
    --jdk)          JDK_VER="$2";   shift 2 ;;
    --jdk=*)        JDK_VER="${1#--jdk=}"; shift ;;
    *)              echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# Validate JDK version
if [ "$VARIANT" = "java" ]; then
  case "$JDK_VER" in
    21|25) ;;
    *) echo "ERROR: Unsupported JDK version '$JDK_VER'. Use 21 or 25." >&2; exit 1 ;;
  esac
fi

# Auto-detect host platform if not specified
if [ -z "$PLATFORM" ]; then
  HOST_ARCH=$(uname -m)
  case "$HOST_ARCH" in
    arm64|aarch64) PLATFORM="linux/arm64" ;;
    x86_64|amd64)  PLATFORM="linux/amd64" ;;
    *)
      echo "WARNING: Unknown host arch '$HOST_ARCH', defaulting to linux/amd64" >&2
      PLATFORM="linux/amd64" ;;
  esac
  echo "==> Platform: $PLATFORM (auto-detected from host arch: $HOST_ARCH)"
else
  echo "==> Platform: $PLATFORM (explicit)"
fi

# ── Compile TypeScript ─────────────────────────────────────────────────────────
echo "==> Compiling TypeScript (npm run build)..."
npm run build
echo "   dist/ ready ($(ls dist/ | wc -l | tr -d ' ') entries)"

# ── Validate prerequisites ─────────────────────────────────────────────────────
if [ ! -d ".docker-debs" ] || [ -z "$(ls .docker-debs/*.deb 2>/dev/null)" ]; then
  echo "ERROR: .docker-debs/ is missing or empty." >&2
  echo "       Run: ./scripts/download-shell-debs.sh" >&2
  exit 1
fi

if [ "$VARIANT" = "python" ] || [ "$VARIANT" = "java" ]; then
  if [ ! -d ".docker-debs-python" ] || [ -z "$(ls .docker-debs-python/*.deb 2>/dev/null)" ]; then
    echo "ERROR: .docker-debs-python/ is missing or empty." >&2
    echo "       Run: ./scripts/download-shell-debs.sh --python" >&2
    exit 1
  fi
fi

if [ "$VARIANT" = "java" ]; then
  if [ ! -d ".docker-debs-java" ] || [ -z "$(ls .docker-debs-java/*.deb 2>/dev/null)" ]; then
    echo "ERROR: .docker-debs-java/ is missing or empty." >&2
    echo "       Run: ./scripts/download-shell-debs.sh --java [--jdk $JDK_VER]" >&2
    exit 1
  fi
  if ! ls ".docker-debs-java/openjdk-${JDK_VER}-jre-headless_"*.deb >/dev/null 2>&1; then
    echo "ERROR: .docker-debs-java/ does not contain openjdk-${JDK_VER}-jre-headless_*.deb" >&2
    echo "       Run: ./scripts/download-shell-debs.sh --java --jdk $JDK_VER" >&2
    exit 1
  fi
fi

# ── Select tag ─────────────────────────────────────────────────────────────────
case "$VARIANT" in
  python) TAG="airflow-nodejs:python";         echo "==> Building: $TAG (base + python 3.13)" ;;
  java)   TAG="airflow-nodejs:java${JDK_VER}"; echo "==> Building: $TAG (base + python 3.13 + JDK $JDK_VER)" ;;
  *)      TAG="airflow-nodejs:local";          echo "==> Building: $TAG (base — shells only)" ;;
esac

# ── Build (single Dockerfile, variant selected via --build-arg VARIANT) ────────
docker build \
  --build-arg TARGETPLATFORM="$PLATFORM" \
  --build-arg VARIANT="${VARIANT:-base}" \
  --build-arg JDK_VER="$JDK_VER" \
  -t "$TAG" \
  .

IMAGE_SIZE=$(docker image inspect "$TAG" --format='{{.Size}}' 2>/dev/null | awk '{printf "%.0f MB", $1/1024/1024}')
echo "   Image size: $IMAGE_SIZE  Platform: $PLATFORM"

echo ""
echo "✓ Build complete!  Image: $TAG ($IMAGE_SIZE)  Platform: $PLATFORM"
echo ""
echo "Quick start (with local MongoDB):"
echo "  docker run -p 3000:3000 \\"
echo "    -e MONGO_URL=mongodb://host.docker.internal:27017 \\"
echo "    -e ENCRYPTION_KEY=\$(openssl rand -hex 32) \\"
echo "    -e ADMIN_KEY=my-bootstrap-key \\"
echo "    -v \$(pwd)/dags:/app/dags \\"
echo "    $TAG"
echo ""
echo "To build for a different platform or variant:"
echo "  ./docker-build.sh --platform linux/amd64                   # x86 servers"
echo "  ./docker-build.sh --platform linux/arm64                   # ARM (Apple Silicon / Graviton)"
echo "  ./docker-build.sh --variant python                         # adds python3.13 (~114 MB)"
echo "  ./docker-build.sh --variant java                           # python3 + JDK 21 (~225 MB)"
echo "  ./docker-build.sh --variant java --jdk 25                  # python3 + JDK 25 (~240 MB)"
echo "  ./docker-build.sh --variant java --jdk 25 --platform linux/amd64"
