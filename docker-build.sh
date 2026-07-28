#!/bin/sh
# Build the airflow-nodejs Docker image.
#
# Usage:
#   ./docker-build.sh                          # base image (sh/bash/zsh/tcsh)
#   ./docker-build.sh --variant python         # python variant (adds python3.13)
#   ./docker-build.sh --platform linux/amd64   # build for x86 servers
#   ./docker-build.sh --platform linux/arm64   # build for ARM (Apple Silicon, AWS Graviton)
#
# Prerequisites:
#   - dist/ must be compiled (run 'npm run build')
#   - .docker-debs/ must exist (run ./scripts/download-shell-debs.sh on public WiFi)
#   - .docker-debs-python/ must exist for --variant python
#   - Docker running
#
set -e

# ── Parse args ─────────────────────────────────────────────────────────────────
PLATFORM=""
VARIANT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --platform)
      PLATFORM="$2"; shift 2 ;;
    --platform=*)
      PLATFORM="${1#--platform=}"; shift ;;
    --variant)
      VARIANT="$2"; shift 2 ;;
    --variant=*)
      VARIANT="${1#--variant=}"; shift ;;
    *)
      echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

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

# ── Build ──────────────────────────────────────────────────────────────────────
if [ "$VARIANT" = "python" ]; then
  TAG="airflow-nodejs:python"
  DOCKERFILE="Dockerfile.python"
  echo "==> Building Python variant image: $TAG ..."
  if [ ! -d ".docker-debs-python" ]; then
    echo "ERROR: .docker-debs-python/ not found." >&2
    echo "       Run: ./scripts/download-shell-debs.sh --python" >&2
    exit 1
  fi
else
  TAG="airflow-nodejs:local"
  DOCKERFILE="Dockerfile"
  echo "==> Building Docker image: $TAG ..."
fi

docker build \
  --build-arg TARGETPLATFORM="$PLATFORM" \
  -f "$DOCKERFILE" \
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
echo "  ./docker-build.sh --platform linux/amd64          # x86 servers"
echo "  ./docker-build.sh --platform linux/arm64          # ARM (Apple Silicon / Graviton)"
echo "  ./docker-build.sh --variant python                # adds python3.13 support"
echo "  ./docker-build.sh --variant python --platform linux/amd64"
