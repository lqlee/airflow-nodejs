#!/bin/sh
# Build the airflow-nodejs Docker image.
#
# Prerequisites:
#   - dist/ must be compiled (run 'npm run build' via Wibey CLI or Bun-aware shell)
#   - Docker running
#
set -e

echo "==> Checking compiled output..."
if [ ! -f "dist/main.js" ]; then
  echo "ERROR: dist/main.js not found."
  echo "  Run 'npm run build' via the Wibey CLI first (requires Bun module resolution)." >&2
  exit 1
fi
echo "   dist/ ready ($(ls dist/ | wc -l | tr -d ' ') entries)"

echo "==> Building Docker image: airflow-nodejs:local ..."
docker build -t airflow-nodejs:local .
IMAGE_SIZE=$(docker image inspect airflow-nodejs:local --format='{{.Size}}' 2>/dev/null | awk '{printf "%.0f MB", $1/1024/1024}')
echo "   Image size: $IMAGE_SIZE"

echo ""
echo "✓ Build complete! Image: airflow-nodejs:local ($IMAGE_SIZE)"
echo ""
echo "Quick start (with local MongoDB):"
echo "  docker run -p 3000:3000 \\"
echo "    -e MONGO_URL=mongodb://host.docker.internal:27017 \\"
echo "    -e ENCRYPTION_KEY=\$(openssl rand -hex 32) \\"
echo "    -e ADMIN_KEY=my-bootstrap-key \\"
echo "    -v \$(pwd)/dags:/app/dags \\"
echo "    airflow-nodejs:local"
