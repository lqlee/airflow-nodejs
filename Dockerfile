# ── airflow-nodejs Dockerfile ─────────────────────────────────────────────────
#
# Multi-stage build:
#   Stage 1 (deps): node:22-alpine — install production deps via npm + Walmart Artifactory
#   Stage 2 (runtime): oven/bun:1.3-alpine — run with Bun for ESM module resolution
#
# Build prerequisites (run on host before `docker build`):
#   npm run build    (compile TypeScript → dist/)
#
# Platform:
#   Default: auto-detected from host (arm64 on Apple Silicon, amd64 on x86)
#   Override: docker build --build-arg TARGETPLATFORM=linux/amd64 .
#   Or use docker-build.sh --platform linux/amd64
#
# TARGETPLATFORM is a Docker built-in ARG — set automatically when using
# `docker buildx build --platform` or passed via --build-arg.
ARG TARGETPLATFORM

# ── Stage 1: Install production deps ──────────────────────────────────────────
FROM --platform=${TARGETPLATFORM:-linux/arm64} node:22-alpine3.21 AS deps

WORKDIR /app

# Copy package manifest, lockfile, and Walmart registry config
COPY package.json package-lock.json .npmrc ./

# Install production-only deps via Walmart's internal Artifactory npm registry
RUN npm ci --omit=dev


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM --platform=${TARGETPLATFORM:-linux/arm64} generic.ci.artifacts.walmart.com/hub-docker-release-remote/oven/bun:1.3-alpine AS runtime

# Note on shell tasks:
#   This Alpine image ships with /bin/sh (busybox ash) only.
#   Shell tasks default to 'bash' — if bash is not available they'll ENOENT.
#   Use interpreter: 'sh' for guaranteed compatibility on Alpine.
#   To add bash: extend this image with `RUN apk add --no-cache bash`
#   (requires access to an Alpine mirror; blocked on Walmart network — use a custom base image).

# Security: non-root user
RUN addgroup -S airflow && adduser -S airflow -G airflow

WORKDIR /app

# Copy production node_modules from the deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy package.json (needed for Bun ESM module resolution)
COPY package.json ./

# Copy pre-compiled JavaScript (built on host with npm run build)
COPY dist/ ./dist/

# Copy static UI assets
COPY public/ ./public/

# Volume mount point for user dag files
RUN mkdir -p /app/dags && chown -R airflow:airflow /app

USER airflow

EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{process.exit(d.includes('\"status\":\"ok\"')?0:1)})}).on('error',()=>process.exit(1))"

CMD ["bun", "run", "dist/main.js"]
