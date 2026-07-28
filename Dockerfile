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
# bun:1.3-slim is Debian-based. Shells available for shell tasks:
#   sh    — always available (POSIX sh)
#   bash  — default interpreter, pre-installed in bun:1.3-slim
#   zsh   — installed from pre-downloaded .deb (no network needed)
#   tcsh  — installed from pre-downloaded .deb (no network needed)
#
# .deb files in .docker-debs/ were downloaded from snapshot.debian.org on a
# public network and committed to the repo for offline builds.
# To refresh: scripts/download-shell-debs.sh
FROM --platform=${TARGETPLATFORM:-linux/arm64} generic.ci.artifacts.walmart.com/hub-docker-release-remote/oven/bun:1.3-slim AS runtime

# Install extra shells via pre-downloaded .deb packages (no apt/network needed)
COPY .docker-debs/ /tmp/debs/
RUN dpkg -i \
      /tmp/debs/libtinfo6_*.deb \
      /tmp/debs/libncursesw6_*.deb \
      /tmp/debs/zsh-common_*.deb \
      /tmp/debs/zsh_*.deb \
      /tmp/debs/tcsh_*.deb && \
    rm -rf /tmp/debs

# Security: non-root user (useradd is available on Debian)
RUN groupadd -r airflow && useradd -r -g airflow airflow

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
