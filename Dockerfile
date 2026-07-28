# ── airflow-nodejs Dockerfile ─────────────────────────────────────────────────
#
# Single Dockerfile for all image variants — controlled by VARIANT build arg.
#
# ┌────────────────┬────────────────────────────────────┬──────────┐
# │ VARIANT        │ Runtimes included                  │ Size     │
# ├────────────────┼────────────────────────────────────┼──────────┤
# │ base (default) │ sh, bash, zsh, tcsh                │ ~90 MB   │
# │ python         │ base + python 3.13                 │ ~114 MB  │
# │ java           │ base + python 3.13 + JRE 21 (LTS)  │ ~225 MB  │
# │ java (JDK 25)  │ base + python 3.13 + JRE 25       │ ~249 MB  │
# └────────────────┴────────────────────────────────────┴──────────┘
#
# JDK 25 is backwards-compatible — runs JARs compiled for Java 8/11/17/21/25.
#
# Build via docker-build.sh (recommended):
#   ./docker-build.sh                            # base
#   ./docker-build.sh --variant python           # python
#   ./docker-build.sh --variant java             # java JDK 21
#   ./docker-build.sh --variant java --jdk 25    # java JDK 25
#
# Or directly:
#   docker build --build-arg VARIANT=base   -t airflow-nodejs:base .
#   docker build --build-arg VARIANT=python -t airflow-nodejs:python .
#   docker build --build-arg VARIANT=java   --build-arg JDK_VER=21 -t airflow-nodejs:java21 .
#   docker build --build-arg VARIANT=java   --build-arg JDK_VER=25 -t airflow-nodejs:java25 .
#
# Prerequisites (.docker-debs-*/ folders are git-ignored, populate on public WiFi):
#   ./scripts/download-shell-debs.sh                      # always required
#   ./scripts/download-shell-debs.sh --python             # for python + java variants
#   ./scripts/download-shell-debs.sh --java               # for java variant (JDK 21)
#   ./scripts/download-shell-debs.sh --java --jdk 25      # for java variant (JDK 25)
#
ARG TARGETPLATFORM
ARG VARIANT=base
ARG JDK_VER=21


# ══════════════════════════════════════════════════════════════════════════════
# Stage: npm deps (shared by all variants)
# ══════════════════════════════════════════════════════════════════════════════
FROM --platform=${TARGETPLATFORM:-linux/arm64} node:22-alpine3.21 AS deps
WORKDIR /app
COPY package.json package-lock.json .npmrc ./
RUN npm ci --omit=dev


# ══════════════════════════════════════════════════════════════════════════════
# Stage: base — shells only (sh/bash/zsh/tcsh)
# ══════════════════════════════════════════════════════════════════════════════
FROM --platform=${TARGETPLATFORM:-linux/arm64} generic.ci.artifacts.walmart.com/hub-docker-release-remote/oven/bun:1.3-slim AS stage-base

# bash is pre-installed in bun:1.3-slim.
# Install from .docker-debs/:
#   zsh, tcsh          — shell task interpreters
#   docker-cli         — required for container tasks (docker run per task)
COPY .docker-debs/ /tmp/debs/
RUN dpkg -i --force-depends /tmp/debs/*.deb && dpkg --configure -a && rm -rf /tmp/debs


# ══════════════════════════════════════════════════════════════════════════════
# Stage: python — base + python 3.13
# ══════════════════════════════════════════════════════════════════════════════
FROM stage-base AS stage-python

COPY .docker-debs-python/ /tmp/pydebs/
RUN dpkg -i --force-depends /tmp/pydebs/*.deb && dpkg --configure -a && rm -rf /tmp/pydebs
RUN python3 --version


# ══════════════════════════════════════════════════════════════════════════════
# Stage: java — base + python 3.13 + OpenJDK JRE
# JDK_VER=21 → LTS (runs Java 8–21 bytecode)
# JDK_VER=25 → latest (runs Java 8–25 bytecode, backwards-compatible)
# ══════════════════════════════════════════════════════════════════════════════
FROM stage-python AS stage-java

ARG JDK_VER=21
COPY .docker-debs-java/ /tmp/javadebs/
RUN dpkg -i --force-depends /tmp/javadebs/*.deb && dpkg --configure -a && rm -rf /tmp/javadebs
RUN java -version 2>&1 | head -1


# ══════════════════════════════════════════════════════════════════════════════
# Final stage — select the right stage via VARIANT arg
# ══════════════════════════════════════════════════════════════════════════════
FROM stage-${VARIANT} AS runtime

# DOCKER_GID: GID of the docker socket on the host (/var/run/docker.sock).
# Set this to enable container tasks (docker run per task).
# Find it with: stat -c %g /var/run/docker.sock   (Linux)
#               stat -f %g /var/run/docker.sock   (macOS)
# Leave as 0 if container tasks are not needed (socket not mounted).
ARG DOCKER_GID=0

# Create airflow user; optionally add to the docker socket group.
RUN groupadd -r airflow && useradd -r -g airflow airflow && \
    if [ "${DOCKER_GID}" != "0" ]; then \
      groupadd -g "${DOCKER_GID}" docker-socket 2>/dev/null || true && \
      usermod -aG "docker-socket" airflow; \
    fi

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY dist/ ./dist/
COPY public/ ./public/

RUN mkdir -p /app/dags && chown -R airflow:airflow /app

USER airflow

EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{process.exit(d.includes('\"status\":\"ok\"')?0:1)})}).on('error',()=>process.exit(1))"

CMD ["bun", "run", "dist/main.js"]
