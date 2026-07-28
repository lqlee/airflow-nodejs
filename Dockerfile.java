# ── airflow-nodejs:java variant ────────────────────────────────────────────────
#
# Extends the python variant with an OpenJDK headless JRE for java task definitions.
# Use this when your DAGs contain tasks with a `java:` block.
#
# Supported JDK versions (Debian trixie):  21 (LTS, default)  |  25 (latest)
# JDK 25 is backwards-compatible: runs JARs compiled for Java 8/11/17/21/25.
# Use --jdk 25 if you want one image that covers all Java versions.
#
# Build:
#   ./docker-build.sh --variant java               # JDK 21 (default)
#   ./docker-build.sh --variant java --jdk 25      # JDK 25
#
# Includes (on top of base):
#   python3.13       (python tasks)
#   OpenJDK JRE      (java tasks — headless, no GUI)
#
# Image sizes (approximate):
#   JDK 21: ~225 MB   JDK 25: ~240 MB
#
# .docker-debs-java/ holds pre-downloaded .deb files (git-ignored).
# Refresh on public WiFi:
#   ./scripts/download-shell-debs.sh --python --java           # JDK 21
#   ./scripts/download-shell-debs.sh --python --java --jdk 25  # JDK 25
#
ARG TARGETPLATFORM
# JDK version baked into the image — set by docker-build.sh via --build-arg JDK_VER=25
ARG JDK_VER=21

# ── Stage 1: npm deps ─────────────────────────────────────────────────────────
FROM --platform=${TARGETPLATFORM:-linux/arm64} node:22-alpine3.21 AS deps
WORKDIR /app
COPY package.json package-lock.json .npmrc ./
RUN npm ci --omit=dev


# ── Stage 2: Runtime + shells + python + java ─────────────────────────────────
FROM --platform=${TARGETPLATFORM:-linux/arm64} generic.ci.artifacts.walmart.com/hub-docker-release-remote/oven/bun:1.3-slim AS runtime

# Re-declare ARG after FROM so it's in scope for RUN
ARG JDK_VER=21

# ── Shells (zsh, tcsh) ────────────────────────────────────────────────────────
COPY .docker-debs/ /tmp/debs/
RUN dpkg -i --force-depends /tmp/debs/*.deb && dpkg --configure -a && rm -rf /tmp/debs

# ── Python 3.13 ───────────────────────────────────────────────────────────────
COPY .docker-debs-python/ /tmp/pydebs/
RUN dpkg -i --force-depends /tmp/pydebs/*.deb && dpkg --configure -a && rm -rf /tmp/pydebs
RUN python3 --version

# ── OpenJDK JRE (version selected by JDK_VER build arg) ──────────────────────
# .docker-debs-java/ must contain the correct openjdk-${JDK_VER}-jre-headless_*.deb
# (and its shared deps). Produced by: ./scripts/download-shell-debs.sh --java [--jdk NN]
COPY .docker-debs-java/ /tmp/javadebs/
RUN dpkg -i --force-depends /tmp/javadebs/*.deb && dpkg --configure -a && rm -rf /tmp/javadebs
RUN java -version 2>&1 | head -1

# ── App ───────────────────────────────────────────────────────────────────────
RUN groupadd -r airflow && useradd -r -g airflow airflow

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
