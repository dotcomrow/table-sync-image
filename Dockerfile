# Production-ready YugabyteDB to BigQuery CDC Processor
FROM python:3.11-slim

# --- Build args / metadata ---
ARG BUILD_TIMESTAMP
ARG GIT_COMMIT
ARG GIT_TAG

# Yugabyte inputs (choose one of these strategies at build time)
# 1) Provide a direct tarball URL:
#      --build-arg YB_TARBALL_URL=https://downloads.yugabyte.com/releases/2.20.1.0/yugabyte-2.20.1.0-b123-linux-x86_64.tar.gz
# 2) Provide version + build (downloads.yugabyte.com):
#      --build-arg YB_VERSION=2.20.1.0 --build-arg YB_BUILD=123
# 3) Provide only version (auto-discover from GitHub releases):
#      --build-arg YB_VERSION=2.20.1.0
ARG YB_TARBALL_URL=""
ARG YB_VERSION="2.20.0.0"
ARG YB_BUILD=""

# --- Environment ---
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP} \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_TAG=${GIT_TAG} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Make yb-admin discoverable by the app
    YB_ADMIN_PATH=/usr/local/bin/yb-admin

# System deps: curl/tar/ca-certs for downloads, jq for GitHub JSON parsing, netcat for healthcheck scripts
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl tar jq netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# --- Install yb-admin (supports amd64 and arm64) ---
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) yb_arch="linux-x86_64" ;; \
      arm64) yb_arch="linux-aarch64" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    # Decide tarball URL
    url=""; \
    if [ -n "${YB_TARBALL_URL}" ]; then \
      url="${YB_TARBALL_URL}"; \
    elif [ -n "${YB_BUILD}" ]; then \
      url="https://downloads.yugabyte.com/yugabyte-${YB_VERSION}-b${YB_BUILD}-${yb_arch}.tar.gz"; \
    else \
      api="https://api.github.com/repos/yugabyte/yugabyte-db/releases/tags/v${YB_VERSION}"; \
      echo "Discovering Yugabyte tarball via ${api} ..."; \
      url="$(curl -fsSL "$api" | jq -r ".assets[]?.browser_download_url | select( test(\"${yb_arch}.*\\\\.tar\\\\.gz$\"))" | head -n1)"; \
    fi; \
    if [ -z "$url" ]; then \
      echo "ERROR: Could not determine Yugabyte tarball URL. Provide YB_TARBALL_URL or YB_BUILD (with YB_VERSION), or ensure GitHub API access." >&2; \
      exit 1; \
    fi; \
    echo "Downloading $url"; \
    curl -fsSL "$url" -o /tmp/yb.tar.gz; \
    ybdir="$(tar -tzf /tmp/yb.tar.gz | head -n1 | cut -d/ -f1)"; \
    mkdir -p /opt; \
    tar -xzf /tmp/yb.tar.gz -C /opt; \
    ln -sf "/opt/${ybdir}/bin/yb-admin" /usr/local/bin/yb-admin; \
    rm -f /tmp/yb.tar.gz; \
    /usr/local/bin/yb-admin --version || true

# --- Security: non-root user ---
RUN useradd --create-home --shell /bin/bash cdc_user
WORKDIR /app
RUN chown cdc_user:cdc_user /app

# --- Python deps (cached layer) ---
COPY requirements.production.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- Application code ---
COPY src/ ./src/
COPY config/ ./config/

# --- Healthcheck ---
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# --- Drop privileges ---
USER cdc_user

# --- Expose health port ---
EXPOSE 8080

# --- Entrypoint ---
ENTRYPOINT ["python", "src/table_sync_orchestrator.py"]
