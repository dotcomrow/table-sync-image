# Production-ready YugabyteDB to BigQuery CDC Processor
FROM python:3.11-slim

# --- Build metadata ---
ARG BUILD_TIMESTAMP
ARG GIT_COMMIT
ARG GIT_TAG

# Yugabyte inputs:
# 1) Provide a direct tarball URL (best):
#    --build-arg YB_TARBALL_URL=https://downloads.yugabyte.com/releases/2.20.0.0/yugabyte-2.20.0.0-b42-linux-x86_64.tar.gz
# 2) Or provide version + build:
#    --build-arg YB_VERSION=2.20.0.0 --build-arg YB_BUILD=42
# 3) Or just version (auto-discover build by scraping the release index page):
#    --build-arg YB_VERSION=2.20.0.0

ARG YB_VERSION="2025.1.0.1"
ARG YB_BUILD="3"
ARG YB_ARCH="linux-x86_64"
ARG YB_DOWNLOAD_URL="https://software.yugabyte.com"

# --- Env ---
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP} \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_TAG=${GIT_TAG} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Tell the app where to find yb-admin
    YB_ADMIN_PATH=/usr/local/bin/yb-admin

# System deps: curl/tar/grep for discovery, netcat for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl tar grep netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# --- Install yb-admin (supports amd64 and arm64) ---
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) yb_arch="linux-x86_64" ;; \
      arm64) yb_arch="linux-aarch64" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url=""; \
    if [ -n "${YB_TARBALL_URL}" ]; then \
      url="${YB_TARBALL_URL}"; \
    elif [ -n "${YB_BUILD}" ]; then \
      url="${YB_DOWNLOAD_URL}/releases/${YB_VERSION}/yugabyte-${YB_VERSION}-b${YB_BUILD}-${yb_arch}.tar.gz"; \
    else \
      index="${YB_DOWNLOAD_URL}/releases/${YB_VERSION}/"; \
      echo "Discovering Yugabyte tarball via ${index} ..."; \
      # Look for yugabyte-<ver>-b<digits>-<arch>.tar.gz on the index page
      fname="$(curl -fsSL "$index" | grep -Eo "yugabyte-${YB_VERSION}-b[0-9]+-${yb_arch}\.tar\.gz" | head -n1 || true)"; \
      if [ -n "$fname" ]; then \
        url="${index}${fname}"; \
      fi; \
    fi; \
    if [ -z "$url" ]; then \
      echo "ERROR: Could not determine Yugabyte tarball URL. Provide YB_TARBALL_URL or YB_BUILD with YB_VERSION, or ensure the index page is reachable." >&2; \
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
COPY src/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- Application code ---
COPY src/ ./src/

# --- Healthcheck ---
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# --- Drop privileges ---
USER cdc_user

# --- Expose health port ---
EXPOSE 8080

# --- Entrypoint ---
ENTRYPOINT ["python", "src/table_sync_orchestrator.py"]

# Ensure PYTHONPATH is set before the entrypoint
ENV PYTHONPATH="/app/src"
