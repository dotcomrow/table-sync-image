# Production-ready YugabyteDB to BigQuery CDC Processor
# Uses only well-established, battle-tested components
FROM python:3.11-slim

# --- Build arguments for version information ---
ARG BUILD_TIMESTAMP
ARG GIT_COMMIT
ARG GIT_TAG

# Yugabyte version to fetch (change as needed)
ARG YB_VERSION=2.20.1.0

# --- Environment ---
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP} \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_TAG=${GIT_TAG} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Make yb-admin discoverable by the app
    YB_ADMIN_PATH=/usr/local/bin/yb-admin

# --- System deps ---
# add: ca-certificates + tar for yb-admin install
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      tar \
      netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# --- Install yb-admin only (from YugabyteDB tarball) ---
# Detect architecture and pull the correct tarball:
#   amd64  -> linux-x86_64
#   arm64  -> linux-aarch64
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) yb_arch="linux-x86_64" ;; \
      arm64) yb_arch="linux-aarch64" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://downloads.yugabyte.com/yugabyte-${YB_VERSION}-${yb_arch}.tar.gz"; \
    echo "Downloading $url"; \
    curl -fsSL "$url" -o /tmp/yb.tar.gz; \
    mkdir -p /opt; \
    tar -xzf /tmp/yb.tar.gz -C /opt; \
    ln -sf "/opt/yugabyte-${YB_VERSION}/bin/yb-admin" /usr/local/bin/yb-admin; \
    rm -f /tmp/yb.tar.gz; \
    # sanity check (don’t fail build if it lacks --version)
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
COPY scripts/ ./scripts/

# --- Permissions / scripts ---
RUN chmod +x scripts/*.sh \
 && mkdir -p /app/logs \
 && chown -R cdc_user:cdc_user /app

# --- Healthcheck ---
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# --- Drop privileges ---
USER cdc_user

# --- Expose health port ---
EXPOSE 8080

# --- Entrypoint ---
ENTRYPOINT ["python", "src/table_sync_orchestrator.py"]
