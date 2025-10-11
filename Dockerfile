# Production-ready YugabyteDB to BigQuery CDC Processor
FROM python:3.11-slim

# --- Build metadata ---
ARG BUILD_TIMESTAMP
ARG GIT_COMMIT
ARG GIT_TAG

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
    url="https://software.yugabyte.com/releases/2025.1.1.1/yugabyte-2025.1.1.1-b1-linux-x86_64.tar.gz"; \
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

# Ensure PYTHONPATH is set before the entrypoint
ENV PYTHONPATH="/app/src"

# --- Entrypoint ---
ENTRYPOINT ["python", "src/table_sync_orchestrator.py"]