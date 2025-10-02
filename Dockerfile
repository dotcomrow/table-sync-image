# Production-ready YugabyteDB to BigQuery CDC Processor
# Uses only well-established, battle-tested components
FROM python:3.11-slim

# Build arguments for version information
ARG BUILD_TIMESTAMP
ARG GIT_COMMIT
ARG GIT_TAG

# Set environment variables for build info
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP} \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_TAG=${GIT_TAG} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash cdc_user
WORKDIR /app
RUN chown cdc_user:cdc_user /app

# Copy requirements first for better Docker layer caching
COPY requirements.production.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Make scripts executable
RUN chmod +x scripts/*.sh

# Set up logging directory
RUN mkdir -p /app/logs && chown cdc_user:cdc_user /app/logs

# Health check using curl (built into image)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Switch to non-root user
USER cdc_user

# Expose health check port
EXPOSE 8080

# Use exec form to ensure proper signal handling
# Run the table sync orchestrator with orchestrator config
ENTRYPOINT ["python", "src/table_sync_orchestrator.py"]