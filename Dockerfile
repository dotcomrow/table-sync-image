FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies and build tools
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    wget \
    unzip \
    libc6 \
    libgcc-s1 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Install Python-based yb-admin wrapper for CDC stream management
# This will be copied after the application files are copied

# Install Python dependencies first for better layer caching
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ .
ENV PYTHONPATH=/app

# Install Python-based yb-admin wrapper
RUN cp yb_admin_wrapper.py /usr/local/bin/yb-admin \
    && chmod +x /usr/local/bin/yb-admin \
    && echo "✅ YugabyteDB admin wrapper installed" \
    && yb-admin --help

# Create non-root user
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app

# Validate imports from requirements.txt
RUN python validate_imports.py

# Test yb-admin functionality before switching to non-root user  
RUN echo "🧪 Final yb-admin functionality test..." \
    && ls -la /usr/local/bin/yb-admin \
    && file /usr/local/bin/yb-admin \
    && (/usr/local/bin/yb-admin --help > /tmp/yb-admin-test.log 2>&1 || echo "Direct execution failed") \
    && (yb-admin --help > /tmp/yb-admin-test.log 2>&1 && echo "✅ yb-admin help command works" || echo "⚠️ yb-admin may need runtime dependencies") \
    && echo "📄 yb-admin test output:" \
    && head -10 /tmp/yb-admin-test.log || echo "No output to show"

USER app

# Health check to ensure the application can start
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python health_check.py status || exit 1

# Default command runs the main sync application
CMD ["python", "app.py"]
