#!/bin/bash
# Production build and test script

set -euo pipefail

echo "🏗️  Building Production Table Sync Orchestrator"
echo "=============================================="

# Configuration
IMAGE_NAME="table-sync-orchestrator"
IMAGE_TAG="latest"
DOCKERFILE="Dockerfile.production"

# Build the image
echo "📦 Building Docker image..."
docker build -f ${DOCKERFILE} -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo "✅ Image built successfully: ${IMAGE_NAME}:${IMAGE_TAG}"

# Test the image
echo "🧪 Testing image..."

# Test 1: Image runs without errors
echo "  → Testing image startup..."
docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} --help > /dev/null 2>&1
echo "  ✅ Image startup test passed"

# Test 2: Health check script exists
echo "  → Testing health check script..."
docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} ls -la /app/scripts/health_check.sh > /dev/null 2>&1
echo "  ✅ Health check script found"

# Test 3: Configuration file exists
echo "  → Testing configuration file..."
docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} ls -la /app/config/production.yaml > /dev/null 2>&1
echo "  ✅ Configuration file found"

# Test 4: Non-root user
echo "  → Testing security (non-root user)..."
USER_ID=$(docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} id -u)
if [ "$USER_ID" != "0" ]; then
    echo "  ✅ Running as non-root user (UID: $USER_ID)"
else
    echo "  ❌ WARNING: Running as root user"
    exit 1
fi

# Test 5: Python packages installed
echo "  → Testing Python dependencies..."
docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} python -c "
import kafka
import google.cloud.bigquery
import yaml
import structlog
import prometheus_client
import flask
import tenacity
print('All dependencies available')
" > /dev/null 2>&1
echo "  ✅ Python dependencies test passed"

echo ""
echo "🎉 All tests passed!"
echo ""
echo "📋 Next Steps:"
echo "  1. Push to registry: docker push ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  2. Update kubernetes.yaml with correct image"
echo "  3. Deploy: kubectl apply -f kubernetes.yaml"
echo ""
echo "🔗 Useful commands:"
echo "  • Health check: curl http://localhost:8080/health"
echo "  • Metrics: curl http://localhost:9090/metrics"
echo "  • Logs: docker logs ${IMAGE_NAME}"
echo ""
echo "📚 Documentation: README.production.md"