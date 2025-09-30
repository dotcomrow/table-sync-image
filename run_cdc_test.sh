#!/bin/bash

# CDC Compatibility Test Deployment Script
# This script deploys the application in test mode to diagnose CDC issues

echo "🧪 =============================================="
echo "🧪 CDC COMPATIBILITY TEST DEPLOYMENT"
echo "🧪 =============================================="

# Configuration
IMAGE_NAME="table-sync-app:test-mode"
CONTAINER_NAME="table-sync-cdc-test"

# Clean up any existing test container
echo "🧹 Cleaning up existing test container..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

echo "🚀 Starting CDC compatibility test..."
echo "📋 Configuration:"
echo "   - CDC Test Mode: ENABLED"
echo "   - Image: $IMAGE_NAME"
echo "   - Container: $CONTAINER_NAME"
echo ""

# Run the container with test mode enabled
docker run --name $CONTAINER_NAME \
    --rm \
    -e CDC_TEST_MODE=true \
    -e BIGQUERY_PROJECT_ID=test-project \
    -e DATABASE_URL=postgresql://yugabyte@yb-tserver-service.yugabyte.svc.cluster.local:5433/mcp \
    -e DEBEZIUM_CONNECTOR_URL=http://kafka-connect-service:8083 \
    -e LOG_LEVEL=INFO \
    -e CLEANUP_CDC_ON_STARTUP=true \
    --network host \
    $IMAGE_NAME

echo ""
echo "🧪 =============================================="
echo "🧪 CDC COMPATIBILITY TEST COMPLETED"
echo "🧪 =============================================="
echo ""
echo "📋 Results Analysis:"
echo "✅ If test passed: YugabyteDB/Debezium are compatible, issue is with complex schemas"
echo "❌ If test failed with NullPointerException: Version incompatibility - need different versions"
echo "❌ If test failed with yb-admin conflicts: CDC metadata corruption - redeploy needed"
echo "❌ If test failed with connectivity: Service availability issues"
echo ""
echo "📄 Check the logs above for specific error patterns and recommendations"