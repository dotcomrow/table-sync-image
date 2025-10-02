#!/bin/bash
# Production Health Check Script
# Returns 0 if healthy, 1 if unhealthy

set -euo pipefail

# Configuration
HEALTH_ENDPOINT="${HEALTH_ENDPOINT:-http://localhost:8080/health}"
TIMEOUT="${HEALTH_TIMEOUT:-10}"
MAX_RETRIES="${HEALTH_MAX_RETRIES:-3}"

# Function to check health
check_health() {
    local retry_count=0
    
    while [ $retry_count -lt $MAX_RETRIES ]; do
        if curl -f -s --max-time $TIMEOUT "$HEALTH_ENDPOINT" > /dev/null 2>&1; then
            echo "✅ Health check passed"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $MAX_RETRIES ]; then
            echo "⚠️  Health check failed, retrying... ($retry_count/$MAX_RETRIES)"
            sleep 2
        fi
    done
    
    echo "❌ Health check failed after $MAX_RETRIES attempts"
    return 1
}

# Function to check dependencies
check_dependencies() {
    echo "🔍 Checking dependencies..."
    
    # Check Kafka connectivity
    if ! nc -z "${KAFKA_HOST:-kafka}" "${KAFKA_PORT:-9092}" 2>/dev/null; then
        echo "❌ Cannot connect to Kafka"
        return 1
    fi
    echo "✅ Kafka connectivity OK"
    
    # Check BigQuery credentials
    if [ ! -f "${GOOGLE_APPLICATION_CREDENTIALS:-/app/credentials/service-account.json}" ]; then
        echo "❌ BigQuery credentials not found"
        return 1
    fi
    echo "✅ BigQuery credentials found"
    
    return 0
}

# Main health check
main() {
    echo "🏥 Starting health check..."
    
    # Check dependencies first
    if ! check_dependencies; then
        exit 1
    fi
    
    # Check application health
    if ! check_health; then
        exit 1
    fi
    
    echo "✅ All health checks passed"
    exit 0
}

# Run if called directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi