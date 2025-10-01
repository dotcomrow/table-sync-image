#!/bin/bash

# YugabyteDB Diagnostic and Cleanup Script
# Run this before considering a full redeploy

echo "🔍 YugabyteDB Health Check and Cleanup Script"
echo "=============================================="

# Function to run kubectl commands safely
run_kubectl() {
    local cmd="$1"
    echo "Running: kubectl $cmd"
    kubectl $cmd 2>/dev/null || echo "⚠️  Command failed or no results"
}

# Function to run yb-admin commands in pod
run_yb_admin() {
    local cmd="$1"
    local pod=$(kubectl get pods -n yugabyte -l app=yb-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$pod" ]; then
        echo "Running yb-admin: $cmd"
        kubectl exec -n yugabyte $pod -- /home/yugabyte/bin/yb-admin $cmd 2>/dev/null || echo "⚠️  yb-admin command failed"
    else
        echo "⚠️  No YugabyteDB master pod found"
    fi
}

echo ""
echo "1. 📊 Checking YugabyteDB cluster status..."
echo "--------------------------------------------"
run_kubectl "get pods -n yugabyte"
run_kubectl "get services -n yugabyte"

echo ""
echo "2. 🔍 Checking YugabyteDB master health..."
echo "------------------------------------------"
run_yb_admin "--master_addresses=yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100 list_tablets"

echo ""
echo "3. 📋 Listing current CDC streams..."
echo "------------------------------------"
run_yb_admin "--master_addresses=yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100 list_cdc_streams"

echo ""
echo "4. 🗑️  CDC Stream Cleanup Options..."
echo "------------------------------------"
echo "To clean up CDC streams, you can:"
echo "   a) Use the table-sync application's cleanup function"
echo "   b) Manually delete streams with yb-admin delete_cdc_stream <stream_id>"
echo "   c) Restart YugabyteDB pods (less disruptive than full redeploy)"

echo ""
echo "5. 🔄 Pod Restart Options (instead of full redeploy)..."
echo "------------------------------------------------------"
echo "# Restart all YugabyteDB pods (preserves data, clears transient issues):"
echo "kubectl rollout restart statefulset/yb-master -n yugabyte"
echo "kubectl rollout restart statefulset/yb-tserver -n yugabyte"
echo ""
echo "# Wait for pods to be ready:"
echo "kubectl rollout status statefulset/yb-master -n yugabyte"
echo "kubectl rollout status statefulset/yb-tserver -n yugabyte"

echo ""
echo "6. 🧪 Test Connectivity..."
echo "--------------------------"
echo "# Test database connectivity:"
echo "kubectl run -i --tty --rm debug --image=postgres:13 --restart=Never -- psql -h yb-tserver-service.yugabyte.svc.cluster.local -p 5433 -U vaultadmin -d mcp"

echo ""
echo "7. ⚡ Quick Fixes to Try First..."
echo "--------------------------------"
echo "Before redeploying YugabyteDB, try these steps:"
echo ""
echo "   1. Deploy the latest table-sync image with null pointer fixes:"
echo "      - Use table-sync:v2.5.0-versioned-logging or later"
echo "      - This includes the YugabyteDB CDC compatibility fixes"
echo ""
echo "   2. Clean up CDC streams using the application:"
echo "      - The app has built-in CDC cleanup on startup"
echo "      - Set CLEANUP_CDC_ON_STARTUP=true (default)"
echo ""
echo "   3. Restart YugabyteDB pods (not full redeploy):"
echo "      - This clears transient state issues"
echo "      - Much faster than full redeploy"
echo "      - Preserves all data"
echo ""
echo "   4. If still failing, check Debezium connector logs:"
echo "      - kubectl logs deployment/kafka-connect -n kafka"
echo "      - Look for specific YugabyteDB errors"

echo ""
echo "🎯 RECOMMENDATION:"
echo "=================="
echo "1. First try deploying the latest table-sync image with our fixes"
echo "2. If that doesn't work, restart YugabyteDB pods (not full redeploy)"
echo "3. Only do full redeploy as last resort if there's actual corruption"
echo ""
echo "Full redeploy should only be needed if:"
echo "  - YugabyteDB version upgrade required"
echo "  - Persistent data corruption detected"
echo "  - Configuration changes requiring cluster recreation"