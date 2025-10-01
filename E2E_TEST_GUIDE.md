#!/bin/bash
# End-to-End CDC Pipeline Test Deployment Guide
echo "🚀 End-to-End CDC Pipeline Test Deployment Guide"
echo "================================================="

echo "
📦 CURRENT IMAGE: 
   Commit: da9457c (includes E2E testing framework)
   Previous: 8a7d34d (bash implementation approach)

🧪 TESTING OPTIONS:

1. 🎯 FULL END-TO-END TEST (Recommended):
   Set environment variable: E2E_TEST_MODE=true
   This tests: YugabyteDB → Debezium → Kafka → BigQuery
   
   kubectl set env deployment/table-sync E2E_TEST_MODE=true
   
   The app will:
   ✅ Create test table in YugabyteDB 
   ✅ Create test table in BigQuery
   ✅ Set up Debezium connector with transforms
   ✅ Add/update/delete test data
   ✅ Verify data appears in BigQuery
   ✅ Exit after test (no normal app startup)

2. 🔧 MINIMAL CONNECTOR TEST:
   Set environment variable: CDC_TEST_MODE=true
   This tests only connector creation capability
   
   kubectl set env deployment/table-sync CDC_TEST_MODE=true

🌍 REQUIRED ENVIRONMENT VARIABLES:

# Basic (for all tests)
DATABASE_URL=postgresql://yugabyte@yb-tserver-service.yugabyte.svc.cluster.local:5433/yugabyte
DEBEZIUM_CONNECTOR_URL=http://kafka-connect.kafka.svc.internal.lan:8083
YUGABYTE_MASTER_ADDRESSES=yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100

# Additional for E2E test
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=cdc_test_dataset  
GOOGLE_APPLICATION_CREDENTIALS=/app/service-account.json

📋 DEPLOYMENT STEPS:

1. Update image to latest:
   kubectl set image deployment/table-sync table-sync=dotcomrow/table-sync-image:da9457c

2. Set test mode:
   kubectl set env deployment/table-sync E2E_TEST_MODE=true

3. Check logs:
   kubectl logs -f deployment/table-sync

4. Expected outcomes:
   ✅ SUCCESS: 'End-to-end test PASSED - Complete CDC pipeline is working!'
   ❌ FAILURE: Check specific failure point in logs

🔄 FALLBACK OPTIONS:

If E2E test fails:
1. Try YugabyteDB redeploy: kubectl delete namespace yugabyte
2. Run individual test components in pod
3. Try PostgreSQL connector fallback  
4. Check component-specific issues

🧩 INDIVIDUAL COMPONENT TESTS:
If you need to debug specific components, run these in the pod:

kubectl exec -it deployment/table-sync -- python test_components/01_yugabyte_connection_test.py
kubectl exec -it deployment/table-sync -- python test_components/02_kafka_connect_test.py  
kubectl exec -it deployment/table-sync -- python test_components/03_minimal_connector_test.py
kubectl exec -it deployment/table-sync -- python test_components/e2e_end_to_end_test.py

💡 SUCCESS CRITERIA:
If the E2E test passes, you'll have confidence that:
- YugabyteDB CDC works
- Debezium connector works with simplified config  
- Kafka message routing works
- BigQuery ingestion works
- The entire pipeline is functional

This gives you a solid foundation to proceed with your actual table sync implementation!
"

echo "Ready to test! 🚀"