# Component Testing Suite

This directory contains modular test components that can isolate and diagnose CDC issues independently.

## Test Components

### 1. YugabyteDB Connection Test (`01_yugabyte_connection_test.py`)
- Tests basic YugabyteDB connectivity
- Verifies database access and replication slot status
- **Run if**: Basic connectivity issues suspected

### 2. Kafka Connect Service Test (`02_kafka_connect_test.py`) 
- Tests Kafka Connect service health
- Verifies YugabyteDB connector plugins are available
- Lists existing connectors
- **Run if**: Kafka Connect service issues suspected

### 3. Minimal Connector Test (`03_minimal_connector_test.py`)
- Creates absolute minimal YugabyteDB connector
- Uses only essential configuration parameters
- Isolates before image NullPointerException issues
- **Run if**: Connector creation fails

### 4. PostgreSQL Fallback Test (`04_postgresql_fallback_test.py`)
- Tests using standard PostgreSQL connector with YugabyteDB
- Alternative approach if YugabyteDB-specific connector fails
- **Run if**: YugabyteDB connector consistently fails

### 5. End-to-End Pipeline Test (`e2e_end_to_end_test.py`)
- **COMPLETE PIPELINE TEST**: YugabyteDB → Debezium → Kafka → BigQuery
- Creates test table in both YugabyteDB and BigQuery
- Sets up Debezium connector with transforms
- Adds/updates/deletes data and verifies it appears in BigQuery
- **Run if**: You want to test the entire CDC flow works end-to-end

## Usage

### Run Individual Tests
```bash
# Test YugabyteDB connection
python test_components/01_yugabyte_connection_test.py

# Test Kafka Connect
python test_components/02_kafka_connect_test.py

# Test minimal connector
python test_components/03_minimal_connector_test.py

# Test PostgreSQL fallback
python test_components/04_postgresql_fallback_test.py

# Test complete end-to-end pipeline
python test_components/e2e_end_to_end_test.py
```

### Run All Tests
```bash
python test_components/run_tests.py
```

### Run E2E Test via App (Recommended)
```bash
# Set environment variable and run main app
E2E_TEST_MODE=true python src/app.py
```

## Environment Variables Required

```bash
# Required for all tests
DATABASE_URL=postgresql://yugabyte@yb-tserver-service.yugabyte.svc.cluster.local:5433/yugabyte
DEBEZIUM_CONNECTOR_URL=http://kafka-connect.kafka.svc.internal.lan:8083
YUGABYTE_MASTER_ADDRESSES=yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100

# Additional for E2E test
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=cdc_test_dataset
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

## Troubleshooting Strategy

1. **Start with Connection Test**: Verify basic YugabyteDB access
2. **Check Kafka Connect**: Ensure service is healthy and has YugabyteDB plugins
3. **Try Minimal Connector**: Test with absolute minimal configuration
4. **Fallback Option**: Try PostgreSQL connector if YugabyteDB connector fails
5. **YugabyteDB Redeploy**: If all tests fail, consider fresh YugabyteDB deployment

## Expected Outcomes

- **All Pass**: Current fix should work
- **Connection Fails**: YugabyteDB deployment issue
- **Kafka Connect Fails**: Service or plugin issue  
- **Minimal Connector Fails**: Core YugabyteDB connector bug (need redeploy or alternative)
- **PostgreSQL Works**: Use PostgreSQL connector as workaround