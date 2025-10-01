# Deployment Instructions: v2.4.1-nullpointer-fix

## Issue Summary
The YugabyteDB Debezium connector was experiencing a critical NullPointerException during task configuration:

```
java.lang.NullPointerException
    at io.debezium.connector.yugabytedb.YBClientUtils.isBeforeImageEnabled(YBClientUtils.java:336)
    at io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector.taskConfigs(YugabyteDBgRPCConnector.java:167)
```

This was preventing the connector from properly configuring tasks, meaning no data would stream to Kafka despite the connector being successfully created.

## Fix Applied
Added the following configuration parameters to work around YugabyteDB CDC metadata limitations:

```json
{
    "provide.transaction.metadata": "false",
    "binary.handling.mode": "base64", 
    "publication.autocreate.mode": "disabled",
    "database.stream.prefix": "${database_name}_${schema_name}_${table_name}"
}
```

## Deployment Steps

### 1. Build and Deploy Image
```bash
# Already built:
docker build -t table-sync:v2.4.1-nullpointer-fix .
docker tag table-sync:v2.4.1-nullpointer-fix table-sync:latest
```

### 2. Update Kubernetes Deployment
Update your deployment YAML to use the new image:
```yaml
spec:
  template:
    spec:
      containers:
      - name: table-sync
        image: table-sync:v2.4.1-nullpointer-fix
```

### 3. Apply Deployment
```bash
kubectl apply -f your-deployment.yaml
kubectl rollout restart deployment/table-sync
```

## Verification Steps

### 1. Check Version in Logs
Look for the new version stamp in startup logs:
```
🚀 Table Sync Application v2.4.1-nullpointer-fix (Built: 2025-09-30) starting up...
📊 Debezium Manager v2.4.1-nullpointer-fix initialized
```

### 2. Monitor Connector Creation
The connector should now:
- ✅ Create successfully (HTTP 201)
- ✅ Configure tasks without NullPointerException
- ✅ Start streaming data to Kafka topics
- ✅ Show RUNNING status

### 3. Check Kafka Topics
Verify data is flowing:
- Topics should be created with naming pattern: `yugabyte-{database}-{schema}.{schema}.{table}`
- Messages should appear in topics after connector starts

## Expected Behavior
- **Before**: Connector created but failed with NullPointerException during task configuration
- **After**: Connector creates and successfully starts streaming data

## Rollback Plan
If issues occur, rollback to previous version:
```bash
kubectl set image deployment/table-sync table-sync=table-sync:v2.4.0-simplified-config
```

## Next Steps
Monitor the connector logs and Kafka topic data flow to ensure the fix is working correctly.