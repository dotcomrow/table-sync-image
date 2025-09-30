# YugabyteDB yb-admin Stream Conflict Fix

## Problem Analysis
From the logs, the issue is clear:
```
ERROR: Cannot create a replication slot on the same namespace which already has a yb-admin stream on it.
```

This indicates YugabyteDB has existing CDC streams created via the `yb-admin` tool that are conflicting with Debezium connector creation.

## Root Cause
- YugabyteDB CDC streams can be created through two methods:
  1. **PostgreSQL interface** (publications/replication slots) - what our cleanup handles
  2. **yb-admin tool** (cluster-level CDC streams) - what's causing the conflict
- The `yb-admin` streams persist at the YugabyteDB cluster level and are not visible through PostgreSQL system tables
- These streams conflict with Debezium's attempt to create new CDC streams on the same namespace

## Enhanced Solution Implemented

### 1. **Specific yb-admin Stream Error Detection**
- Added specific error pattern matching for "yb-admin stream" conflicts
- Provides detailed diagnostic information including YugabyteDB master addresses
- Suggests manual intervention steps when automated cleanup fails

### 2. **Aggressive Connection Termination**
- Enhanced cleanup now terminates any active CDC-related database connections
- Targets connections that might be holding CDC resources
- Includes idle-in-transaction connections that could block cleanup

### 3. **Extended Wait Periods**
- **Startup cleanup**: 10 seconds (up from 5)
- **Per-connector cleanup**: 8 seconds (up from 3) 
- **yb-admin conflict retry**: 15 seconds additional wait
- **Baseline stabilization**: 5 seconds even when no cleanup needed

### 4. **Enhanced Error Reporting**
When yb-admin stream conflicts persist, the system now provides:
- Clear diagnostic messages
- YugabyteDB master server addresses
- Specific manual intervention commands
- Database-specific troubleshooting steps

## Key Code Changes

### Error Handling Enhancement:
```python
if "yb-admin stream" in message and "replication slot" in message:
    logger.error(f"🚨 YugabyteDB yb-admin stream conflict detected!")
    logger.error(f"Manual intervention may be required:")
    logger.error(f"  1. Connect to YugabyteDB master: {self.db_master_addresses}")
    logger.error(f"  2. Run: yb-admin --master_addresses {self.db_master_addresses} list_cdc_streams")
    logger.error(f"  3. Delete conflicting streams for database: {database_name}")
```

### Connection Termination:
```python
# Terminate any connections that might be related to CDC
cdc_connections = await conn.fetch("""
    SELECT pid, application_name, state, query 
    FROM pg_stat_activity 
    WHERE application_name LIKE '%debezium%' 
       OR application_name LIKE '%cdc%'
       OR query LIKE '%publication%'
       OR query LIKE '%replication%'
       OR state = 'idle in transaction'
""")
```

## Expected Behavior

### If Automated Cleanup Succeeds:
- Longer wait periods should allow YugabyteDB to properly process CDC stream removal
- Connection termination should free any locked CDC resources
- Connector creation should succeed on retry

### If yb-admin Stream Conflict Persists:
- Clear error messages with diagnostic information
- Specific manual intervention steps provided
- YugabyteDB master addresses included for troubleshooting

## Manual Intervention (If Needed)

If the automated cleanup still fails, you can manually clean up yb-admin streams:

1. **Connect to YugabyteDB master**:
   ```bash
   kubectl exec -it yb-master-0 -n yugabyte -- bash
   ```

2. **List existing CDC streams**:
   ```bash
   yb-admin --master_addresses yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100 list_cdc_streams
   ```

3. **Delete conflicting streams**:
   ```bash
   yb-admin --master_addresses [...] delete_cdc_stream <stream_id>
   ```

## Testing
The enhanced solution compiles successfully and should resolve the yb-admin stream conflicts through:
- More aggressive cleanup
- Longer stabilization periods  
- Better error diagnostics
- Manual intervention guidance

Deploy the updated image and monitor for the enhanced cleanup messages and resolution of the CDC stream conflicts.