# Automated YB-Admin Stream Cleanup Solution

## Problem Solved
The issue was that YugabyteDB CDC streams created by `yb-admin` were conflicting with Debezium's attempt to create replication slots, causing HTTP 500 errors with the message:
```
ERROR: Cannot create a replication slot on the same namespace which already has a yb-admin stream on it. : INVALID_REQUEST
```

## Solution Implemented

### 1. Enhanced Error Detection
- **HTTP 500 Error Parsing**: The code now specifically detects "yb-admin stream" conflicts in HTTP 500 responses
- **Immediate Detection**: Instead of waiting 60+ seconds for timeouts, conflicts are detected immediately when Kafka Connect validates the connector

### 2. Automated Cleanup Functions

#### `_cleanup_yb_admin_streams(database_name)` 
- **Purpose**: Clean up yb-admin streams for a specific database when conflicts are detected
- **Process**:
  1. Lists all CDC streams using `yb-admin list_cdc_streams`
  2. Identifies streams related to the target database
  3. If no specific matches found, cleans up ALL streams as last resort
  4. Deletes each stream using `yb-admin delete_cdc_stream <stream_id>`

#### `_cleanup_all_yb_admin_streams()`
- **Purpose**: Proactive cleanup of ALL yb-admin streams at startup
- **Process**:
  1. Called during startup to prevent conflicts before they occur
  2. Lists and deletes ALL existing yb-admin CDC streams
  3. Gracefully handles missing `yb-admin` command (for development environments)

### 3. Integration Points

#### Startup Cleanup
```python
# Called in cleanup_all_cdc_streams_on_startup()
yb_admin_cleanup = await self._cleanup_all_yb_admin_streams()
```

#### HTTP 500 Error Handling
```python
if "yb-admin stream" in response_text.lower():
    logger.error(f"🚨 YugabyteDB yb-admin stream conflict detected!")
    cleanup_success = await self._cleanup_yb_admin_streams(database_name)
```

#### JSON Error Response Handling  
```python
if "yb-admin stream" in message and "replication slot" in message:
    cleanup_success = await self._cleanup_yb_admin_streams(database_name)
```

## Expected Behavior

### Before This Fix
1. Connector creation takes 60+ seconds
2. Eventually fails with HTTP 500 error
3. Manual `yb-admin` commands required
4. Process repeats on next attempt

### After This Fix
1. **Startup**: Proactively cleans all yb-admin streams
2. **Conflict Detection**: Immediate detection via HTTP 500 parsing
3. **Automated Resolution**: Runs `yb-admin` commands automatically
4. **Retry Logic**: Waits 10 seconds after cleanup, then retries connector creation
5. **Fallback**: Manual intervention guidance if automated cleanup fails

## Logging Enhancements

### Successful Cleanup
```
🧹 Attempting automated yb-admin stream cleanup for database: mcp
📋 Current CDC streams: [stream details]
🎯 Found stream to delete: abc123...
✅ Successfully deleted CDC stream: abc123...
✅ Automated yb-admin stream cleanup completed, retrying connector creation...
```

### Fallback to Manual
```
❌ PERSISTENT YB-ADMIN STREAM CONFLICT
Attempting final cleanup before giving up...
Manual intervention may still be required:
  1. Connect to YugabyteDB master: yb-tserver:7100
  2. Run: yb-admin --master_addresses yb-tserver:7100 list_cdc_streams
  3. Delete conflicting streams for database: mcp
```

## Benefits

1. **Automatic Resolution**: No manual intervention required in most cases
2. **Faster Detection**: Immediate error detection instead of 60+ second timeouts  
3. **Proactive Prevention**: Startup cleanup prevents issues before they occur
4. **Graceful Degradation**: Falls back to manual instructions if automated cleanup fails
5. **Environment Flexibility**: Handles missing `yb-admin` command in development environments

## Testing

Deploy the updated image and monitor logs for:
- Startup yb-admin cleanup messages
- Immediate HTTP 500 error detection  
- Automated stream deletion success
- Successful connector creation after cleanup

The solution should eliminate the yb-admin stream conflicts and allow Debezium connectors to be created successfully.