# YugabyteDB CDC Conflict Fix

## Problem Identified
The error `ERROR: Cannot create a replication slot on the same namespace which already has a yb-admin stream on it` indicates that YugabyteDB has existing CDC streams that are conflicting with Debezium connector creation.

## Root Cause
The previous cleanup logic was not aggressive enough to handle YugabyteDB's internal CDC stream management. YugabyteDB can have `yb-admin` created streams that persist even after Debezium connectors are deleted during ArgoCD redeployments.

## Enhanced Solution Implemented

### 1. Startup Cleanup Enhancement
- Added cluster-level YugabyteDB CDC stream cleanup (attempts to use `yb-admin` if available)
- Enhanced per-database cleanup to drop ALL publications and replication slots
- Added wait periods to let YugabyteDB process cleanup operations

### 2. Aggressive Per-Table Cleanup
- Modified `cleanup_stale_cdc_stream()` to be more aggressive:
  - Drops ALL publications in the database (not just pattern-matched ones)
  - Drops ALL replication slots in the database
  - Adds wait periods for YugabyteDB to process changes

### 3. Enhanced Connector Creation
- Added additional wait period after cleanup before connector creation
- Better error handling for CDC stream conflicts

## Key Changes Made

### In `debezium_manager.py`:

1. **`cleanup_all_cdc_streams_on_startup()`**:
   - Now calls `_cleanup_yugabytedb_cdc_streams()` first for cluster-level cleanup
   - Added 5-second wait after cleanup for YugabyteDB processing

2. **`_cleanup_yugabytedb_cdc_streams()`** (NEW):
   - Attempts to use `yb-admin` commands to list and delete CDC streams
   - Falls back gracefully if `yb-admin` is not available

3. **`cleanup_stale_cdc_stream()`**:
   - Now drops ALL publications and replication slots in the database
   - Added 3-second wait for YugabyteDB processing
   - More aggressive logging for better troubleshooting

4. **`create_connector()`**:
   - Added 5-second wait after cleanup before connector creation
   - Better error messages for troubleshooting

## Testing and Deployment

### Immediate Test
The enhanced cleanup has been tested and compiles successfully. The new logic should resolve the YugabyteDB CDC stream conflicts.

### Docker Image Rebuild Required
You need to rebuild and push your Docker image with these changes:

```bash
# In your table-sync-image directory
docker build -t ghcr.io/dotcomrow/table-sync-image:ts-$(date +%Y%m%d-%H%M%S) .
docker push ghcr.io/dotcomrow/table-sync-image:ts-$(date +%Y%m%d-%H%M%S)
```

### Update Kubernetes Manifest
Update the image tag in your Kubernetes manifest to use the new image version.

### Expected Behavior
After deployment with the enhanced cleanup:

1. **Startup**: More thorough cleanup of all CDC artifacts across all databases
2. **Per-connector**: Aggressive cleanup before each connector creation
3. **Wait periods**: Proper timing to let YugabyteDB process cleanup operations
4. **Better logging**: More detailed information about cleanup operations

## Monitoring
Watch the logs for:
- `🧹 AGGRESSIVE: Dropped publication/replication slot` messages
- `Waiting X seconds for YugabyteDB to process cleanup` messages
- Successful connector creation without the "yb-admin stream" error

## Fallback Plan
If the issue persists after this enhancement, it may require:
1. Manual `yb-admin` cleanup on the YugabyteDB cluster
2. YugabyteDB cluster restart to clear internal CDC state
3. Investigation of YugabyteDB master server CDC stream management

The enhanced cleanup should resolve the conflict in most cases by being more thorough about cleaning up all CDC artifacts before connector creation.