# CDC Stream Cleanup

This application now includes automatic cleanup of YugabyteDB CDC streams on startup to ensure clean deployments, especially useful for ArgoCD-managed applications.

## Automatic Startup Cleanup

By default, the application will clean up all CDC streams across all databases when it starts. This ensures:

- **Clean State**: No leftover CDC streams from previous deployments
- **ArgoCD Compatible**: Works with complete teardown/rebuild deployment patterns
- **Automatic Recovery**: Handles stale streams that might cause connector creation failures

### Configuration

Control the startup cleanup behavior with the environment variable:

```bash
# Enable cleanup on startup (default)
CLEANUP_CDC_ON_STARTUP=true

# Disable cleanup on startup
CLEANUP_CDC_ON_STARTUP=false
```

### What Gets Cleaned

The startup cleanup process removes:

1. **All Publications** across all databases
2. **All Replication Slots** across all databases
3. **System databases are skipped** (template0, template1, postgres, system_platform)

## Manual Cleanup

You can also run cleanup manually using the standalone script:

```bash
# Run manual cleanup
python cleanup_cdc_streams.py

# Show help
python cleanup_cdc_streams.py --help
```

### Use Cases for Manual Cleanup

- **Pre-deployment**: Clean state before deploying new version
- **Troubleshooting**: Clear problematic CDC streams
- **Maintenance**: Regular cleanup during maintenance windows

## Logging

The cleanup process provides detailed logging:

```
🧹 Starting cleanup of ALL CDC streams across all databases...
🧹 Cleaned 3 CDC streams/publications in database 'mcp'
🧹 Cleaned 1 CDC streams/publications in database 'keycloak'  
✅ Startup CDC cleanup completed: 4 streams/publications cleaned across 2 databases
```

## Safety Features

- **Error Tolerance**: Cleanup failures don't prevent application startup
- **Database Discovery**: Automatically finds all non-system databases
- **Graceful Handling**: Continues if individual cleanup operations fail
- **Detailed Logging**: Clear visibility into what was cleaned

## Integration with ArgoCD

This feature is specifically designed for ArgoCD deployment patterns where:

1. **Complete Teardown**: ArgoCD deletes all application resources
2. **Fresh Deployment**: New pods start with clean slate
3. **CDC Conflicts**: Old streams might persist in YugabyteDB
4. **Automatic Resolution**: Startup cleanup ensures clean state

The cleanup runs automatically on every pod start, ensuring consistent behavior across all deployments.