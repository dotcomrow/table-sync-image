# Comprehensive Database Scanning

## Overview

The Table Sync Orchestrator has been updated to support comprehensive scanning across all databases in the YugabyteDB cluster, not just the configured target database. This enables discovery and synchronization of annotated tables across the entire database ecosystem.

## Changes Made

### 1. Database Discovery Enhancement

**Before:**
- Only scanned the configured target database (typically "kafka")
- Limited scope to a single database

**After:**
- Scans ALL databases in the cluster by default
- Returns complete list of databases for processing
- Configurable behavior via `comprehensive_database_scan` setting

### 2. Configuration Options

Added new configuration in `config/orchestrator.yaml`:

```yaml
# Scanning Configuration
scan_interval_seconds: ${SCAN_INTERVAL_SECONDS:-30}
# Enable comprehensive scanning of all databases (vs single target database)
comprehensive_database_scan: ${COMPREHENSIVE_DATABASE_SCAN:-true}
# Databases to exclude from scanning (comma-separated list)
excluded_databases: ${EXCLUDED_DATABASES:-postgres,template0,template1}
# Maximum number of concurrent database scanning threads (0 = number of databases)
max_scan_threads: ${MAX_SCAN_THREADS:-0}
```

**Environment Variables:** 
- `COMPREHENSIVE_DATABASE_SCAN=true|false` (default: `true`)
- `EXCLUDED_DATABASES=database1,database2,database3` (default: `postgres,template0,template1`)
- `MAX_SCAN_THREADS=number` (default: `0` = one thread per database)

### 3. Enhanced Logging and Monitoring

#### Database-Level Metrics
- Total databases scanned
- Per-database scan timing
- Tables found per database
- Annotated tables per database

#### Performance Warnings
- Warns when scanning large numbers of databases (>5)
- Recommends tuning `scan_interval_seconds` for performance

#### Example Log Output
```json
{"event": "Starting comprehensive database scan", "database_count": 5, "databases": ["postgres", "yugabyte", "kafka", "analytics", "reporting"]}
{"event": "Database scan completed", "database": "kafka", "tables_found": 12, "annotated_tables": 3, "scan_time_seconds": 0.45}
{"event": "Comprehensive scan completed", "duration": 2.31, "databases_scanned": 5, "total_tables_found": 47, "annotated_tables_found": 8, "active_syncs": 3}
```

### 4. Scanning Scope

#### What Gets Scanned
- ✅ **ALL databases** in the cluster (when comprehensive_database_scan=true)
- ✅ **ALL user schemas** within each database (excludes system schemas)
- ✅ **ALL tables** within each schema
- ✅ **Table comments** for sync annotations

#### What Gets Excluded
- ❌ System databases with `datistemplate = true`
- ❌ **Configured excluded databases** (default: `postgres`, `template0`, `template1`)
- ❌ System schemas: `information_schema`, `pg_catalog`, `pg_toast`
- ❌ Tables without annotations or with `enabled: false`

## Performance Considerations

### Impact
- **Increased scan time:** Proportional to number of databases and tables
- **Higher resource usage:** More database connections and queries
- **Network overhead:** Additional cross-database communication

### Optimization Strategies

#### 1. Adjust Scan Interval
```yaml
# Increase interval for large environments
scan_interval_seconds: 60  # Default: 30
```

#### 2. Selective Scanning
```yaml
# Disable comprehensive scanning for specific environments
comprehensive_database_scan: false
```

#### 3. Database Exclusion
Exclude specific databases from scanning:
```yaml
# Exclude test, temporary, and system databases
excluded_databases: postgres,template0,template1,test_db,temp_analytics
```

## Migration Guide

### Existing Deployments
- **No breaking changes:** Comprehensive scanning is enabled by default
- **Backward compatible:** Single database scanning available via configuration
- **Gradual rollout:** Can be disabled during initial deployment

### Environment Variables
```bash
# Enable comprehensive scanning (default)
COMPREHENSIVE_DATABASE_SCAN=true

# Disable for performance-sensitive environments
COMPREHENSIVE_DATABASE_SCAN=false

# Exclude specific databases from scanning
EXCLUDED_DATABASES=postgres,template0,template1,test_db,temp_db

# Adjust scan frequency for large clusters
SCAN_INTERVAL_SECONDS=60
```

### Monitoring
Monitor these new metrics:
- `databases_scanned`: Number of databases processed per scan
- `total_tables_found`: Total tables discovered across all databases
- `annotated_tables_found`: Tables with valid sync annotations
- `scan_time_seconds`: Per-database scan duration

## Benefits

### 1. Complete Visibility
- Discovers tables across all databases automatically
- No need to configure multiple orchestrator instances
- Centralized management of all sync operations

### 2. Dynamic Discovery
- Automatically detects new databases
- Finds tables as they're created and annotated
- Responds to schema changes across the cluster

### 3. Operational Simplicity
- Single orchestrator instance manages entire cluster
- Unified logging and monitoring
- Consistent sync policies across databases

## Best Practices

### 1. Performance Tuning
- Start with longer scan intervals in large environments
- Monitor resource usage and adjust accordingly
- Consider database-specific exclusions for test/temp databases

### 2. Annotation Standards
- Use consistent annotation formats across databases
- Document annotation schemas for teams
- Implement validation for annotation syntax

### 3. Monitoring
- Set up alerts for scan duration increases
- Monitor failed database connections
- Track annotation compliance across databases

## Future Enhancements

### 1. Advanced Filtering
- Database name pattern matching
- Schema-level exclusions
- Table size thresholds

### 2. Parallel Scanning
- Concurrent database scanning
- Improved performance for large clusters
- Configurable concurrency limits

### 3. Incremental Discovery
- Change detection mechanisms
- Reduced full-scan frequency
- Event-driven discovery updates

## Troubleshooting

### High Scan Times
1. Check database count: Look for unexpected databases
2. Increase scan interval: Reduce scanning frequency
3. Disable comprehensive scanning: Fall back to single database
4. Review table counts: Identify databases with many tables

### Missing Tables
1. Verify database visibility: Check user permissions
2. Confirm annotation format: Validate JSON syntax
3. Check schema exclusions: Ensure tables are in user schemas
4. Review enabled flags: Confirm `enabled: true` in annotations

### Resource Usage
1. Monitor connection pools: Watch for connection exhaustion
2. Check memory usage: Large result sets can consume memory
3. Network monitoring: Verify cluster connectivity
4. Database load: Monitor impact on source databases