# Table Sync Application - Version Changelog

## v2.4.0-simplified-config (2025-09-30)

### 🚀 Major Fixes
- **FIXED**: Removed duplicate `table.whitelist` parameter causing connector creation failures
- **FIXED**: Eliminated CDC stream cross-database conflicts by implementing auto-creation per connector
- **IMPROVED**: Simplified Debezium connector configuration to reduce parameter conflicts

### 🔧 Configuration Changes
- Removed deprecated `table.whitelist` parameter (was conflicting with `table.include.list`)
- Removed potentially conflicting `schema.include.list` parameter
- Removed unnecessary filtering parameters (`log.mining.filter.enabled`, `table.exclude.list`)
- Simplified to use only `table.include.list` for table filtering

### 📊 Monitoring Improvements
- **NEW**: Added version stamps to application startup logs
- **NEW**: Application version prominently displayed: `Version: v2.4.0-simplified-config (Built: 2025-09-30)`
- **NEW**: Debezium Manager version tracking for easier debugging

### 🏗️ Technical Changes
- Disabled CDC stream ID reuse to prevent cross-database conflicts (temporary measure)
- Each Debezium connector now auto-creates its own CDC stream
- Maintained table-level granular control architecture

### 🐛 Issues Resolved
- ❌ `"table.whitelist" is already specified` errors
- ❌ `The table kafka.public.table_sync_state is not a part of the stream ID` errors
- ❌ HTTP 400 connector creation failures
- ❌ Cross-database CDC stream validation conflicts

### 📦 Docker Images
- `table-sync:v2.4.0-simplified-config` - Versioned release
- `table-sync:latest` - Latest stable release

### 🚀 Deployment Notes
To deploy this version:
1. Update Kubernetes deployment to use `table-sync:v2.4.0-simplified-config`
2. Monitor logs for the version stamp to confirm deployment
3. Verify connector creation succeeds without configuration errors
4. Confirm data streaming is working as expected

### 🔍 How to Verify Version in Production
Look for this line in the startup logs:
```
Version: v2.4.0-simplified-config (Built: 2025-09-30)
```

And this line when Debezium manager initializes:
```
Debezium Manager v2.4.0-simplified-config - connecting to YugabyteDB...
```