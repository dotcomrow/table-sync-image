# Table Sync Application - Version Changelog

## v2.5.0-versioned-logging (2025-09-30)

### 🚀 Major Enhancement: Versioned Logging System
- **NEW**: Version information automatically included in every log line
- **NEW**: Compact mode shows clean version (e.g., `v2.5.0`)
- **NEW**: Verbose mode shows full version with commit hash (e.g., `v2.5.0-dynamic-version@6241d06`)
- **NEW**: `LOG_VERSION_VERBOSE` environment variable for detailed version logging

### 🔧 Logging Improvements
- **NEW**: Centralized logging configuration in `logging_config.py`
- **NEW**: Automatic version detection in all log messages
- **NEW**: Production-friendly compact version display
- **NEW**: Development-friendly verbose version display with commit hashes

### 📊 Operational Benefits
- **IMPROVED**: Easy version tracking in production logs
- **IMPROVED**: Quick identification of version-specific issues
- **IMPROVED**: Better deployment verification through log analysis
- **IMPROVED**: Multi-version environment debugging capabilities

### 🐛 Issues Resolved
- ❌ Difficulty correlating logs with deployed versions
- ❌ Manual version identification in production environments
- ❌ Lack of version traceability in log analysis
- ❌ Confusion between different deployment versions

---

## v2.5.0-dynamic-version (2025-09-30)

### 🚀 Major Enhancement: Dynamic Version System
- **NEW**: Completely replaced hardcoded version strings with dynamic detection
- **NEW**: Version automatically detected from Git tags, commits, and Docker build metadata
- **NEW**: Multi-tier version detection with intelligent fallbacks
- **NEW**: Build script with automatic version capture (`./build.sh`)

### 🔧 Version Detection Methods
1. **Docker Build-time Git Tag** (highest priority) - Most reliable for containers
2. **Runtime Git Detection** - Works when running from source
3. **Docker Image Metadata** - Container environment fallback
4. **Environment Variables** - Manual override capability
5. **Git Commit Hash** - Development builds
6. **Timestamp Fallback** - Last resort

### 📦 Build System Improvements
- **NEW**: `./build.sh` script with automatic version capture
- **NEW**: Docker build arguments for version information
- **NEW**: Comprehensive version logging at application startup
- **IMPROVED**: No more manual version maintenance required

### 🐛 Issues Resolved
- ❌ Manual version string maintenance
- ❌ Version mismatches between deployments
- ❌ Hardcoded build dates requiring updates
- ❌ Lack of Git integration in version tracking

---

## v2.4.1-nullpointer-fix (2025-09-30)

### 🚀 Critical Fix
- **FIXED**: YugabyteDB Debezium connector NullPointerException during task configuration
- **FIXED**: Connector task reconfiguration failures preventing data streaming

### 🔧 Configuration Changes
- Added `provide.transaction.metadata: false` to disable problematic transaction metadata checks
- Added `binary.handling.mode: base64` for proper binary data handling
- Added `publication.autocreate.mode: disabled` to prevent publication conflicts
- Added `database.stream.prefix` for better stream naming isolation

### 🐛 Issues Resolved
- ❌ `java.lang.NullPointerException at io.debezium.connector.yugabytedb.YBClientUtils.isBeforeImageEnabled`
- ❌ Connector creation succeeding but task configuration failing
- ❌ No data streaming to Kafka topics despite successful connector creation

---

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