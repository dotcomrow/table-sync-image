# Versioned Logging System

The application now includes **version information in every log line**, making it easy to track which version is generating each log entry.

## Log Format

### Default (Compact) Mode
```
2025-09-30 19:56:23 | v2.5.0 | INFO     | __main__:<module>:7 - Testing logger with version information
```

### Verbose Mode
```
2025-09-30 19:57:31 | v2.5.0-dynamic-version@6241d06 | INFO | __main__:<module>:4 - Testing verbose version logging
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Standard log level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_VERSION_VERBOSE` | `false` | Show full version with commit hash |

### Examples

**Compact Version (Default):**
```bash
# Shows just the version number
docker run -e LOG_LEVEL=INFO your-app
# Output: 2025-09-30 19:56:23 | v2.5.0 | INFO | ...
```

**Verbose Version:**
```bash
# Shows full version with commit hash
docker run -e LOG_VERSION_VERBOSE=true your-app
# Output: 2025-09-30 19:57:31 | v2.5.0-dynamic-version@6241d06 | INFO | ...
```

## Version Display Logic

### Compact Mode (`LOG_VERSION_VERBOSE=false`)
- Removes the `v` prefix if present
- For tagged versions like `v2.5.0-dynamic-version`, shows just `v2.5.0`
- For commit versions like `commit-abc1234`, shows the commit hash
- Clean, readable format for production logs

### Verbose Mode (`LOG_VERSION_VERBOSE=true`)
- Shows full version tag with commit hash
- Format: `version@commit` (e.g., `v2.5.0-dynamic-version@6241d06`)
- Useful for development and debugging
- Provides complete version traceability

## Benefits

✅ **Version Traceability** - Every log line shows which version generated it  
✅ **Easy Debugging** - Quickly identify version-specific issues  
✅ **Deployment Verification** - Confirm the correct version is running  
✅ **Multi-Version Environments** - Distinguish between different deployments  
✅ **Historical Analysis** - Track issues across version deployments  

## Implementation Details

The versioned logging system:

1. **Centralized Configuration** - Single `logging_config.py` module
2. **Dynamic Version Detection** - Uses the same version system as the application
3. **Automatic Import** - All modules get versioned logging automatically
4. **Performance Optimized** - Version string calculated once at startup
5. **Environment Aware** - Different formats for different deployment needs

## Usage in Code

```python
# Import the configured logger (automatic version info)
from logging_config import logger

# All log statements now include version automatically
logger.info("Application starting up")
logger.warning("Configuration issue detected")
logger.error("Database connection failed")
```

## Kubernetes Deployment

For Kubernetes deployments, you can control logging verbosity:

```yaml
spec:
  containers:
  - name: table-sync
    image: ghcr.io/dotcomrow/table-sync-image:latest
    env:
    - name: LOG_LEVEL
      value: "INFO"
    - name: LOG_VERSION_VERBOSE  # Optional: show full version details
      value: "false"
```

## Production Recommendations

### Standard Production
```yaml
env:
- name: LOG_LEVEL
  value: "INFO"
- name: LOG_VERSION_VERBOSE
  value: "false"  # Compact format
```

### Development/Debugging
```yaml
env:
- name: LOG_LEVEL
  value: "DEBUG"
- name: LOG_VERSION_VERBOSE
  value: "true"   # Full version details
```

## Log Analysis

With versioned logging, you can easily:

```bash
# Filter logs by version
kubectl logs deployment/table-sync | grep "v2.5.0"

# Find logs from specific commit
kubectl logs deployment/table-sync | grep "@6241d06"

# Compare behavior across versions
kubectl logs deployment/table-sync --since=1h | grep -E "(v2.4.1|v2.5.0)"
```

This makes it much easier to correlate logs with specific code versions and track down version-specific issues.