# Dynamic Version System

This application now uses a **dynamic version detection system** that automatically determines the application version without hardcoded values.

## Version Detection Priority

The system checks for version information in the following order:

1. **Docker Build-time Git Tag** (highest priority)
   - Uses `GIT_TAG` environment variable set during Docker build
   - Most reliable for containerized deployments

2. **Runtime Git Detection**
   - Reads Git information directly from the repository
   - Works when running from source or when `.git` directory is available

3. **Docker Image Metadata**
   - Uses `DOCKER_IMAGE_TAG` environment variable
   - Fallback for container environments

4. **Environment Variables**
   - Uses `APP_VERSION` environment variable if set

5. **Git Commit Hash**
   - Uses `GIT_COMMIT` from Docker build args

6. **Timestamp Fallback** (lowest priority)
   - Generated timestamp when no other version info is available

## Building Images

### Using the Build Script (Recommended)
```bash
# Build with automatic version detection
./build.sh table-sync v2.5.0

# Build with latest tag
./build.sh table-sync latest

# Build with default name and latest tag
./build.sh
```

The build script automatically captures:
- Git tag (if on a tagged commit)
- Git commit hash
- Build timestamp
- Docker image tag

### Manual Docker Build
```bash
# Capture version info manually
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%d_%H:%M:%S_UTC")
GIT_COMMIT=$(git rev-parse --short HEAD)
GIT_TAG=$(git describe --tags --exact-match HEAD 2>/dev/null || echo "no-tag")

docker build \
    --build-arg BUILD_TIMESTAMP="${BUILD_TIMESTAMP}" \
    --build-arg GIT_COMMIT="${GIT_COMMIT}" \
    --build-arg GIT_TAG="${GIT_TAG}" \
    --build-arg DOCKER_IMAGE_TAG="table-sync:your-tag" \
    -t "table-sync:your-tag" \
    .
```

## Version Information at Runtime

The application logs comprehensive version information at startup:

```
🚀 Table Sync Application v2.5.0-dynamic-version starting up...
📊 Debezium Manager v2.5.0-dynamic-version initialized  
🔍 Version details: {
  "version": "v2.5.0-dynamic-version",
  "build_info": "docker-git-6241d06",
  "git_version": null,
  "commit_hash": null,
  "docker_version": "table-sync:v2.5.0-dynamic-version",
  "build_timestamp": "2025-09-30_23:45:36_UTC",
  "detection_method": "docker-git-6241d06",
  "docker_git_tag": "v2.5.0-dynamic-version",
  "docker_git_commit": "6241d06",
  "docker_build_timestamp": "2025-09-30_23:45:36_UTC",
  "is_container": true
}
```

## Deployment Scenarios

### Tagged Release
```bash
git tag v2.5.0
./build.sh table-sync v2.5.0
```
Result: Version = `v2.5.0`, Build Info = `docker-git-abc1234`

### Development Build
```bash
./build.sh table-sync dev
```
Result: Version = `commit-abc1234`, Build Info = `docker-commit`

### CI/CD Pipeline
```bash
# In your CI/CD pipeline
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%d_%H:%M:%S_UTC")
GIT_COMMIT=${GITHUB_SHA::7}
GIT_TAG=${GITHUB_REF_NAME}

docker build \
    --build-arg BUILD_TIMESTAMP="${BUILD_TIMESTAMP}" \
    --build-arg GIT_COMMIT="${GIT_COMMIT}" \
    --build-arg GIT_TAG="${GIT_TAG}" \
    --build-arg DOCKER_IMAGE_TAG="table-sync:${GIT_TAG}" \
    -t "table-sync:${GIT_TAG}" \
    .
```

## Environment Variable Override

You can still override the version at runtime if needed:
```bash
docker run -e APP_VERSION="custom-version" table-sync:latest
```

## Benefits

✅ **No more hardcoded versions** - No need to manually update version strings  
✅ **Automatic Git integration** - Versions reflect actual Git state  
✅ **Build-time capture** - Version info is baked into the image  
✅ **Multiple fallbacks** - Works in various deployment scenarios  
✅ **Detailed logging** - Complete version information for debugging  
✅ **CI/CD friendly** - Easy to integrate with automated pipelines  

## Migration from Hardcoded Versions

The old hardcoded version system:
```python
APP_VERSION = "v2.4.1-nullpointer-fix"  # ❌ Manual maintenance
BUILD_DATE = "2025-09-30"               # ❌ Manual maintenance
```

Is replaced with:
```python
from version_utils import APP_VERSION, BUILD_INFO, get_version_info  # ✅ Automatic
```

No changes needed in your deployment - the application will automatically detect and use the appropriate version information.