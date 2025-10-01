"""
Dynamic version detection utility
Detects application version from multiple sources in order of preference:
1. Git commit hash and tag
2. Docker image metadata 
3. Environment variables
4. Build timestamp fallback
"""
import os
import subprocess
import datetime
import json
from typing import Optional, Tuple

def get_git_version() -> Tuple[Optional[str], Optional[str]]:
    """Get version from Git repository"""
    try:
        # Get current commit hash (short)
        commit_hash = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'], 
            stderr=subprocess.DEVNULL,
            text=True
        ).strip()
        
        # Try to get the current tag
        try:
            tag = subprocess.check_output(
                ['git', 'describe', '--tags', '--exact-match', 'HEAD'],
                stderr=subprocess.DEVNULL,
                text=True
            ).strip()
            return tag, commit_hash
        except subprocess.CalledProcessError:
            # No exact tag match, try to get the latest tag
            try:
                latest_tag = subprocess.check_output(
                    ['git', 'describe', '--tags', '--abbrev=0'],
                    stderr=subprocess.DEVNULL,
                    text=True
                ).strip()
                return f"{latest_tag}-{commit_hash}", commit_hash
            except subprocess.CalledProcessError:
                return f"git-{commit_hash}", commit_hash
                
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None, None

def get_docker_image_version() -> Optional[str]:
    """Get version from Docker image metadata if running in container"""
    try:
        # Check build-time Docker arguments first
        docker_tag = os.getenv('DOCKER_IMAGE_TAG')
        if docker_tag:
            return docker_tag
            
        # Check if we're running in a container
        if os.path.exists('/.dockerenv'):
            # Try to read from container metadata if available
            try:
                with open('/proc/1/environ', 'rb') as f:
                    environ = f.read().decode('utf-8', errors='ignore')
                    # Look for common image-related environment variables
                    for line in environ.split('\x00'):
                        if 'IMAGE=' in line or 'TAG=' in line:
                            return line.split('=', 1)[1]
            except (FileNotFoundError, PermissionError):
                pass
                
    except Exception:
        pass
    return None

def get_build_timestamp() -> str:
    """Get build timestamp as fallback"""
    # Try Docker build timestamp first
    build_time = os.getenv('BUILD_TIMESTAMP')
    if build_time:
        return build_time
        
    # Use current timestamp as last resort
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H:%M:%S_UTC')

def detect_version() -> Tuple[str, str]:
    """
    Detect application version dynamically
    Returns: (version, build_info)
    """
    # Try Docker build-time Git tag first (most reliable in container)
    git_tag = os.getenv('GIT_TAG')
    git_commit = os.getenv('GIT_COMMIT')
    if git_tag and git_tag != 'no-tag':
        build_info = f"docker-git-{git_commit}" if git_commit else "docker-git"
        return git_tag, build_info
    
    # Try runtime Git detection
    git_version, commit_hash = get_git_version()
    if git_version:
        build_info = f"runtime-git-{commit_hash}" if commit_hash else "runtime-git"
        return git_version, build_info
    
    # Try Docker image metadata
    docker_version = get_docker_image_version()
    if docker_version:
        return docker_version, "docker-image"
    
    # Try environment variables
    env_version = os.getenv('APP_VERSION')
    if env_version:
        return env_version, "env-var"
    
    # Use Docker build commit if available
    if git_commit and git_commit != 'unknown':
        return f"commit-{git_commit}", "docker-commit"
    
    # Fallback to timestamp
    timestamp = get_build_timestamp()
    return f"dev-{timestamp}", "timestamp-fallback"

def get_version_info() -> dict:
    """Get comprehensive version information"""
    version, build_info = detect_version()
    git_version, commit_hash = get_git_version()
    docker_version = get_docker_image_version()
    
    return {
        "version": version,
        "build_info": build_info,
        "git_version": git_version,
        "commit_hash": commit_hash,
        "docker_version": docker_version,
        "build_timestamp": get_build_timestamp(),
        "detection_method": build_info,
        # Docker build-time information
        "docker_git_tag": os.getenv('GIT_TAG'),
        "docker_git_commit": os.getenv('GIT_COMMIT'),
        "docker_build_timestamp": os.getenv('BUILD_TIMESTAMP'),
        "is_container": os.path.exists('/.dockerenv')
    }

# Main version detection
APP_VERSION, BUILD_INFO = detect_version()