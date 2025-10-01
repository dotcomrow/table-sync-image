"""
Centralized logging configuration with version information
"""
import os
from loguru import logger
from version_utils import APP_VERSION, BUILD_INFO

def configure_logging():
    """Configure logger with version information"""
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_VERSION_VERBOSE = os.getenv("LOG_VERSION_VERBOSE", "false").lower() == "true"
    
    # Determine version format for logs
    if LOG_VERSION_VERBOSE:
        # Full version but keep it reasonable
        if BUILD_INFO.startswith('runtime-git-') or BUILD_INFO.startswith('docker-git-'):
            commit_hash = BUILD_INFO.split('-')[-1][:7]  # Just first 7 chars of commit
            version_display = f"{APP_VERSION}@{commit_hash}"
        else:
            version_display = f"{APP_VERSION}"
    else:
        # Create a shorter version for logs (remove prefixes, take last part)
        short_version = APP_VERSION
        if short_version.startswith('v'):
            short_version = short_version[1:]  # Remove 'v' prefix
        if '-' in short_version:
            # For versions like "2.5.0-dynamic-version", just take the version number
            parts = short_version.split('-')
            if len(parts) > 1 and parts[0].replace('.', '').isdigit():
                short_version = parts[0]  # Just take version number part
        version_display = f"v{short_version}"
    
    # Remove default logger
    logger.remove()
    
    # Add logger with version information
    logger.add(
        lambda msg: print(msg, end=""),
        level=LOG_LEVEL,
        format=f"<green>{{time:YYYY-MM-DD HH:mm:ss}}</green> | <yellow>{version_display}</yellow> | <level>{{level: <8}}</level> | <cyan>{{name}}</cyan>:<cyan>{{function}}</cyan>:<cyan>{{line}}</cyan> - <level>{{message}}</level>"
    )
    
    return logger

def get_logger_with_version():
    """Get a logger instance with version information pre-configured"""
    return logger

# Configure logging on import
configure_logging()