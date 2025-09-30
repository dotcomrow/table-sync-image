#!/usr/bin/env python3
"""
Standalone script to clean up all CDC streams in YugabyteDB
This can be run manually or as part of deployment scripts
"""

import os
import sys
import asyncio
import logging

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from debezium_manager import DebeziumConnectorManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def main():
    """Main cleanup function"""
    logger.info("🧹 Starting manual CDC stream cleanup...")
    
    # Get configuration from environment
    debezium_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
    
    try:
        # Initialize the connector manager
        manager = DebeziumConnectorManager(debezium_url)
        
        # Perform the cleanup
        success = await manager.cleanup_all_cdc_streams_on_startup()
        
        if success:
            logger.info("✅ Manual CDC cleanup completed successfully")
            return 0
        else:
            logger.error("❌ CDC cleanup encountered errors")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Failed to perform CDC cleanup: {e}")
        return 1

if __name__ == "__main__":
    # Check for help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("Usage: python cleanup_cdc_streams.py")
        print("Environment variables:")
        print("  DATABASE_URL - YugabyteDB connection string")
        print("  DEBEZIUM_CONNECTOR_URL - Debezium connector URL (optional)")
        sys.exit(0)
    
    # Run the cleanup
    exit_code = asyncio.run(main())
    sys.exit(exit_code)