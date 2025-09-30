#!/usr/bin/env python3
"""
Test script to verify CDC detection improvements
"""
import asyncio
import os
import sys
sys.path.append('/Users/christopherlyons/GitHub/table-sync-image/src')

from debezium_manager import DebeziumConnectorManager

async def test_cdc_detection():
    """Test the improved CDC detection functionality"""
    print("Testing CDC detection functionality...")
    
    # Mock the environment variables
    os.environ["DATABASE_URL"] = "postgresql://yugabyte@localhost:5433/yugabyte"
    
    # Create DebeziumConnectorManager instance (use a mock URL)
    manager = DebeziumConnectorManager("http://localhost:8083")
    
    # Test with a hypothetical table that might have CDC
    database_name = "mcp"
    schema_name = "mcp_openapi_ro"
    table_name = "mcp_openapi_augmentations"
    
    print(f"Checking CDC status for {database_name}.{schema_name}.{table_name}")
    
    try:
        # This will show us the improved logging and detection logic
        has_cdc = await manager.check_cdc_stream_exists(database_name, schema_name, table_name)
        print(f"CDC detection result: {has_cdc}")
        
        if has_cdc:
            print("✅ CDC stream detected - data copy should be skipped")
        else:
            print("⚠️  No CDC stream detected - data copy would proceed")
            
    except Exception as e:
        print(f"❌ Error during CDC detection: {e}")
        # This is expected if we can't connect to YugabyteDB
        print("This is expected if YugabyteDB is not running locally")

if __name__ == "__main__":
    print("🔍 Testing improved CDC detection...")
    asyncio.run(test_cdc_detection())
    print("✅ Test completed")