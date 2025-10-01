#!/usr/bin/env python3
"""
Component Test 2: Kafka Connect Health Check
Tests if Kafka Connect is accessible and responsive
"""
import asyncio
import aiohttp
import json
import os

async def test_kafka_connect():
    """Test Kafka Connect service health"""
    print("🔌 Testing Kafka Connect Service...")
    
    connect_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
    print(f"Connect URL: {connect_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Test basic connectivity
            async with session.get(f"{connect_url}/") as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Kafka Connect responsive: {data.get('version', 'unknown version')}")
                else:
                    print(f"❌ Kafka Connect returned status: {response.status}")
                    return False
            
            # Test connector plugins
            async with session.get(f"{connect_url}/connector-plugins") as response:
                if response.status == 200:
                    plugins = await response.json()
                    yugabyte_plugins = [p for p in plugins if 'yugabyte' in p.get('class', '').lower()]
                    print(f"✅ Found {len(plugins)} connector plugins")
                    print(f"✅ YugabyteDB plugins: {len(yugabyte_plugins)}")
                    for plugin in yugabyte_plugins:
                        print(f"   - {plugin['class']} (v{plugin.get('version', 'unknown')})")
                else:
                    print(f"❌ Failed to get connector plugins: {response.status}")
                    return False
            
            # Test existing connectors
            async with session.get(f"{connect_url}/connectors") as response:
                if response.status == 200:
                    connectors = await response.json()
                    print(f"✅ Existing connectors: {len(connectors)}")
                    for connector in connectors:
                        print(f"   - {connector}")
                else:
                    print(f"❌ Failed to get connectors: {response.status}")
                    return False
        
        print("✅ Kafka Connect test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Kafka Connect test FAILED: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_kafka_connect())