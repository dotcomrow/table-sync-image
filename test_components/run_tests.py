#!/usr/bin/env python3
"""
Component Test Runner
Runs all component tests in sequence to isolate issues
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

async def run_all_tests():
    """Run all component tests in sequence"""
    print("🧪" + "="*60)
    print("🧪 COMPONENT TEST SUITE")
    print("🧪 Testing individual components to isolate issues")
    print("🧪" + "="*60)
    
    tests = [
        ("YugabyteDB Connection", "01_yugabyte_connection_test"),
        ("Kafka Connect Service", "02_kafka_connect_test"),
        ("Minimal Connector", "03_minimal_connector_test")
    ]
    
    results = {}
    
    for test_name, test_module in tests:
        print(f"\n🔍 Running: {test_name}")
        print("-" * 40)
        
        try:
            # Import and run the test
            module = __import__(test_module)
            if test_module == "01_yugabyte_connection_test":
                result = await module.test_yugabyte_connection()
            elif test_module == "02_kafka_connect_test":
                result = await module.test_kafka_connect()
            elif test_module == "03_minimal_connector_test":
                result = await module.test_minimal_connector()
            
            results[test_name] = result
            
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n🧪" + "="*60)
    print("🧪 TEST RESULTS SUMMARY")
    print("🧪" + "="*60)
    
    all_passed = True
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"🧪 {test_name:.<30} {status}")
        if not result:
            all_passed = False
    
    print("\n🧪 OVERALL:", "✅ ALL TESTS PASSED" if all_passed else "❌ SOME TESTS FAILED")
    
    if not all_passed:
        print("\n🧪 TROUBLESHOOTING RECOMMENDATIONS:")
        
        if not results.get("YugabyteDB Connection", True):
            print("🔧 - Check YugabyteDB deployment and DATABASE_URL")
            print("🔧 - Verify YugabyteDB pods are running and accessible")
        
        if not results.get("Kafka Connect Service", True):
            print("🔧 - Check Kafka Connect deployment and DEBEZIUM_CONNECTOR_URL")
            print("🔧 - Verify YugabyteDB connector plugin is installed")
        
        if not results.get("Minimal Connector", True):
            print("🔧 - Issue is in YugabyteDB connector itself")
            print("🔧 - Consider YugabyteDB redeploy or different connector version")
    
    return all_passed

if __name__ == "__main__":
    asyncio.run(run_all_tests())