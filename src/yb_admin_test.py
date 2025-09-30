#!/usr/bin/env python3
"""
Test script to check yb-admin availability and functionality
"""
import subprocess
import os
import sys

def test_yb_admin():
    """Test if yb-admin is available and working"""
    
    print("🔍 Testing yb-admin availability...")
    
    # Get master addresses from environment
    master_addresses = os.getenv("YUGABYTE_MASTER_ADDRESSES", "localhost:7100")
    print(f"Master addresses: {master_addresses}")
    
    try:
        # Test 1: Check if yb-admin command exists
        print("\n1. Testing yb-admin command availability...")
        result = subprocess.run(
            ["which", "yb-admin"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print(f"✅ yb-admin found at: {result.stdout.strip()}")
        else:
            print("❌ yb-admin command not found in PATH")
            print("This explains why automated cleanup isn't working!")
            return False
            
        # Test 2: Try to connect and list CDC streams
        print("\n2. Testing yb-admin connectivity...")
        list_cmd = [
            "yb-admin", 
            "--master_addresses", master_addresses,
            "list_cdc_streams"
        ]
        
        print(f"Running: {' '.join(list_cmd)}")
        
        result = subprocess.run(
            list_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print("✅ Successfully connected to YugabyteDB masters")
            print("📋 Current CDC streams:")
            print(result.stdout)
            
            # Count streams
            lines = result.stdout.strip().split('\n')
            stream_count = sum(1 for line in lines if 'stream_id' in line.lower())
            print(f"Found {stream_count} CDC streams")
            
            return True
        else:
            print(f"❌ Failed to connect to YugabyteDB masters")
            print(f"Error: {result.stderr}")
            print(f"This indicates connectivity issues with master addresses: {master_addresses}")
            return False
            
    except FileNotFoundError:
        print("❌ yb-admin command not found - not installed in container")
        return False
    except subprocess.TimeoutExpired:
        print("⏰ Timeout connecting to YugabyteDB masters")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def suggest_solutions():
    """Suggest alternative solutions"""
    print("\n🔧 ALTERNATIVE SOLUTIONS:")
    print("="*50)
    
    print("\n1. CONTAINER SOLUTION:")
    print("   - Install yb-admin in your Docker container")
    print("   - Add YugabyteDB tools to container image")
    
    print("\n2. EXTERNAL CLEANUP SCRIPT:")
    print("   - Run yb-admin commands from outside the container")
    print("   - Create a cleanup job that runs before connector creation")
    
    print("\n3. YUGABYTEDB RESTART:")
    print("   - Restart YugabyteDB cluster to clear all CDC streams")
    print("   - Nuclear option but guaranteed to work")
    
    print("\n4. KUBERNETES JOB:")
    print("   - Create a K8s job with yb-admin access")
    print("   - Run cleanup before deploying connectors")
    
    print("\n5. API-BASED CLEANUP:")
    print("   - Use YugabyteDB REST API instead of yb-admin")
    print("   - May have CDC stream management endpoints")

if __name__ == "__main__":
    print("YugabyteDB yb-admin Diagnostic Tool")
    print("="*40)
    
    success = test_yb_admin()
    
    if not success:
        suggest_solutions()
        sys.exit(1)
    else:
        print("\n✅ yb-admin is working correctly!")
        print("The automated cleanup should work once deployed.")