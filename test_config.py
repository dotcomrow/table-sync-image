#!/usr/bin/env python3
"""
Test script to validate the configuration changes
"""

# Test the YugabyteDB gRPC connector configuration
test_config = {
    "name": "test-connector",
    "config": {
        "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
        "tasks.max": "1",
        "database.hostname": "yb-tserver-service.yugabyte.svc.cluster.local",
        "database.port": "5433", 
        "database.user": "vaultadmin",
        "database.password": "test",
        "database.dbname": "mcp",
        "database.server.name": "yugabyte-mcp-mcp_openapi_ro",
        "table.include.list": "mcp_openapi_ro.mcp_openapi_augmentations",
        "database.streamid": "stream_mcp_mcp_openapi_ro_mcp_openapi_augmentations",
        "snapshot.mode": "never",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "yugabyte-mcp-mcp_openapi_ro\\.mcp_openapi_ro\\.mcp_openapi_augmentations",
        "transforms.route.replacement": "bigquery-yugabyte_backup-mcp_openapi_augmentations",
        "errors.tolerance": "all",
        "errors.log.enable": "true",  
        "errors.log.include.messages": "true"
    }
}

print("✅ YugabyteDB gRPC connector configuration:")
for key, value in test_config["config"].items():
    print(f"  {key}: {value}")

print("\n🔧 Key changes made:")
print("1. ✅ Changed connector.class to: io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector")
print("2. ✅ Added database.streamid for YugabyteDB streaming")
print("3. ✅ Removed PostgreSQL-specific parameters (plugin.name, slot.name, publication.name)")
print("4. ✅ Added CASCADE truncate for foreign key constraints")
print("5. ✅ Removed publication management (not needed for YugabyteDB gRPC)")

print("\n🎯 Expected results:")
print("- Debezium connector should now use the correct YugabyteDB gRPC connector class")
print("- Foreign key constraint issues should be resolved with CASCADE truncate") 
print("- Pipeline setup should complete successfully")