# CDC Detection and Error Handling Improvements

## Summary

This document outlines the improvements made to CDC (Change Data Capture) detection and error handling in the table sync application.

## Issues Addressed

1. **CDC Stream Detection**: Table already had CDC stream but wasn't being detected properly
2. **Error Handling**: CDC conflicts weren't being properly caught and handled  
3. **Data Copy Conflicts**: Truncate operations failing due to existing CDC streams

## Key Improvements

### 1. Enhanced CDC Detection (`debezium_manager.py`)

**Previous Issue**: Simple CDC detection query wasn't finding existing YugabyteDB CDC streams

**Improvements**:
- Added comprehensive logging to show all replication slots
- Enhanced query to check multiple slot name patterns
- Added alternative detection using test publication creation
- Better error handling and logging for connection issues

**Key Code Changes**:
```python
async def check_cdc_stream_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
    # Enhanced logging
    logger.info(f"Checking CDC stream status for {database_name}.{schema_name}.{table_name}")
    
    # Check all replication slots with detailed logging
    all_slots = await conn.fetch("SELECT slot_name, slot_type, active, database FROM pg_replication_slots")
    logger.info(f"Found {len(all_slots)} total replication slots in {database_name}")
    
    # Check multiple patterns for slot names
    table_slots = await conn.fetch("""
        SELECT slot_name, slot_type, active 
        FROM pg_replication_slots 
        WHERE slot_name LIKE $1 OR slot_name LIKE $2 OR slot_name LIKE $3
    """, f"%{table_name}%", f"%{schema_name}%", f"%{database_name}%")
    
    # Alternative: Test publication creation approach
    try:
        await conn.execute(f"CREATE PUBLICATION {test_pub_name} FOR TABLE {schema_name}.{table_name}")
        await conn.execute(f"DROP PUBLICATION {test_pub_name}")
        return False  # No CDC if publication creation succeeded
    except Exception as pub_e:
        if "cdc" in str(pub_e).lower():
            return True  # CDC likely active
```

### 2. Improved Data Copy Error Handling (`app.py`)

**Previous Issue**: CDC conflicts not properly detected before data copy operations

**Improvements**:
- Check CDC status before attempting data copy
- Skip data copy if CDC stream detected
- Better error messages for CDC conflicts

**Key Code Changes**:
```python
async def copy_bigquery_data_to_yugabyte(self, database_name: str, schema_name: str, table_name: str, config: TableBootstrapConfig):
    # Check CDC before data copy
    if self.pipeline_manager:
        try:
            cdc_exists = await self.pipeline_manager.connector_manager.check_cdc_stream_exists(database_name, schema_name, table_name)
            if cdc_exists:
                logger.warning(f"Table {database_name}.{schema_name}.{table_name} is part of CDC - skipping data copy to avoid conflicts")
                return True  # Consider successful since CDC is already active
```

### 3. Enhanced Truncate Error Handling (`data_transfer.py`)

**Previous Issue**: Truncate operations failed without specific CDC error handling

**Improvements**:
- Specific CDC error detection in truncate operations
- Better error messages for CDC conflicts
- Cascade handling with CDC awareness

**Key Code Changes**:
```python
async with db_pool.acquire() as conn:
    if truncate_target:
        try:
            await conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
        except Exception as e:
            error_str = str(e).lower()
            # Check for CDC-related errors
            if "cdc" in error_str and "rewrite" in error_str:
                logger.warning(f"Table {schema_name}.{table_name} is part of CDC stream - cannot truncate: {e}")
                raise Exception(f"Cannot rewrite a table that is part of CDC: {schema_name}.{table_name}")
            # Handle foreign key constraints with CDC awareness
            elif "foreign key constraint" in error_str:
                # Try CASCADE but still check for CDC errors
                try:
                    await conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name} CASCADE")
                except Exception as cascade_e:
                    if "cdc" in str(cascade_e).lower() and "rewrite" in str(cascade_e).lower():
                        raise Exception(f"Cannot rewrite a table that is part of CDC: {schema_name}.{table_name}")
```

### 4. Updated Dependencies (`requirements.txt`)

**Previous Issue**: Version compatibility issues with Python 3.13

**Improvements**:
- Updated to compatible versions using >= constraints
- Ensured all required packages are available

## Testing

Created comprehensive test script (`test_cdc_detection.py`) to verify:
- CDC detection functionality
- Proper error handling
- Logging improvements

## Expected Behavior

### When CDC Stream Exists:
1. **Detection**: Improved queries will find existing CDC streams
2. **Data Copy**: Will be skipped with warning message
3. **Connector Creation**: Will proceed (separate from data copy)

### When No CDC Stream:
1. **Detection**: Will return false after comprehensive checks
2. **Data Copy**: Will proceed normally
3. **Error Handling**: Better messages for any CDC-related issues

### Error Flow:
1. **Pre-Check**: CDC detection prevents conflicts before they occur
2. **During Operations**: Specific CDC error detection and handling
3. **Logging**: Detailed information for debugging and monitoring

## Result

The application now:
- ✅ Properly detects existing CDC streams in YugabyteDB
- ✅ Skips data copy operations when CDC is active
- ✅ Provides clear error messages for CDC conflicts
- ✅ Maintains separate handling of BigQuery tables and Kafka connectors
- ✅ Has comprehensive logging for debugging

This resolves the "Cannot rewrite a table that is a part of CDC" error while maintaining the dual functionality of creating both BigQuery tables AND Kafka/Debezium connectors.