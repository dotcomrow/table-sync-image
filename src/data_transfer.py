"""
Data transfer utilities for copying data between YugabyteDB and BigQuery
"""
import asyncio
import csv
import tempfile
from typing import List, Dict, Optional
import os
from google.cloud import storage
from google.cloud import bigquery
import asyncpg
from loguru import logger

class DataTransferManager:
    def __init__(self, project_id: str, temp_bucket: Optional[str] = None):
        self.project_id = project_id
        self.temp_bucket = temp_bucket or f"{project_id}-table-sync-temp"
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        
    async def copy_yugabyte_to_bigquery(
        self, 
        db_pool: asyncpg.Pool,
        schema_name: str, 
        table_name: str,
        bq_dataset: str,
        bq_table: str,
        columns: Optional[str] = None,
        batch_size: int = 10000
    ):
        """Copy data from YugabyteDB table to BigQuery"""
        
        logger.info(f"Starting data copy from {schema_name}.{table_name} to {bq_dataset}.{bq_table}")
        
        # Create temporary file for data export
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_filename = temp_file.name
            
            try:
                # Export data to CSV
                await self._export_yugabyte_to_csv(
                    db_pool, schema_name, table_name, temp_filename, columns, batch_size
                )
                
                # Upload to Cloud Storage
                blob_name = f"table-sync/{schema_name}/{table_name}/{table_name}.csv"
                await self._upload_to_gcs(temp_filename, blob_name)
                
                # Load into BigQuery
                await self._load_csv_to_bigquery(blob_name, bq_dataset, bq_table)
                
                # Cleanup
                await self._cleanup_gcs_file(blob_name)
                
                logger.info(f"Successfully copied data from {schema_name}.{table_name} to BigQuery")
                
            finally:
                # Cleanup local temp file
                if os.path.exists(temp_filename):
                    os.unlink(temp_filename)
    
    async def copy_bigquery_to_yugabyte(
        self,
        db_pool: asyncpg.Pool,
        bq_dataset: str,
        bq_table: str,
        schema_name: str,
        table_name: str,
        truncate_target: bool = True
    ):
        """Copy data from BigQuery to YugabyteDB (with optional truncation)"""
        
        logger.info(f"Starting data copy from {bq_dataset}.{bq_table} to {schema_name}.{table_name}")
        
        # Export BigQuery table to Cloud Storage
        blob_name = f"table-sync-reverse/{bq_dataset}/{bq_table}/{bq_table}.csv"
        await self._export_bigquery_to_gcs(bq_dataset, bq_table, blob_name)
        
        # Download and import to YugabyteDB
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp_file:
            temp_filename = temp_file.name
            
            try:
                await self._download_from_gcs(blob_name, temp_filename)
                await self._import_csv_to_yugabyte(
                    db_pool, temp_filename, schema_name, table_name, truncate_target
                )
                
                # Cleanup
                await self._cleanup_gcs_file(blob_name)
                
                logger.info(f"Successfully copied data from BigQuery to {schema_name}.{table_name}")
                
            finally:
                if os.path.exists(temp_filename):
                    os.unlink(temp_filename)
    
    async def _export_yugabyte_to_csv(
        self, 
        db_pool: asyncpg.Pool,
        schema_name: str, 
        table_name: str, 
        filename: str, 
        columns: Optional[str] = None,
        batch_size: int = 10000
    ):
        """Export YugabyteDB table data to CSV file"""
        
        columns_clause = columns if columns else "*"
        base_query = f"SELECT {columns_clause} FROM {schema_name}.{table_name}"
        
        # Get total count for progress tracking
        count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        
        async with db_pool.acquire() as conn:
            total_rows = await conn.fetchval(count_query)
            logger.info(f"Exporting {total_rows} rows from {schema_name}.{table_name}")
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = None
                offset = 0
                
                while offset < total_rows:
                    query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"
                    rows = await conn.fetch(query)
                    
                    if not rows:
                        break
                    
                    # Initialize CSV writer with headers from first batch
                    if writer is None:
                        fieldnames = list(rows[0].keys())
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                    
                    # Write batch
                    for row in rows:
                        writer.writerow(dict(row))
                    
                    offset += batch_size
                    
                    if offset % (batch_size * 10) == 0:
                        logger.info(f"Exported {offset}/{total_rows} rows")
                
        logger.info(f"Completed export of {schema_name}.{table_name} to {filename}")
    
    async def _import_csv_to_yugabyte(
        self,
        db_pool: asyncpg.Pool,
        filename: str,
        schema_name: str,
        table_name: str,
        truncate_target: bool = True
    ):
        """Import CSV data to YugabyteDB table"""
        
        async with db_pool.acquire() as conn:
            if truncate_target:
                await conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
                logger.info(f"Truncated table {schema_name}.{table_name}")
            
            # Use PostgreSQL COPY command for efficient import
            with open(filename, 'r', encoding='utf-8') as csvfile:
                await conn.copy_from_table(
                    table_name,
                    source=csvfile,
                    schema_name=schema_name,
                    format='csv',
                    header=True
                )
            
        logger.info(f"Imported data to {schema_name}.{table_name}")
    
    async def _upload_to_gcs(self, local_filename: str, blob_name: str):
        """Upload file to Google Cloud Storage"""
        
        bucket = self.storage_client.bucket(self.temp_bucket)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_filename(local_filename)
        logger.info(f"Uploaded {local_filename} to gs://{self.temp_bucket}/{blob_name}")
    
    async def _download_from_gcs(self, blob_name: str, local_filename: str):
        """Download file from Google Cloud Storage"""
        
        bucket = self.storage_client.bucket(self.temp_bucket)
        blob = bucket.blob(blob_name)
        
        blob.download_to_filename(local_filename)
        logger.info(f"Downloaded gs://{self.temp_bucket}/{blob_name} to {local_filename}")
    
    async def _load_csv_to_bigquery(self, blob_name: str, dataset_id: str, table_id: str):
        """Load CSV from Cloud Storage to BigQuery"""
        
        dataset_ref = self.bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,  # Use existing table schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        uri = f"gs://{self.temp_bucket}/{blob_name}"
        load_job = self.bq_client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        
        if load_job.errors:
            raise Exception(f"BigQuery load job failed: {load_job.errors}")
        
        logger.info(f"Loaded data from {uri} to {dataset_id}.{table_id}")
    
    async def _export_bigquery_to_gcs(self, dataset_id: str, table_id: str, blob_name: str):
        """Export BigQuery table to Cloud Storage"""
        
        dataset_ref = self.bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        destination_uri = f"gs://{self.temp_bucket}/{blob_name}"
        
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.CSV,
            print_header=True
        )
        
        extract_job = self.bq_client.extract_table(
            table_ref, destination_uri, job_config=job_config
        )
        
        extract_job.result()  # Wait for job to complete
        
        if extract_job.errors:
            raise Exception(f"BigQuery extract job failed: {extract_job.errors}")
        
        logger.info(f"Exported {dataset_id}.{table_id} to {destination_uri}")
    
    async def _cleanup_gcs_file(self, blob_name: str):
        """Delete temporary file from Cloud Storage"""
        
        bucket = self.storage_client.bucket(self.temp_bucket)
        blob = bucket.blob(blob_name)
        
        try:
            blob.delete()
            logger.info(f"Cleaned up gs://{self.temp_bucket}/{blob_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {blob_name}: {e}")
    
    def ensure_temp_bucket_exists(self):
        """Ensure the temporary storage bucket exists"""
        
        try:
            bucket = self.storage_client.bucket(self.temp_bucket)
            if not bucket.exists():
                bucket = self.storage_client.create_bucket(self.temp_bucket)
                logger.info(f"Created temporary bucket: {self.temp_bucket}")
            else:
                logger.info(f"Using existing temporary bucket: {self.temp_bucket}")
        except Exception as e:
            logger.error(f"Failed to ensure temp bucket exists: {e}")
            raise