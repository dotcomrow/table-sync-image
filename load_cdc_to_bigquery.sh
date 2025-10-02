#!/bin/bash
echo "🏗️  Creating BigQuery Dataset and Table"

# Create dataset (ignore error if exists)
bq mk --dataset --location=US k8s-kafka-774d:cdc_test_dataset 2>/dev/null || echo "Dataset already exists"

# Create table with schema
bq mk --table \
  k8s-kafka-774d:cdc_test_dataset.yugabyte_cdc_data \
  id:INTEGER,name:STRING,message:STRING,created_at:TIMESTAMP,cdc_operation:STRING,cdc_timestamp:TIMESTAMP,cdc_table:STRING,cdc_database:STRING,cdc_transaction_id:STRING,cdc_tablet_id:STRING

echo "✅ Table created"

# Load the data
echo "📊 Loading CDC data into BigQuery..."
bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --replace \
  k8s-kafka-774d:cdc_test_dataset.yugabyte_cdc_data \
  live_cdc_data_for_bigquery.json

echo "🎉 Data loaded into BigQuery!"
echo "🔍 Query your data: SELECT * FROM \`k8s-kafka-774d.cdc_test_dataset.yugabyte_cdc_data\`"