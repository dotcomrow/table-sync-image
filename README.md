# Table Sync Orchestrator# YugabyteDB to BigQuery CDC Processor



Production-ready table discovery and synchronization orchestrator for YugabyteDB to BigQuery.A **production-ready, rock-solid** Docker image for streaming Change Data Capture (CDC) events from YugabyteDB to Google BigQuery. Built using battle-tested, industry-standard components for maximum reliability and minimal maintenance.



## Overview## 🎯 Design Philosophy



The Table Sync Orchestrator automatically discovers annotated tables in YugabyteDB and manages their synchronization to BigQuery. It scans all databases, schemas, and tables every 30 seconds, creating BigQuery resources and CDC connectors as needed.- **Battle-tested components only**: Uses libraries with 5+ years of production use

- **Zero custom protocols**: Standard Kafka + BigQuery APIs

## Features- **Maximum observability**: Prometheus metrics, structured logging, health checks

- **Kubernetes-native**: Designed for cloud-native deployments

- **🔍 Automatic Discovery**: Scans all databases/schemas/tables for annotations- **Minimal maintenance**: Self-healing, auto-scaling, graceful degradation

- **📊 Intelligent Sync**: Only syncs when needed (new/changed/missing resources)  - **AI-assistant friendly**: Well-documented, standard patterns, extensive logging

- **⚡ Auto-Provisioning**: Creates BigQuery datasets/tables automatically

- **🔄 CDC Management**: Creates and manages Kafka Connect CDC connectors## 🏗️ Architecture

- **📈 Production Ready**: Health checks, metrics, structured logging

- **🛡️ Battle-Tested**: Uses proven libraries with 5+ years production experience```

YugabyteDB → Kafka CDC → This Processor → Google BigQuery

## Architecture                 ↓

            [Prometheus Metrics + Health Checks]

``````

┌─────────────────────────────────────────────────────────────┐

│                Table Sync Orchestrator                      │### Core Components

├─────────────────────────────────────────────────────────────┤

│                                                             │- **Kafka Consumer**: `kafka-python` (8+ years in production)

│  Every 30s:                                                 │- **BigQuery Client**: `google-cloud-bigquery` (Google's official SDK)

│  1. Scan all databases/schemas/tables                       │- **Observability**: Prometheus + structured logging

│  2. Parse table annotations                                 │- **Health**: Flask endpoints for Kubernetes probes

│  3. Compare with status table                               │- **Resilience**: Tenacity for retry logic with exponential backoff

│  4. Create BigQuery resources if needed                     │

│  5. Sync initial data if BigQuery was missing               │## 🚀 Features

│  6. Create/update CDC connectors                            │

│  7. Update status table                                     │- **Automatic Table Discovery**: Scans YugabyteDB for tables with bootstrap configuration comments

│                                                             │- **Bidirectional Sync**: Supports both YugabyteDB → BigQuery and BigQuery → YugabyteDB data flow

└─────────────────────────────────────────────────────────────┘- **Real-time CDC**: Uses Debezium connectors for real-time change capture

```- **Smart State Management**: Tracks table sync status in YugabyteDB

- **Auto-provisioning**: Automatically creates BigQuery datasets and tables

## Table Annotations- **Lifecycle Management**: Handles table addition, modification, and removal

- **Health Monitoring**: Built-in health checks and metrics collection

Tables are enabled for sync using comment annotations:- **Docker-ready**: Complete containerized setup with Docker Compose



```sql## 📋 Prerequisites

COMMENT ON TABLE mcp_openapi_ro.mcp_openapi_augmentations IS

'{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.mcp_openapi_augmentations"}}';- Docker and Docker Compose

```- Google Cloud Platform account with BigQuery API enabled

- Service account with BigQuery Admin permissions

## Quick Start- Google Cloud Storage bucket for temporary data transfer



### Docker## 🏗️ Architecture



```bash```

# Build the image┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐

docker build -f Dockerfile.production -t table-sync-orchestrator .│   YugabyteDB    │    │   Kafka Connect  │    │   Google Cloud  │

│                 │    │   (Debezium)     │    │                 │

# Run with environment variables│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │

docker run -e YUGABYTEDB_HOST=localhost \│ │   Tables    │◄├────┤ │ Connectors   │ ├────┤►│  BigQuery   │ │

           -e YUGABYTEDB_PORT=5433 \│ │ with        │ │    │ │              │ │    │ │  Tables     │ │

           -e KAFKA_CONNECT_URL=http://localhost:8083 \│ │ Comments    │ │    │ │              │ │    │ │             │ │

           -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │

           -v /path/to/credentials.json:/app/credentials.json \│                 │    │                  │    │                 │

           table-sync-orchestrator│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │

```│ │ State       │ │    │ │    Kafka     │ │    │ │   Cloud     │ │

│ │ Table       │ │    │ │   Topics     │ │    │ │  Storage    │ │

### Kubernetes│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │

└─────────────────┘    └──────────────────┘    └─────────────────┘

```bash           ▲                       ▲                       ▲

kubectl apply -f deployment/production-cdc-processor.yaml           │                       │                       │

```           └───────────────────────┴───────────────────────┘

                     Table Sync Application

## Configuration```



The orchestrator uses YAML configuration with environment variable substitution:## 🔧 Configuration



```yaml### Environment Variables

# Scanning Configuration

scan_interval_seconds: 30Create a `.env` file (use `src/.env.example` as template):



# YugabyteDB Connection```bash

yugabytedb:# YugabyteDB Configuration

  host: yb-tserver-service.yugabyte.svc.cluster.localDATABASE_URL=postgresql://yugabyte:yugabyte@localhost:5433/yugabyte

  port: 5433

  user: yugabyte# Google Cloud Configuration

  password: yugabyteBIGQUERY_PROJECT_ID=your-gcp-project-id

GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# BigQuery Configuration (project auto-derived from service account)TEMP_STORAGE_BUCKET=your-temp-storage-bucket

bigquery:

  credentials_path: /vault/secrets/gcp-key.json# Kafka & Debezium Configuration

  location: USKAFKA_BOOTSTRAP_SERVERS=localhost:9092

DEBEZIUM_CONNECTOR_URL=http://localhost:8083

# Kafka Connect Configuration

kafka_connect:# Application Configuration

  url: http://kafka-connect.kafka.svc.internal.lan:8083SCAN_INTERVAL_SECONDS=30

```LOG_LEVEL=INFO

BATCH_SIZE=10000

## Monitoring```



### Health Checks### Table Bootstrap Configuration

- **Health**: `http://localhost:8080/health`

- **Readiness**: `http://localhost:8080/ready`Add comments to YugabyteDB tables to configure synchronization:



### Metrics (Prometheus)```sql

- **Endpoint**: `http://localhost:8000/metrics`COMMENT ON TABLE public.orders IS $$

- `sync_tables_scanned_total`: Total tables scanned{

- `sync_tables_synced_total`: Total tables synced    "bootstrap": {

- `sync_errors_total`: Total sync errors    "enabled": true,

- `sync_scan_duration_seconds`: Scan duration histogram    "bq": "sales_raw.orders",

- `sync_active_syncs`: Number of active syncs    "columns": "id,customer_id,status,total,created_at,updated_at"

  }

## Development}

$$;

### Local Setup```



```bash**Configuration Options:**

# Install dependencies- `enabled` (required): Boolean to enable/disable sync

pip install -r requirements.production.txt- `bq` (required): BigQuery destination in format `dataset.table`

- `columns` (optional): Explicit column order for COPY operations

# Copy environment template

cp src/.env.example .env## 🚀 Quick Start



# Edit configuration### 1. Setup

vim .env

Set required environment variables:

# Run the orchestrator

python src/table_sync_orchestrator.py```bash

```export BIGQUERY_PROJECT_ID="your-gcp-project"

export GOOGLE_APPLICATION_CREDENTIALS_PATH="/path/to/service-account.json"

### Buildingexport TEMP_STORAGE_BUCKET="your-temp-bucket"

```

```bash

# Use the provided build scriptRun the setup script:

./scripts/build.sh

```bash

# Or build manually./setup.sh

docker build -f Dockerfile.production -t table-sync-orchestrator .```

```

### 2. Start Services

## Directory Structure

```bash

```docker-compose up -d

├── src/```

│   ├── table_sync_orchestrator.py  # Main orchestrator application

│   ├── __init__.py                 # Python package marker### 3. Monitor

│   └── .env.example               # Environment template

├── config/```bash

│   └── orchestrator.yaml         # Configuration template# View application logs

├── deployment/docker-compose logs -f table-sync-app

│   └── production-cdc-processor.yaml  # Kubernetes deployment

├── scripts/# Check health status

│   ├── build.sh                  # Build scriptdocker-compose exec table-sync-app python health_check.py health

│   └── health_check.sh          # Health check script

├── .github/workflows/# View sync metrics

│   └── build-production-cdc.yaml # GitHub Actions builddocker-compose exec table-sync-app python health_check.py metrics

├── Dockerfile.production         # Production container

└── requirements.production.txt   # Python dependencies# List tracked tables

```docker-compose exec table-sync-app python health_check.py tables

```

## License

## � Schema Initialization

MIT License - see LICENSE file for details.
The application automatically validates and prepares the YugabyteDB schema on startup:

### Automatic Schema Setup

On first startup, the application will:

1. **Test Database Connectivity**: Validate connection and basic permissions
2. **Check Database Capabilities**: Ensure JSONB support and logical replication
3. **Create State Tables**: Set up `table_sync_state` and `table_sync_metadata` tables
4. **Create Indexes**: Add performance indexes for efficient querying
5. **Validate Schema**: Test all operations to ensure everything works

### Manual Schema Testing

You can test the schema initialization independently:

```bash
# Test schema initialization
docker-compose exec table-sync-app python test_schema.py

# Expected output shows:
# - Database connectivity validation
# - Schema preparation steps
# - State table creation and testing
# - Performance validation
```

### Schema Components

The application creates these database objects:

- **`table_sync_state`**: Main state tracking table
  - Tracks each table's sync configuration and status
  - Stores bootstrap config as JSONB
  - Maintains timestamps and status flags

- **`table_sync_metadata`**: Application metadata
  - Stores schema version information
  - Tracks initialization timestamps
  - Future extensibility for app settings

- **Performance Indexes**: Optimized for common queries
  - Bootstrap configuration lookups
  - Status filtering
  - Time-based queries

### Schema Validation

The startup process validates:

- ✅ Database connectivity and permissions
- ✅ Required PostgreSQL extensions (uuid-ossp)
- ✅ JSONB support for configuration storage
- ✅ Logical replication capabilities for CDC
- ✅ Complete CRUD operations on state tables
- ✅ Index creation and performance optimization

If any validation fails, the application will log detailed error messages and exit gracefully.

## �📖 How It Works

### 1. Table Discovery
Every 30 seconds, the application scans all YugabyteDB tables for bootstrap configuration comments.

### 2. State Management
Table states are tracked in the `table_sync_state` table:
- Current configuration hash
- BigQuery table status
- Pipeline configuration status
- Last update timestamp

### 3. Synchronization Logic

#### New Table with Bootstrap Config
1. **BigQuery table doesn't exist**:
   - Create BigQuery dataset (if needed)
   - Create BigQuery table with matching schema
   - Copy existing YugabyteDB data to BigQuery
   - Set up Debezium connector for real-time CDC

2. **BigQuery table exists**:
   - Copy BigQuery data to YugabyteDB (overwrite)
   - Set up Debezium connector for real-time CDC

#### Configuration Changes
- **Bootstrap enabled**: Create sync pipeline
- **Bootstrap disabled**: Remove BigQuery table and pipeline
- **Config modified**: Update pipeline configuration

#### Table Removal
- Delete BigQuery table
- Remove Debezium connector
- Clean up state records

### 4. Real-time Sync
Debezium connectors capture all changes (INSERT, UPDATE, DELETE) and stream them through Kafka to BigQuery.

## 🛠️ Components

### Core Application (`src/app.py`)
- Main application loop
- Table discovery and state management
- Synchronization orchestration

### Database Manager (`src/app.py`)
- YugabyteDB connection pooling
- State table management
- Schema introspection

### BigQuery Manager (`src/app.py`)
- BigQuery table and dataset management
- Schema mapping from PostgreSQL to BigQuery
- Table lifecycle operations

### Data Transfer Manager (`src/data_transfer.py`)
- Bulk data transfer between YugabyteDB and BigQuery
- Uses Cloud Storage as intermediate staging
- Handles large datasets efficiently

### Debezium Manager (`src/debezium_manager.py`)
- Debezium connector lifecycle management
- YugabyteDB publication management
- Kafka Connect API integration

### Health Check (`src/health_check.py`)
- System health monitoring
- Metrics collection
- Component status validation

## 📊 Monitoring & Observability

### Health Checks
```bash
# Overall system health
docker-compose exec table-sync-app python health_check.py health

# Response includes:
# - YugabyteDB connectivity
# - BigQuery connectivity  
# - Debezium Connect API status
# - State table accessibility
```

### Metrics
```bash
# Synchronization metrics
docker-compose exec table-sync-app python health_check.py metrics

# Provides:
# - Total tracked tables
# - Active bootstrap configurations
# - Running pipelines
# - Recent activity
```

### Table Details
```bash
# Detailed table information
docker-compose exec table-sync-app python health_check.py tables

# Shows per-table:
# - Configuration status
# - BigQuery targets
# - Pipeline status
# - Last update times
```

## 🔍 Troubleshooting

### Common Issues

1. **Debezium Connector Fails**
   - Check YugabyteDB publication exists
   - Verify WAL level configuration
   - Ensure proper permissions

2. **BigQuery Connection Issues**
   - Validate service account permissions
   - Check BigQuery API enabled
   - Verify credentials file path

3. **Data Transfer Failures**
   - Ensure temp bucket exists and is accessible
   - Check Cloud Storage permissions
   - Verify network connectivity

### Logs

```bash
# Application logs
docker-compose logs table-sync-app

# Kafka Connect logs
docker-compose logs kafka-connect

# YugabyteDB logs
docker-compose logs yugabytedb
```

## 🏭 Production Deployment

### Kubernetes Deployment

The application is designed to run in Kubernetes environments:

1. **ConfigMaps**: Store configuration
2. **Secrets**: Store sensitive credentials
3. **Deployments**: Run the sync application
4. **Services**: Expose health check endpoints
5. **ServiceMonitors**: Prometheus monitoring integration

### Scaling Considerations

- **Single Instance**: Recommended to avoid conflicts
- **Database Pooling**: Configure appropriate connection limits
- **Resource Limits**: Set memory/CPU limits based on data volume
- **Storage**: Provision adequate storage for Kafka topics

### Security

- Use Google Cloud Workload Identity for GKE deployments
- Rotate service account keys regularly
- Enable audit logging for BigQuery operations
- Use network policies to restrict traffic

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review application logs
3. Open an issue with detailed information
