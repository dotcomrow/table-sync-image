# Table Sync Application

A comprehensive Python application that automatically synchronizes YugabyteDB tables with Google BigQuery using Debezium and Kafka for real-time change data capture (CDC).

## 🚀 Features

- **Automatic Table Discovery**: Scans YugabyteDB for tables with bootstrap configuration comments
- **Bidirectional Sync**: Supports both YugabyteDB → BigQuery and BigQuery → YugabyteDB data flow
- **Real-time CDC**: Uses Debezium connectors for real-time change capture
- **Smart State Management**: Tracks table sync status in YugabyteDB
- **Auto-provisioning**: Automatically creates BigQuery datasets and tables
- **Lifecycle Management**: Handles table addition, modification, and removal
- **Health Monitoring**: Built-in health checks and metrics collection
- **Docker-ready**: Complete containerized setup with Docker Compose

## 📋 Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account with BigQuery API enabled
- Service account with BigQuery Admin permissions
- Google Cloud Storage bucket for temporary data transfer

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   YugabyteDB    │    │   Kafka Connect  │    │   Google Cloud  │
│                 │    │   (Debezium)     │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │   Tables    │◄├────┤ │ Connectors   │ ├────┤►│  BigQuery   │ │
│ │ with        │ │    │ │              │ │    │ │  Tables     │ │
│ │ Comments    │ │    │ │              │ │    │ │             │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │ State       │ │    │ │    Kafka     │ │    │ │   Cloud     │ │
│ │ Table       │ │    │ │   Topics     │ │    │ │  Storage    │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
           ▲                       ▲                       ▲
           │                       │                       │
           └───────────────────────┴───────────────────────┘
                     Table Sync Application
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file (use `src/.env.example` as template):

```bash
# YugabyteDB Configuration
DATABASE_URL=postgresql://yugabyte:yugabyte@localhost:5433/yugabyte

# Google Cloud Configuration
BIGQUERY_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
TEMP_STORAGE_BUCKET=your-temp-storage-bucket

# Kafka & Debezium Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DEBEZIUM_CONNECTOR_URL=http://localhost:8083

# Application Configuration
SCAN_INTERVAL_SECONDS=30
LOG_LEVEL=INFO
BATCH_SIZE=10000
```

### Table Bootstrap Configuration

Add comments to YugabyteDB tables to configure synchronization:

```sql
COMMENT ON TABLE public.orders IS $$
{
  "bootstrap": {
    "enabled": true,
    "bq": "sales_raw.orders",
    "columns": "id,customer_id,status,total,created_at,updated_at"
  }
}
$$;
```

**Configuration Options:**
- `enabled` (required): Boolean to enable/disable sync
- `bq` (required): BigQuery destination in format `dataset.table`
- `columns` (optional): Explicit column order for COPY operations

## 🚀 Quick Start

### 1. Setup

Set required environment variables:

```bash
export BIGQUERY_PROJECT_ID="your-gcp-project"
export GOOGLE_APPLICATION_CREDENTIALS_PATH="/path/to/service-account.json"
export TEMP_STORAGE_BUCKET="your-temp-bucket"
```

Run the setup script:

```bash
./setup.sh
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Monitor

```bash
# View application logs
docker-compose logs -f table-sync-app

# Check health status
docker-compose exec table-sync-app python health_check.py health

# View sync metrics
docker-compose exec table-sync-app python health_check.py metrics

# List tracked tables
docker-compose exec table-sync-app python health_check.py tables
```

## 📖 How It Works

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
