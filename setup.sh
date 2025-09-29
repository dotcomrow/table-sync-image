#!/bin/bash

# Table Sync Application Setup Script

set -e

echo "🚀 Setting up Table Sync Application..."

# Check if required environment variables are set
check_env_vars() {
    echo "📋 Checking required environment variables..."
    
    required_vars=(
        "BIGQUERY_PROJECT_ID"
        "GOOGLE_APPLICATION_CREDENTIALS_PATH"
        "TEMP_STORAGE_BUCKET"
    )
    
    missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -ne 0 ]; then
        echo "❌ Missing required environment variables:"
        printf '%s\n' "${missing_vars[@]}"
        echo ""
        echo "Please set these variables and run the script again."
        echo "Example:"
        echo "export BIGQUERY_PROJECT_ID='your-project-id'"
        echo "export GOOGLE_APPLICATION_CREDENTIALS_PATH='/path/to/service-account.json'"
        echo "export TEMP_STORAGE_BUCKET='your-temp-bucket'"
        exit 1
    fi
    
    echo "✅ All required environment variables are set"
}

# Create necessary directories
setup_directories() {
    echo "📁 Setting up directories..."
    
    mkdir -p logs
    mkdir -p credentials
    mkdir -p debezium-plugins
    
    echo "✅ Directories created"
}

# Download Debezium PostgreSQL connector
setup_debezium_connector() {
    echo "🔌 Setting up Debezium PostgreSQL connector..."
    
    if [ ! -f "debezium-plugins/debezium-connector-postgres-2.4.1.Final-plugin.tar.gz" ]; then
        echo "Downloading Debezium PostgreSQL connector..."
        curl -L "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.4.1.Final/debezium-connector-postgres-2.4.1.Final-plugin.tar.gz" \
            -o debezium-plugins/debezium-connector-postgres-2.4.1.Final-plugin.tar.gz
        
        cd debezium-plugins
        tar -xzf debezium-connector-postgres-2.4.1.Final-plugin.tar.gz
        cd ..
    else
        echo "Debezium connector already downloaded"
    fi
    
    echo "✅ Debezium connector setup complete"
}

# Create .env file from template
setup_env_file() {
    echo "⚙️  Setting up environment file..."
    
    if [ ! -f ".env" ]; then
        cp src/.env.example .env
        echo "📝 Created .env file from template"
        echo "Please review and update the .env file with your specific configuration"
    else
        echo "ℹ️  .env file already exists"
    fi
}

# Validate Google Cloud credentials
validate_gcp_credentials() {
    echo "🔑 Validating Google Cloud credentials..."
    
    if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS_PATH" ]; then
        echo "❌ Google Cloud service account file not found at: $GOOGLE_APPLICATION_CREDENTIALS_PATH"
        echo "Please ensure the file exists and the path is correct."
        exit 1
    fi
    
    # Test if credentials work
    if command -v gcloud &> /dev/null; then
        echo "Testing BigQuery access..."
        export GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS_PATH"
        
        if gcloud auth application-default print-access-token &> /dev/null; then
            echo "✅ Google Cloud credentials are valid"
        else
            echo "⚠️  Could not validate credentials, but file exists"
        fi
    else
        echo "ℹ️  gcloud CLI not found, skipping credential validation"
    fi
}

# Build Docker images
build_images() {
    echo "🐳 Building Docker images..."
    
    docker-compose build table-sync-app
    
    echo "✅ Docker images built successfully"
}

# Start infrastructure services
start_infrastructure() {
    echo "🚦 Starting infrastructure services..."
    
    echo "Starting YugabyteDB, Kafka, and Debezium..."
    docker-compose up -d yugabytedb zookeeper kafka kafka-connect
    
    echo "⏳ Waiting for services to be ready..."
    
    # Wait for YugabyteDB
    echo "Waiting for YugabyteDB..."
    for i in {1..30}; do
        if docker-compose exec -T yugabytedb ysqlsh -h localhost -U yugabyte -d yugabyte -c "SELECT 1;" &> /dev/null; then
            echo "✅ YugabyteDB is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "❌ YugabyteDB failed to start within timeout"
            exit 1
        fi
        sleep 5
    done
    
    # Wait for Kafka Connect
    echo "Waiting for Kafka Connect..."
    for i in {1..20}; do
        if curl -s http://localhost:8083/connectors &> /dev/null; then
            echo "✅ Kafka Connect is ready"
            break
        fi
        if [ $i -eq 20 ]; then
            echo "❌ Kafka Connect failed to start within timeout"
            exit 1
        fi
        sleep 10
    done
    
    echo "✅ Infrastructure services are ready"
}

# Initialize database schema
initialize_database() {
    echo "🗄️  Initializing database schema..."
    
    # The app will create the state table automatically, but we can create test data
    docker-compose exec -T yugabytedb ysqlsh -h localhost -U yugabyte -d yugabyte << 'EOF'
-- Create a test schema and table
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create a sample table with bootstrap comment
CREATE TABLE IF NOT EXISTS test_schema.sample_orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add the bootstrap comment
COMMENT ON TABLE test_schema.sample_orders IS $$
{
  "bootstrap": {
    "enabled": true,
    "bq": "test_dataset.orders",
    "columns": "id,customer_id,status,total,created_at,updated_at"
  }
}
$$;

-- Insert some sample data
INSERT INTO test_schema.sample_orders (customer_id, status, total) VALUES 
(1, 'pending', 99.99),
(2, 'completed', 149.50),
(3, 'cancelled', 75.00)
ON CONFLICT DO NOTHING;

\echo 'Database initialization complete'
EOF
    
    echo "✅ Database schema initialized"
}

# Run health check
run_health_check() {
    echo "🏥 Running health check..."
    
    # Start the app temporarily for health check
    docker-compose up -d table-sync-app
    
    sleep 10
    
    # Run health check
    if docker-compose exec -T table-sync-app python health_check.py health; then
        echo "✅ Health check passed"
    else
        echo "⚠️  Health check failed, but this is expected on first run"
    fi
}

# Display final instructions
show_final_instructions() {
    echo ""
    echo "🎉 Setup complete!"
    echo ""
    echo "Next steps:"
    echo "1. Review and update the .env file with your specific configuration"
    echo "2. Start the application: docker-compose up -d"
    echo "3. Monitor logs: docker-compose logs -f table-sync-app"
    echo "4. Check health: docker-compose exec table-sync-app python health_check.py health"
    echo "5. View metrics: docker-compose exec table-sync-app python health_check.py metrics"
    echo ""
    echo "Useful commands:"
    echo "- View YugabyteDB: docker-compose exec yugabytedb ysqlsh -h localhost -U yugabyte -d yugabyte"
    echo "- Check Kafka Connect: curl http://localhost:8083/connectors"
    echo "- Stop all services: docker-compose down"
    echo ""
    echo "The application will automatically:"
    echo "- Scan for tables with bootstrap comments every 30 seconds"
    echo "- Create BigQuery datasets and tables as needed"
    echo "- Set up Debezium connectors for real-time sync"
    echo "- Manage the complete synchronization lifecycle"
    echo ""
}

# Main execution
main() {
    echo "Table Sync Application Setup"
    echo "==========================="
    echo ""
    
    check_env_vars
    setup_directories
    setup_debezium_connector
    setup_env_file
    validate_gcp_credentials
    build_images
    start_infrastructure
    initialize_database
    run_health_check
    show_final_instructions
}

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed or not in PATH"
    exit 1
fi

# Run main function
main