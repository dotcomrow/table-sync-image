#!/bin/bash

# Start YugabyteDB in the background
bin/yugabyted start --daemon=true

# Wait for YugabyteDB to be ready
until curl -s http://yugabytedb:7000 > /dev/null; do
  echo "Waiting for YugabyteDB to be ready..."
  sleep 5
done

# Execute the init.sql script
ysqlsh -h yugabytedb -p 5433 -f /docker-entrypoint-initdb.d/init.sql

# Keep the container running
tail -f /dev/null