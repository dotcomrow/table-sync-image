#!/bin/sh

# Debugging: Log the script execution
echo "Entrypoint script executed."

# Check for a test target file
if [ -f "/app/test_target.txt" ]; then
    TEST_FILE=$(cat /app/test_target.txt)
    echo "Running single test from file: $TEST_FILE"
    python -Xfrozen_modules=off -m debugpy --listen 0.0.0.0:5678 --wait-for-client -m unittest "$TEST_FILE"
elif [ -n "$1" ]; then
    echo "Running single test: $1"
    python -Xfrozen_modules=off -m debugpy --listen 0.0.0.0:5678 --wait-for-client -m unittest "$1"
else
    echo "Running all tests."
    python -Xfrozen_modules=off -m debugpy --listen 0.0.0.0:5678 --wait-for-client -m unittest discover -s tests
fi