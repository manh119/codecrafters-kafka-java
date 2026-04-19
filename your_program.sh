#!/bin/sh
set -e

cd "$CODECRAFTERS_REPOSITORY_DIR"

echo "Building project..."
#mvn package   # ❗ bỏ -q để thấy lỗi

echo "Running server..."
exec java -jar target/codecrafters-kafka.jar "$@"