#!/bin/bash
# Mock ScyllaDB server listening on 9043 for testing

echo "Starting mock ScyllaDB on port 9043..."
nc -l -k -p 9043 &
MOCK_PID=$!

# Simulate a response
sleep 2
echo "Mock ScyllaDB ready. Send queries to localhost:9043"

# Keep running until Ctrl+C
trap "kill $MOCK_PID" EXIT
wait $MOCK_PID
