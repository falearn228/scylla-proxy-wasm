#!/bin/bash
set -e

echo "=== Starting integration test ==="

# 1. Start echo server
echo "Starting echo server on 127.0.0.1:9043"
go run scripts/echo_server.go &
ECHO_PID=$!
sleep 2

# 2. Start proxy
echo "Starting proxy on localhost:9042"
go run cmd/main.go &
PROXY_PID=$!
sleep 2

# 3. Run client
echo "Sending test query via proxy..."
cat <<EOF | nc localhost 9042
SELECT email FROM users;
EOF
# Wait a bit
sleep 1

# 4. Cleanup
echo "Killing processes..."
kill $PROXY_PID $ECHO_PID 2>/dev/null || true
wait $PROXY_PID $ECHO_PID 2>/dev/null || true

echo "=== Test complete ==="