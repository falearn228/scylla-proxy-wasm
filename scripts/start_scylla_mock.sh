#!/bin/bash
# Mock ScyllaDB server listening on 9043 for testing

echo "Starting mock ScyllaDB on port 9043..."

# Create a more robust mock that echoes data and waits a bit before closing
cat > /tmp/mock_scylla.sh << 'EOF'
#!/bin/bash
# Read input and echo it back with a small delay
while IFS= read -r line; do
    echo "$line"
    sleep 0.1
done
EOF
chmod +x /tmp/mock_scylla.sh

socat -v TCP-LISTEN:9043,fork EXEC:'/tmp/mock_scylla.sh' &
MOCK_PID=$!

# Wait for the server to be ready
sleep 1
echo "Mock ScyllaDB ready (PID: $MOCK_PID). Send queries to localhost:9043"

# Keep running until Ctrl+C
trap "kill $MOCK_PID" EXIT
wait $MOCK_PID
