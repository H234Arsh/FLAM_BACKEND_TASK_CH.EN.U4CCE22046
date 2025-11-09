#!/usr/bin/env bash
# start workers in background for quick demo
python3 queuectl.py worker start --count 2 &
W_PID=$!
echo "Workers started (pid $W_PID)"
sleep 15
echo "Stopping workers..."
kill -INT $W_PID || true
