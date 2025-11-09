#!/usr/bin/env bash
set -e
python3 queuectl.py enqueue '{"id":"job-ok","command":"echo OK","max_retries":2}'
python3 queuectl.py enqueue '{"id":"job-fail","command":"bash -c \"exit 2\"","max_retries":2}'
python3 queuectl.py enqueue '{"id":"job-sleep","command":"sleep 5 && echo done","max_retries":1,"timeout":3}'
# scheduled job run 10 seconds in future
FUTURE=$(python3 - <<PY
from datetime import datetime, timezone, timedelta
print((datetime.now(timezone.utc)+timedelta(seconds=10)).isoformat())
PY
)
python3 queuectl.py enqueue "{\"id\":\"job-scheduled\",\"command\":\"echo scheduled\",\"run_at\":\"$FUTURE\"}"
echo "Enqueued demo jobs"
