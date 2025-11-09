import uuid
from datetime import datetime

def now_iso():
    return datetime.utcnow().isoformat()

def new_id():
    return str(uuid.uuid4())[:8]

def parse_iso(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))
