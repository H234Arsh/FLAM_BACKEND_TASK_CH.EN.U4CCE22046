import sqlite3
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any

DB_DEFAULT = "queue.db"


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def now_iso():
    return datetime.utcnow().isoformat()


@dataclass
class Job:
    id: str
    command: str
    state: str = "pending"
    attempts: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    run_at: Optional[str] = None
    output: Optional[str] = None
    last_run_at: Optional[str] = None
    created_at: Optional[str] = field(default_factory=now_iso)
    updated_at: Optional[str] = field(default_factory=now_iso)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        # Filter to allowed fields to avoid TypeError
        allowed = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in allowed}
        # Provide defaults for missing fields if needed
        if "created_at" not in filtered:
            filtered["created_at"] = now_iso()
        if "updated_at" not in filtered:
            filtered["updated_at"] = now_iso()
        return cls(**filtered)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class JobDB:
    def __init__(self, path: str = DB_DEFAULT):
        self.path = path
        self._ensure_schema()

    def _connect(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = dict_factory
        return conn

    def _ensure_schema(self):
        conn = self._connect()
        cur = conn.cursor()

        # jobs table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            timeout REAL,
            run_at TEXT,
            output TEXT,
            last_run_at TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """)

        # job_runs for logging each run (optional/used by metrics)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            started_at TEXT,
            finished_at TEXT,
            success INTEGER,
            output TEXT
        );
        """)

        # config table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)

        conn.commit()
        conn.close()

    # ---------------- basic job ops ----------------
    def insert_job(self, job: Job):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO jobs
          (id, command, state, attempts, max_retries, timeout, run_at, output, last_run_at, created_at, updated_at)
        VALUES
          (:id, :command, :state, :attempts, :max_retries, :timeout, :run_at, :output, :last_run_at, :created_at, :updated_at)
        """, job.to_dict())
        conn.commit()
        conn.close()

    def list_jobs(self, state: Optional[str] = None, limit: int = 100) -> List[Job]:
        conn = self._connect()
        cur = conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at DESC LIMIT ?", (state, limit))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return [Job.from_dict(r) for r in rows]

    def fetch_pending_job_for_update(self) -> Optional[Job]:
        """
        Try to atomically pick a pending job and mark it processing.
        Uses a simple UPDATE ... WHERE state='pending' ... and returns that job.
        This reduces duplicate selection (best-effort locking).
        """
        conn = self._connect()
        cur = conn.cursor()
        # Pick one pending job (oldest)
        cur.execute("SELECT id FROM jobs WHERE state='pending' ORDER BY created_at ASC LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.close()
            return None
        job_id = row["id"]
        # Update to processing and set updated_at
        cur.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'", (now_iso(), job_id))
        conn.commit()
        # Return job
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job_row = cur.fetchone()
        conn.close()
        if not job_row:
            return None
        return Job.from_dict(job_row)

    def update_job_state(self, job_id: str, new_state: str, attempts: Optional[int] = None, output: Optional[str] = None):
        conn = self._connect()
        cur = conn.cursor()
        fields = ["state = ?", "updated_at = ?"]
        vals = [new_state, now_iso()]
        if attempts is not None:
            fields.append("attempts = ?")
            vals.append(attempts)
        if output is not None:
            fields.append("output = ?")
            vals.append(output)
        vals.append(job_id)
        cur.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?", vals)
        conn.commit()
        conn.close()

    def record_job_run(self, job_id: str, started_at: str, finished_at: str, success: int, output: Optional[str]):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO job_runs (job_id, started_at, finished_at, success, output) VALUES (?, ?, ?, ?, ?)",
                    (job_id, started_at, finished_at, success, output))
        # update last_run_at for job
        cur.execute("UPDATE jobs SET last_run_at=?, updated_at=? WHERE id=?", (finished_at, now_iso(), job_id))
        conn.commit()
        conn.close()

    def retry_dead_job(self, job_id: str) -> bool:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE id=? AND state='dead'", (job_id,))
        ok = cur.fetchone()
        if not ok:
            conn.close()
            return False
        cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=? WHERE id=?", (now_iso(), job_id))
        conn.commit()
        conn.close()
        return True

    # ---------------- config ----------------
    def set_config(self, key: str, value: str):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        conn.close()

    def get_config(self, key: str) -> Optional[str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT value FROM config WHERE key=?", (key,))
        r = cur.fetchone()
        conn.close()
        return r["value"] if r else None

    # ---------------- metrics / helpers ----------------
    def counts_by_state(self) -> Dict[str, int]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state")
        rows = cur.fetchall()
        conn.close()
        return {r["state"]: r["cnt"] for r in rows}

    def list_job_runs(self, job_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self._connect()
        cur = conn.cursor()
        if job_id:
            cur.execute("SELECT * FROM job_runs WHERE job_id=? ORDER BY started_at DESC LIMIT ?", (job_id, limit))
        else:
            cur.execute("SELECT * FROM job_runs ORDER BY started_at DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return rows

    def get_metrics_summary(self, recent_runs: int = 50) -> Dict[str, Any]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total_jobs FROM jobs")
        total_jobs = cur.fetchone()["total_jobs"]
        cur.execute("SELECT COUNT(*) AS success FROM job_runs WHERE success=1")
        success_runs = cur.fetchone()["success"]
        cur.execute("SELECT COUNT(*) AS failed FROM job_runs WHERE success=0")
        failed_runs = cur.fetchone()["failed"]
        conn.close()
        avg_attempts = None
        # compute avg attempts safely
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT AVG(attempts) as avg_att FROM jobs")
            v = cur.fetchone()
            avg_attempts = v["avg_att"] if v else None
            conn.close()
        except Exception:
            avg_attempts = None

        return {
            "total_jobs": total_jobs,
            "success_runs": success_runs,
            "failed_runs": failed_runs,
            "avg_attempts_per_job": avg_attempts
        }
