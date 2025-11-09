#!/usr/bin/env python3
"""
queuectl - CLI entry point (uses JobDB)
"""

import json
import sys
from datetime import datetime
import click

from db import Job, JobDB, now_iso
from utils import new_id, parse_iso

DB_PATH = "queue.db"
DEFAULT_CONFIG = {"max_retries": 3, "base_backoff": 2}


@click.group()
def cli():
    pass


# ---------------- ENQUEUE ----------------
@cli.command(help="Enqueue a job. Pass a JSON string or use --file <path>")
@click.argument("job_json", required=False)
@click.option("--file", "-f", type=click.Path(exists=True), help="JSON file containing job object")
def enqueue(job_json, file):
    db = JobDB(DB_PATH)
    raw = None
    if file:
        with open(file, "r") as fh:
            raw = fh.read()
    elif job_json:
        raw = job_json
    else:
        click.echo("Provide job JSON as argument or use --file")
        sys.exit(2)

    try:
        payload = json.loads(raw)
    except Exception as e:
        click.echo(f"Invalid JSON: {e}")
        sys.exit(2)

    # defaults
    if "id" not in payload:
        payload["id"] = new_id()
    if "command" not in payload:
        click.echo("Job must include 'command' field")
        sys.exit(2)

    payload.setdefault("state", "pending")
    payload.setdefault("attempts", 0)
    payload.setdefault("max_retries", int(db.get_config("max_retries") or DEFAULT_CONFIG["max_retries"]))
    payload.setdefault("timeout", None)

    if payload.get("run_at"):
        try:
            payload["run_at"] = parse_iso(payload["run_at"]).isoformat()
        except Exception as e:
            click.echo(f"Invalid run_at format: {e}")
            sys.exit(2)

    job = Job.from_dict(payload)
    db.insert_job(job)
    click.echo(f"✅ Enqueued job {job.id}")


# --------------- WORKER MANAGEMENT ---------------
@cli.group(help="Worker management")
def worker():
    pass


@worker.command("start", help="Start worker(s) (spawns threads managed in worker.py)")
@click.option("--count", "-c", default=1, help="Number of workers")
@click.option("--poll-interval", default=1.0, help="Poll interval seconds")
def worker_start(count, poll_interval):
    from worker import WorkerManager
    manager = WorkerManager(DB_PATH, worker_count=count, poll_interval=poll_interval)
    click.echo(f"Starting {count} worker(s). Press Ctrl+C to stop gracefully.")
    manager.start()


@worker.command("stop", help="Stop running workers (if running in background manager mode)")
def worker_stop():
    # left as a convenience; WorkerManager.stop_all can be called via IPC or other mechanism
    from worker import WorkerManager
    manager = WorkerManager(DB_PATH)
    click.echo("Stopping all workers gracefully...")
    manager.stop_all()
    click.echo("✅ Workers stopped.")


# ---------------- STATUS ----------------
@cli.command(help="Show job counts and metrics summary")
def status():
    db = JobDB(DB_PATH)
    counts = db.counts_by_state()
    click.echo("📊 Job counts by state:")
    for s in ("pending", "processing", "completed", "failed", "dead"):
        click.echo(f"  {s}: {counts.get(s, 0)}")
    m = db.get_metrics_summary()
    click.echo("\n📈 Metrics:")
    for k, v in m.items():
        click.echo(f"  {k}: {v}")


# ---------------- LIST ----------------
@cli.command("list", help="List jobs (optionally filter by state)")
@click.option("--state", default=None, help="Filter by state")
@click.option("--limit", default=100)
def list_cmd(state, limit):
    db = JobDB(DB_PATH)
    jobs = db.list_jobs(state=state, limit=limit)
    for j in jobs:
        click.echo(json.dumps(j.to_dict(), default=str))


# ---------------- DLQ ----------------
@cli.group(help="Dead Letter Queue operations")
def dlq():
    pass


@dlq.command("list", help="List DLQ (dead) jobs")
@click.option("--limit", default=100)
def dlq_list(limit):
    db = JobDB(DB_PATH)
    jobs = db.list_jobs(state="dead", limit=limit)
    for j in jobs:
        click.echo(json.dumps(j.to_dict(), default=str))


@dlq.command("retry", help="Retry a job from DLQ (move to pending)")
@click.argument("job_id")
def dlq_retry(job_id):
    db = JobDB(DB_PATH)
    ok = db.retry_dead_job(job_id)
    if ok:
        click.echo(f"🔄 Moved job {job_id} from dead → pending")
    else:
        click.echo(f"❌ Job {job_id} not found in DLQ")


# ---------------- CONFIG ----------------
@cli.group(help="Config management")
def config():
    pass


@config.command("set", help="Set config key")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    db = JobDB(DB_PATH)
    db.set_config(key, value)
    click.echo(f"⚙️ Set {key} = {value}")


@config.command("get", help="Get config key")
@click.argument("key")
def config_get(key):
    db = JobDB(DB_PATH)
    v = db.get_config(key)
    if v is None and key in DEFAULT_CONFIG:
        v = DEFAULT_CONFIG[key]
        db.set_config(key, v)
    click.echo(f"{key} = {v}")


# ---------------- RUNS ----------------
@cli.group(help="Job run logs")
def runs():
    pass


@runs.command("list", help="List job runs (optionally filter by job_id)")
@click.option("--job", "job_id", default=None)
@click.option("--limit", default=50)
def runs_list(job_id, limit):
    db = JobDB(DB_PATH)
    rows = db.list_job_runs(job_id=job_id, limit=limit)
    for r in rows:
        click.echo(json.dumps(r, default=str))


# ---------------- METRICS ----------------
@cli.group(help="Metrics")
def metrics():
    pass


@metrics.command("show", help="Show metrics summary")
@click.option("--recent", default=50)
def metrics_show(recent):
    db = JobDB(DB_PATH)
    m = db.get_metrics_summary(recent_runs=recent)
    click.echo(json.dumps(m, indent=2, default=str))


if __name__ == "__main__":
    cli()
