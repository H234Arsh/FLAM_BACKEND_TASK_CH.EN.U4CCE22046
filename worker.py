import os
import time
import subprocess
from datetime import datetime
from threading import Thread, Event
from db import JobDB
from utils import now_iso


class Worker(Thread):
    def __init__(self, db_path, poll_interval=1.0):
        super().__init__()
        self.db = JobDB(db_path)
        self.poll_interval = poll_interval
        self.running = True

    def run(self):
        while self.running:
            jobs = self.db.list_jobs(state="pending", limit=1)
            if not jobs:
                time.sleep(self.poll_interval)
                continue

            job = jobs[0]
            print(f"⚙️ Worker picked job {job.id}: {job.command}")
            self.db.update_job_state(job.id, "processing")
            start_time = now_iso()
            success = False
            output = ""

            try:
                result = subprocess.run(
                    job.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=job.timeout or 60,
                )
                output = result.stdout + result.stderr
                success = result.returncode == 0
            except Exception as e:
                output = str(e)

            self.db.record_job_run(job.id, start_time, now_iso(), int(success), output)
            new_state = "completed" if success else "failed"

            if not success:
                job.attempts += 1
                if job.attempts >= job.max_retries:
                    new_state = "dead"

            self.db.update_job_state(job.id, new_state)

        print("🛑 Worker stopped gracefully.")

    def stop(self):
        self.running = False


class WorkerManager:
    def __init__(self, db_path, worker_count=1, poll_interval=1.0):
        self.db_path = db_path
        self.worker_count = worker_count
        self.poll_interval = poll_interval
        self.workers = []
        self.stop_event = Event()

    def poll_jobs(self):
        """Continuously runs workers that pick up jobs."""
        # Remove any stopped workers
        self.workers = [w for w in self.workers if w.is_alive()]

        # Start new workers if needed
        while len(self.workers) < self.worker_count and not self.stop_event.is_set():
            w = Worker(self.db_path, poll_interval=self.poll_interval)
            w.daemon = True
            w.start()
            self.workers.append(w)
            print(f"🚀 Started worker thread ({len(self.workers)}/{self.worker_count})")

        # Wait a bit before next poll
        time.sleep(self.poll_interval)

    def start(self):
        print(f"👷 Starting {self.worker_count} worker(s)... Press Ctrl+C to stop.")
        try:
            while not self.stop_event.is_set():
                self.poll_jobs()
        except KeyboardInterrupt:
            print("🛑 Ctrl+C detected, stopping workers...")
            self.stop()
        self.stop_all()

    def stop(self):
        self.stop_event.set()

    def stop_all(self):
        print("🧹 Gracefully stopping all workers...")
        for w in self.workers:
            w.stop()
        for w in self.workers:
            w.join()
        print("✅ All workers stopped cleanly.")
