from flask import Flask, render_template_string, jsonify
import sqlite3
from utils import now_iso

DB_FILE = "queue.db"
app = Flask(__name__)

# --------------------------------------------------------------------
# 🧩 Auto Schema Upgrade - fixes all missing columns automatically
# --------------------------------------------------------------------
def ensure_schema():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(jobs)")
    existing = [r[1] for r in cur.fetchall()]

    # Required columns for display & operations
    required_cols = {
        "retries": "INTEGER DEFAULT 0",
        "last_run_at": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
        "attempts": "INTEGER DEFAULT 0",
        "state": "TEXT DEFAULT 'pending'",
        "command": "TEXT",
    }

    for col, coltype in required_cols.items():
        if col not in existing:
            print(f"⚙️ Adding missing column: {col}")
            cur.execute(f"ALTER TABLE jobs ADD COLUMN {col} {coltype}")

    conn.commit()
    conn.close()

# --------------------------------------------------------------------
# 📊 Fetch job & metric data
# --------------------------------------------------------------------
def fetch_data():
    ensure_schema()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Fetch jobs
    cur.execute("""
        SELECT id, command, state, attempts, max_retries, created_at, updated_at
        FROM jobs
        ORDER BY created_at DESC LIMIT 100
    """)
    jobs = [dict(r) for r in cur.fetchall()]

    # Fetch summary
    cur.execute("SELECT state, COUNT(*) AS count FROM jobs GROUP BY state")
    summary = {row["state"]: row["count"] for row in cur.fetchall()}

    # Metrics
    cur.execute("SELECT COUNT(*) FROM job_runs WHERE success=1")
    success = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM job_runs WHERE success=0")
    failed = cur.fetchone()[0]

    conn.close()
    metrics = {
        "total_jobs": sum(summary.values()),
        "success_runs": success,
        "failed_runs": failed,
        "timestamp": now_iso(),
    }
    return jobs, summary, metrics

# --------------------------------------------------------------------
# 🌐 Web Routes
# --------------------------------------------------------------------
@app.route("/")
def dashboard():
    jobs, summary, metrics = fetch_data()
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Job Queue Dashboard</title>
        <style>
            body { font-family: Arial; background: #f6f8fa; margin: 20px; color: #333; }
            h1 { color: #222; }
            table { border-collapse: collapse; width: 100%; margin-top: 10px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; }
            tr:nth-child(even){background-color: #f2f2f2;}
            .summary-box { display: flex; gap: 1rem; margin: 1rem 0; }
            .card { background: white; border-radius: 8px; padding: 1rem; box-shadow: 0 0 4px rgba(0,0,0,0.1); }
            .footer { font-size: 0.8em; color: gray; text-align: center; margin-top: 20px; }
        </style>
    </head>
    <body>
        <h1>🧭 Job Queue Dashboard</h1>

        <div class="summary-box">
            {% for k, v in summary.items() %}
                <div class="card">
                    <b>{{k.capitalize()}}</b><br>{{v}}
                </div>
            {% endfor %}
        </div>

        <div class="summary-box">
            <div class="card"><b>Total Jobs:</b> {{metrics.total_jobs}}</div>
            <div class="card"><b>Success Runs:</b> {{metrics.success_runs}}</div>
            <div class="card"><b>Failed Runs:</b> {{metrics.failed_runs}}</div>
            <div class="card"><b>Updated:</b> {{metrics.timestamp}}</div>
        </div>

        <h2>📋 Recent Jobs</h2>
        <table>
            <tr>
                <th>ID</th><th>Command</th><th>State</th><th>Attempts</th><th>Max Retries</th><th>Created</th><th>Updated</th>
            </tr>
            {% for j in jobs %}
            <tr>
                <td>{{j.id}}</td>
                <td>{{j.command}}</td>
                <td>{{j.state}}</td>
                <td>{{j.attempts}}</td>
                <td>{{j.max_retries}}</td>
                <td>{{j.created_at}}</td>
                <td>{{j.updated_at}}</td>
            </tr>
            {% endfor %}
        </table>

        <div class="footer">
            <p>🚀 Flask Dashboard for Job Queue — Auto-updating DB schema ensured.</p>
        </div>
    </body>
    </html>
    """, jobs=jobs, summary=summary, metrics=metrics)


@app.route("/api/jobs")
def api_jobs():
    jobs, summary, metrics = fetch_data()
    return jsonify({"jobs": jobs, "summary": summary, "metrics": metrics})


# --------------------------------------------------------------------
# 🏁 Run Flask app
# --------------------------------------------------------------------
if __name__ == "__main__":
    ensure_schema()
    print("✅ Dashboard running at http://127.0.0.1:5000/")
    app.run(debug=True)
