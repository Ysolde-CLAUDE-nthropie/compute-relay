#!/usr/bin/env python3
"""compute-relay coordinator.

A small self-hosted coordinator for a pool of compute workers you own or are
authorized to manage. Workers connect out to this server (outbound HTTPS only,
so they work behind NAT), authenticate with a shared token, and pull jobs to
run. You submit jobs with the bundled client.

Design goals: single-file, standard-library only, minimal configuration.

This is ordinary distributed-task-runner infrastructure (think a tiny
self-hosted CI runner pool). It is NOT covert: workers log what they run, the
server requires authentication, and there is no hiding, persistence trickery,
or self-propagation. Only run workers against a coordinator you trust, and only
enroll machines you own or have explicit permission to use.
"""

import json
import os
import secrets
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
TOKENS_FILE = os.environ.get("RELAY_TOKENS_FILE", os.path.join(HERE, "relay-tokens.json"))

# How long a worker's poll request blocks waiting for work before we answer
# "nothing yet" and let it poll again.
POLL_WAIT_SECONDS = 25
# A worker is considered offline if we have not heard from it in this long.
WORKER_TIMEOUT_SECONDS = 90


class State:
    """In-memory state for workers and jobs, guarded by a condition variable."""

    def __init__(self):
        self.cond = threading.Condition()
        self.workers = {}          # worker_id -> info dict
        self.jobs = {}             # job_id -> job dict
        self.pending = []          # list of job_ids waiting to be claimed

    # --- workers -------------------------------------------------------
    def register_worker(self, name, caps):
        with self.cond:
            worker_id = uuid.uuid4().hex[:12]
            self.workers[worker_id] = {
                "id": worker_id,
                "name": name,
                "caps": caps,
                "registered_at": time.time(),
                "last_seen": time.time(),
                "current_job": None,
            }
            return worker_id

    def touch_worker(self, worker_id):
        w = self.workers.get(worker_id)
        if w:
            w["last_seen"] = time.time()

    def worker_online(self, w):
        return (time.time() - w["last_seen"]) <= WORKER_TIMEOUT_SECONDS

    # --- jobs ----------------------------------------------------------
    def submit_job(self, command, shell, cwd, env, timeout, target, label):
        with self.cond:
            job_id = uuid.uuid4().hex[:12]
            self.jobs[job_id] = {
                "id": job_id,
                "label": label,
                "command": command,
                "shell": shell,
                "cwd": cwd,
                "env": env,
                "timeout": timeout,
                "target": target,           # worker name/id to pin to, or None
                "status": "pending",        # pending|running|done|error
                "worker_id": None,
                "output": "",
                "exit_code": None,
                "created_at": time.time(),
                "started_at": None,
                "finished_at": None,
            }
            self.pending.append(job_id)
            self.cond.notify_all()
            return job_id

    def claim_job(self, worker_id):
        """Pop the first pending job this worker is eligible to run."""
        w = self.workers.get(worker_id)
        if not w:
            return None
        for i, job_id in enumerate(self.pending):
            job = self.jobs.get(job_id)
            if not job:
                continue
            target = job.get("target")
            if target and target not in (worker_id, w["name"]):
                continue
            del self.pending[i]
            job["status"] = "running"
            job["worker_id"] = worker_id
            job["started_at"] = time.time()
            w["current_job"] = job_id
            return job
        return None


STATE = State()
TOKENS = {}


def load_or_create_tokens():
    worker_tok = os.environ.get("RELAY_WORKER_TOKEN")
    admin_tok = os.environ.get("RELAY_ADMIN_TOKEN")
    if worker_tok and admin_tok:
        return {"worker": worker_tok, "admin": admin_tok}

    # Fall back to a persisted token file so restarts keep the same tokens.
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE) as f:
                saved = json.load(f)
            return {
                "worker": worker_tok or saved["worker"],
                "admin": admin_tok or saved["admin"],
            }
        except (OSError, KeyError, json.JSONDecodeError):
            pass

    toks = {
        "worker": worker_tok or secrets.token_urlsafe(24),
        "admin": admin_tok or secrets.token_urlsafe(24),
    }
    try:
        with open(TOKENS_FILE, "w") as f:
            json.dump(toks, f)
        os.chmod(TOKENS_FILE, 0o600)
    except OSError:
        pass
    return toks


def bearer(headers):
    auth = headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return None


class Handler(BaseHTTPRequestHandler):
    server_version = "compute-relay/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Keep the access log quiet; we log meaningful events ourselves.
        pass

    # --- helpers -------------------------------------------------------
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, code, text, ctype="text/plain; charset=utf-8"):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _auth(self, role):
        tok = bearer(self.headers)
        if tok and secrets.compare_digest(tok, TOKENS[role]):
            return True
        self._send_json(401, {"error": "unauthorized"})
        return False

    # --- routing -------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/":
            return self._send_text(200, HELP_PAGE)
        if path == "/healthz":
            return self._send_json(200, {"ok": True})
        if path in ("/install.sh", "/agent.py"):
            return self._serve_bootstrap(path)
        if path == "/api/poll":
            return self._handle_poll(qs)
        if path == "/api/workers":
            return self._handle_workers()
        if path.startswith("/api/jobs/"):
            return self._handle_job_status(path.rsplit("/", 1)[-1])
        if path == "/api/jobs":
            return self._handle_jobs_list()
        return self._send_json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/register":
            return self._handle_register()
        if path == "/api/result":
            return self._handle_result()
        if path == "/api/jobs":
            return self._handle_submit()
        return self._send_json(404, {"error": "not found"})

    # --- bootstrap files ----------------------------------------------
    def _serve_bootstrap(self, path):
        fname = path.lstrip("/")
        full = os.path.join(HERE, fname)
        if not os.path.exists(full):
            return self._send_json(404, {"error": "not found"})
        with open(full, "rb") as f:
            body = f.read()
        ctype = "text/x-shellscript" if fname.endswith(".sh") else "text/x-python"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # --- worker endpoints ---------------------------------------------
    def _handle_register(self):
        if not self._auth("worker"):
            return
        data = self._read_json()
        name = str(data.get("name") or "worker")[:64]
        caps = data.get("caps") or {}
        worker_id = STATE.register_worker(name, caps)
        print(f"[register] worker '{name}' id={worker_id} caps={caps}", flush=True)
        return self._send_json(200, {"worker_id": worker_id,
                                     "poll_wait": POLL_WAIT_SECONDS})

    def _handle_poll(self, qs):
        if not self._auth("worker"):
            return
        worker_id = (qs.get("worker_id") or [""])[0]
        with STATE.cond:
            if worker_id not in STATE.workers:
                return self._send_json(409, {"error": "unknown worker, re-register"})
            STATE.touch_worker(worker_id)
            deadline = time.time() + POLL_WAIT_SECONDS
            while True:
                job = STATE.claim_job(worker_id)
                if job:
                    print(f"[dispatch] job={job['id']} -> {worker_id} "
                          f"({job['label'] or job['command']!r})", flush=True)
                    return self._send_json(200, {"job": {
                        "id": job["id"],
                        "command": job["command"],
                        "shell": job["shell"],
                        "cwd": job["cwd"],
                        "env": job["env"],
                        "timeout": job["timeout"],
                    }})
                remaining = deadline - time.time()
                if remaining <= 0:
                    return self._send_json(204, {})
                STATE.cond.wait(remaining)

    def _handle_result(self):
        if not self._auth("worker"):
            return
        data = self._read_json()
        job_id = data.get("job_id")
        with STATE.cond:
            job = STATE.jobs.get(job_id)
            if not job:
                return self._send_json(404, {"error": "unknown job"})
            chunk = data.get("output_chunk")
            if chunk:
                job["output"] += chunk
            if data.get("done"):
                job["exit_code"] = data.get("exit_code")
                job["status"] = "error" if data.get("error") else "done"
                job["finished_at"] = time.time()
                w = STATE.workers.get(job["worker_id"])
                if w:
                    w["current_job"] = None
                print(f"[result] job={job_id} status={job['status']} "
                      f"exit={job['exit_code']}", flush=True)
            STATE.cond.notify_all()
        return self._send_json(200, {"ok": True})

    # --- admin endpoints ----------------------------------------------
    def _handle_submit(self):
        if not self._auth("admin"):
            return
        data = self._read_json()
        command = data.get("command")
        if not command:
            return self._send_json(400, {"error": "command required"})
        job_id = STATE.submit_job(
            command=command,
            shell=bool(data.get("shell", True)),
            cwd=data.get("cwd"),
            env=data.get("env") or {},
            timeout=data.get("timeout"),
            target=data.get("target"),
            label=data.get("label"),
        )
        print(f"[submit] job={job_id} target={data.get('target')} "
              f"cmd={command!r}", flush=True)
        return self._send_json(200, {"job_id": job_id})

    def _handle_job_status(self, job_id):
        if not self._auth("admin"):
            return
        job = STATE.jobs.get(job_id)
        if not job:
            return self._send_json(404, {"error": "unknown job"})
        return self._send_json(200, job)

    def _handle_jobs_list(self):
        if not self._auth("admin"):
            return
        jobs = sorted(STATE.jobs.values(), key=lambda j: j["created_at"], reverse=True)
        summary = [{k: j[k] for k in ("id", "label", "status", "worker_id",
                                      "exit_code", "created_at")} for j in jobs]
        return self._send_json(200, {"jobs": summary})

    def _handle_workers(self):
        if not self._auth("admin"):
            return
        with STATE.cond:
            out = []
            for w in STATE.workers.values():
                out.append({
                    "id": w["id"],
                    "name": w["name"],
                    "caps": w["caps"],
                    "online": STATE.worker_online(w),
                    "last_seen": w["last_seen"],
                    "current_job": w["current_job"],
                })
        return self._send_json(200, {"workers": out})


HELP_PAGE = """compute-relay coordinator is running.

This is a self-hosted distributed task runner. Worker machines you own or manage
connect here, authenticate, and pull compute jobs. See the repository README for
usage. Endpoints:

  GET  /healthz            liveness check
  GET  /install.sh         worker bootstrap script
  GET  /agent.py           worker agent source
  POST /api/register       worker enrollment     (worker token)
  GET  /api/poll           worker long-poll      (worker token)
  POST /api/result         worker result upload  (worker token)
  POST /api/jobs           submit a job          (admin token)
  GET  /api/jobs           list jobs             (admin token)
  GET  /api/jobs/<id>      job status + output   (admin token)
  GET  /api/workers        list workers          (admin token)
"""


def main():
    global TOKENS
    TOKENS = load_or_create_tokens()
    host = os.environ.get("RELAY_HOST", "0.0.0.0")
    port = int(os.environ.get("RELAY_PORT", os.environ.get("PORT", "8787")))

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True

    print("=" * 64, flush=True)
    print(f"compute-relay coordinator listening on {host}:{port}", flush=True)
    print(f"  worker token: {TOKENS['worker']}", flush=True)
    print(f"  admin  token: {TOKENS['admin']}", flush=True)
    print("Give the worker token (with your server URL) to machines you want to", flush=True)
    print("enroll. Keep the admin token private: it can submit jobs.", flush=True)
    print("=" * 64, flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down", flush=True)
        httpd.shutdown()


if __name__ == "__main__":
    main()
