#!/usr/bin/env python3
"""compute-relay coordinator.

A small self-hosted coordinator for a pool of compute workers you own or are
authorized to manage. Workers connect out to this server (outbound HTTPS only,
so they work behind NAT), authenticate with a shared token, and pull work. You
submit work with the bundled client.

It supports three kinds of work:
  - one-shot jobs   : run a command, stream text output, report exit code
  - sessions        : a long-lived PTY-backed process on a chosen worker that
                      you feed input to and read output from (e.g. adb shell,
                      an interactive debugger) -- stateful across commands
  - artifacts       : binary files pulled back from a worker (screenshots,
                      screen recordings), uploaded in chunks

Design goals: single-file, standard-library only, minimal configuration.

This is ordinary distributed-task-runner infrastructure (think a tiny
self-hosted CI runner pool). It is NOT covert: workers log what they run, the
server requires authentication, and there is no hiding, persistence trickery,
or self-propagation. Only run workers against a coordinator you trust, and only
enroll machines you own or have explicit permission to use.
"""

import base64
import collections
import hashlib
import json
import os
import re
import secrets
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.cookies import CookieError, SimpleCookie
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
TOKENS_FILE = os.environ.get("RELAY_TOKENS_FILE", os.path.join(HERE, "relay-tokens.json"))
UI_FILE = os.path.join(HERE, "ui.html")

# How long a worker's poll request blocks waiting for work before we answer
# "nothing yet" and let it poll again.
POLL_WAIT_SECONDS = 25
# A worker is considered offline if we have not heard from it in this long.
WORKER_TIMEOUT_SECONDS = 90
# Upper bound on a single artifact held in memory (bytes).
MAX_ARTIFACT_BYTES = int(os.environ.get("RELAY_MAX_ARTIFACT_BYTES", str(512 * 1024 * 1024)))
# Web dashboard sign-in: a one-time link (/ui#login=<token>) is exchanged for a
# session cookie. Links are single-use and short-lived; sessions last longer.
UI_LINK_TTL = int(os.environ.get("RELAY_UI_LINK_TTL", "600"))
UI_SESSION_TTL = int(os.environ.get("RELAY_UI_SESSION_TTL", str(7 * 24 * 3600)))
UI_COOKIE = "relay_ui"
# Cookie-authenticated requests must also carry this header. Cross-site forms
# cannot set custom headers, so this blocks CSRF even where SameSite doesn't.
UI_HEADER = "X-Relay-UI"
# Base URL advertised to browsers and enroll scripts. Empty means "derive it
# from the bind address", which is only right for local use; behind a proxy set
# RELAY_PUBLIC_URL=https://relay.example.com.
PUBLIC_URL = os.environ.get("RELAY_PUBLIC_URL", "").rstrip("/")


class State:
    """In-memory state, guarded by a single condition variable."""

    def __init__(self):
        self.cond = threading.Condition()
        self.workers = {}          # worker_id -> info dict (has a control deque)
        self.jobs = {}             # job_id -> job dict
        self.pending = []          # job_ids waiting to be claimed
        self.sessions = {}         # session_id -> session dict
        self.artifacts = {}        # artifact_id -> artifact dict
        self.ui_links = {}         # one-time dashboard sign-in token -> expiry
        self.ui_sessions = {}      # dashboard session id (cookie) -> expiry

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
                "control": collections.deque(),
            }
            return worker_id

    def touch_worker(self, worker_id):
        w = self.workers.get(worker_id)
        if w:
            w["last_seen"] = time.time()

    def worker_online(self, w):
        return (time.time() - w["last_seen"]) <= WORKER_TIMEOUT_SECONDS

    def find_worker(self, target):
        """Resolve a worker by id or name, preferring an online match."""
        match = None
        for w in self.workers.values():
            if target in (w["id"], w["name"]):
                if self.worker_online(w):
                    return w
                match = match or w
        return match

    def enqueue_control(self, worker_id, msg):
        w = self.workers.get(worker_id)
        if not w:
            return False
        w["control"].append(msg)
        self.cond.notify_all()
        return True

    # --- jobs ----------------------------------------------------------
    def submit_job(self, **kw):
        with self.cond:
            job_id = uuid.uuid4().hex[:12]
            self.jobs[job_id] = {
                "id": job_id,
                "label": kw.get("label"),
                "command": kw["command"],
                "shell": kw.get("shell", True),
                "cwd": kw.get("cwd"),
                "env": kw.get("env") or {},
                "timeout": kw.get("timeout"),
                "target": kw.get("target"),
                "binary_stdout": bool(kw.get("binary_stdout")),
                "binary_out_name": kw.get("binary_out_name"),
                "binary_out_type": kw.get("binary_out_type"),
                "upload_paths": kw.get("upload_paths") or [],
                "status": "pending",
                "worker_id": None,
                "output": "",
                "exit_code": None,
                "artifacts": [],
                "created_at": time.time(),
                "started_at": None,
                "finished_at": None,
            }
            self.pending.append(job_id)
            self.cond.notify_all()
            return job_id

    def claim_job(self, worker_id):
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

    # --- sessions ------------------------------------------------------
    def create_session(self, worker_id, command, shell, cwd, env):
        with self.cond:
            session_id = uuid.uuid4().hex[:12]
            self.sessions[session_id] = {
                "id": session_id,
                "worker_id": worker_id,
                "command": command,
                "shell": shell,
                "cwd": cwd,
                "env": env or {},
                "status": "starting",   # starting|open|closed|error
                "output": "",
                "exit_code": None,
                "created_at": time.time(),
            }
            return session_id

    # --- artifacts -----------------------------------------------------
    def create_artifact(self, name, content_type, job_id=None, session_id=None,
                        worker=None, status="uploading"):
        with self.cond:
            artifact_id = uuid.uuid4().hex[:12]
            self.artifacts[artifact_id] = {
                "id": artifact_id,
                "name": name or "artifact.bin",
                "content_type": content_type or "application/octet-stream",
                "size": 0,
                "status": status,       # pending|uploading|complete|error
                "data": bytearray(),
                "job_id": job_id,
                "session_id": session_id,
                "worker": worker,
                "error": None,
                "created_at": time.time(),
            }
            if job_id and job_id in self.jobs:
                self.jobs[job_id]["artifacts"].append(artifact_id)
            return artifact_id

    @staticmethod
    def artifact_meta(a):
        return {k: a[k] for k in ("id", "name", "content_type", "size",
                                  "status", "job_id", "session_id", "worker",
                                  "error", "created_at")}


    # --- dashboard sign-in ---------------------------------------------
    def mint_ui_link(self):
        with self.cond:
            now = time.time()
            for tok in [t for t, exp in self.ui_links.items() if exp < now]:
                del self.ui_links[tok]
            tok = secrets.token_urlsafe(32)
            self.ui_links[tok] = now + UI_LINK_TTL
            return tok

    def redeem_ui_link(self, tok):
        """Trade a sign-in link token for a new session id (or None)."""
        with self.cond:
            exp = self.ui_links.pop(tok, None)   # single use, even if expired
            if not exp or exp < time.time():
                return None
            now = time.time()
            for sid in [s for s, e in self.ui_sessions.items() if e < now]:
                del self.ui_sessions[sid]
            sid = secrets.token_urlsafe(32)
            self.ui_sessions[sid] = now + UI_SESSION_TTL
            return sid

    def ui_session_valid(self, sid):
        with self.cond:
            exp = self.ui_sessions.get(sid) if sid else None
            if exp and exp >= time.time():
                return True
            self.ui_sessions.pop(sid, None)
            return False

    def end_ui_session(self, sid):
        with self.cond:
            self.ui_sessions.pop(sid, None)


STATE = State()
TOKENS = {}


def load_or_create_tokens():
    worker_tok = os.environ.get("RELAY_WORKER_TOKEN")
    admin_tok = os.environ.get("RELAY_ADMIN_TOKEN")
    if worker_tok and admin_tok:
        return {"worker": worker_tok, "admin": admin_tok}
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE) as f:
                saved = json.load(f)
            return {"worker": worker_tok or saved["worker"],
                    "admin": admin_tok or saved["admin"]}
        except (OSError, KeyError, json.JSONDecodeError):
            pass
    toks = {"worker": worker_tok or secrets.token_urlsafe(24),
            "admin": admin_tok or secrets.token_urlsafe(24)}
    try:
        with open(TOKENS_FILE, "w") as f:
            json.dump(toks, f)
        os.chmod(TOKENS_FILE, 0o600)
    except OSError:
        pass
    return toks


def bearer(headers):
    auth = headers.get("Authorization", "")
    return auth[7:].strip() if auth.startswith("Bearer ") else None


class Handler(BaseHTTPRequestHandler):
    server_version = "compute-relay/2.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    # --- io helpers ----------------------------------------------------
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _read_json(self):
        raw = self._read_body()
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return {}

    def _cookie(self, name):
        jar = SimpleCookie()
        try:
            jar.load(self.headers.get("Cookie", ""))
        except CookieError:
            return None
        return jar[name].value if name in jar else None

    def _auth(self, role):
        tok = bearer(self.headers)
        if tok and secrets.compare_digest(tok, TOKENS[role]):
            return True
        # The dashboard authenticates with its session cookie instead.
        if (role == "admin" and self.headers.get(UI_HEADER) == "1"
                and STATE.ui_session_valid(self._cookie(UI_COOKIE))):
            return True
        self._read_body()  # drain so keep-alive stays sane
        self._send_json(401, {"error": "unauthorized"})
        return False

    # --- routing -------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        if path == "/":
            return self._send_bytes(200, HELP_PAGE.encode(), "text/plain; charset=utf-8")
        if path == "/healthz":
            return self._send_json(200, {"ok": True})
        if path in ("/install.sh", "/agent.py"):
            return self._serve_bootstrap(path)
        if path in ("/ui", "/ui/"):
            return self._serve_ui()
        if path == "/api/poll":
            return self._handle_poll(qs)
        if path == "/api/workers":
            return self._handle_workers()
        if path == "/api/enroll":
            return self._handle_enroll()
        if path == "/api/jobs":
            return self._handle_jobs_list()
        if path.startswith("/api/jobs/"):
            return self._handle_job_status(path.rsplit("/", 1)[-1])
        if path == "/api/sessions":
            return self._handle_sessions_list()
        if path.startswith("/api/sessions/"):
            return self._handle_session_get(path.rsplit("/", 1)[-1], qs)
        if path == "/api/artifacts":
            return self._handle_artifacts_list()
        if path.startswith("/api/artifacts/"):
            return self._handle_artifact_get(path.rsplit("/", 1)[-1], qs)
        return self._send_json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        if path == "/api/register":
            return self._handle_register()
        if path == "/api/ui-link":
            return self._handle_ui_link()
        if path == "/api/session":
            return self._handle_ui_login()
        if path == "/api/logout":
            return self._handle_ui_logout()
        if path == "/api/result":
            return self._handle_result()
        if path == "/api/jobs":
            return self._handle_submit()
        if path == "/api/sessions":
            return self._handle_session_create()
        if path.endswith("/input") and path.startswith("/api/sessions/"):
            return self._handle_session_input(path.split("/")[3])
        if path.endswith("/close") and path.startswith("/api/sessions/"):
            return self._handle_session_close(path.split("/")[3])
        if path == "/api/session_output":
            return self._handle_session_output()
        if path == "/api/pull":
            return self._handle_pull()
        if path == "/api/artifacts":
            return self._handle_artifact_create()
        if path.endswith("/chunk") and path.startswith("/api/artifacts/"):
            return self._handle_artifact_chunk(path.split("/")[3], qs)
        return self._send_json(404, {"error": "not found"})

    # --- bootstrap -----------------------------------------------------
    def _serve_bootstrap(self, path):
        full = os.path.join(HERE, path.lstrip("/"))
        if not os.path.exists(full):
            return self._send_json(404, {"error": "not found"})
        with open(full, "rb") as f:
            body = f.read()
        ctype = "text/x-shellscript" if full.endswith(".sh") else "text/x-python"
        self._send_bytes(200, body, ctype)

    # --- web dashboard -----------------------------------------------
    def _serve_ui(self):
        """Serve the admin dashboard. The page itself is public (it holds no
        secrets); every API call it makes needs the admin token. The CSP pins
        the page's single inline script by hash and only allows same-origin
        requests, so worker-reported text can't load or run anything else."""
        try:
            with open(UI_FILE, "rb") as f:
                body = f.read()
        except OSError:
            return self._send_json(404, {"error": "ui.html not found"})
        m = re.search(rb"<script>(.*?)</script>", body, re.S)
        script = ("'sha256-%s'" % base64.b64encode(hashlib.sha256(m.group(1)).digest()).decode()
                  if m else "'none'")
        csp = ("default-src 'none'; script-src %s; style-src 'unsafe-inline'; "
               "connect-src 'self'; img-src 'self' blob: data:; media-src blob:; "
               "base-uri 'none'; form-action 'none'; frame-ancestors 'none'" % script)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Security-Policy", csp)
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # Sign-in flow: an admin mints a one-time link (/ui#login=<token>); the
    # page POSTs that token to /api/session and gets an HttpOnly session
    # cookie. The token rides in the URL fragment, which browsers never send
    # to the server, so it stays out of proxy access logs and Referer headers.
    #
    # The cookie is set without a Path, so it defaults to the directory of
    # /api/session, i.e. /api (or <prefix>/api behind a reverse proxy that
    # mounts the coordinator under a sub-path). The page itself needs no
    # cookie.
    def _set_ui_cookie(self, value, max_age):
        attrs = [f"{UI_COOKIE}={value}", f"Max-Age={max_age}", "HttpOnly", "SameSite=Strict"]
        if self.headers.get("X-Forwarded-Proto", "").lower() == "https":
            attrs.append("Secure")
        self.send_header("Set-Cookie", "; ".join(attrs))

    def _handle_ui_link(self):
        if not self._auth("admin"):
            return
        self._read_body()
        tok = STATE.mint_ui_link()
        print("[ui] minted a one-time sign-in link", flush=True)
        self._send_json(200, {"login_token": tok, "path": f"ui#login={tok}",
                              "expires_in": UI_LINK_TTL})

    def _handle_enroll(self):
        """Return everything the dashboard needs to show a ready-to-paste enroll
        command: the public base URL and the worker token.

        The worker token is not a secret from the dashboard — it is the token
        handed to workers, and the dashboard itself sits behind an admin
        session. The *admin* token is never served here.
        """
        if not self._auth("admin"):
            return
        self._read_body()
        base = PUBLIC_URL or ("http://%s:%s" % (
            self.headers.get("Host", "127.0.0.1").rsplit(":", 1)[0],
            self.server.server_address[1]))
        self._send_json(200, {
            "server": base,
            "worker_token": TOKENS["worker"],
            "install_sh": base + "/install.sh",
            "agent_py": base + "/agent.py",
        })

    def _handle_ui_login(self):
        data = self._read_json()
        sid = STATE.redeem_ui_link(str(data.get("login") or ""))
        if not sid:
            return self._send_json(401, {"error": "sign-in link is invalid, expired, or already used"})
        print("[ui] dashboard session started", flush=True)
        body = json.dumps({"ok": True, "expires_in": UI_SESSION_TTL}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._set_ui_cookie(sid, UI_SESSION_TTL)
        self.end_headers()
        self.wfile.write(body)

    def _handle_ui_logout(self):
        self._read_body()
        if self.headers.get(UI_HEADER) != "1":
            return self._send_json(403, {"error": f"{UI_HEADER} header required"})
        STATE.end_ui_session(self._cookie(UI_COOKIE))
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._set_ui_cookie("", 0)
        self.end_headers()
        self.wfile.write(body)

    # --- worker: enroll + poll ----------------------------------------
    def _handle_register(self):
        if not self._auth("worker"):
            return
        data = self._read_json()
        name = str(data.get("name") or "worker")[:64]
        caps = data.get("caps") or {}
        worker_id = STATE.register_worker(name, caps)
        print(f"[register] worker '{name}' id={worker_id} caps={caps}", flush=True)
        self._send_json(200, {"worker_id": worker_id, "poll_wait": POLL_WAIT_SECONDS})

    def _handle_poll(self, qs):
        if not self._auth("worker"):
            return
        worker_id = (qs.get("worker_id") or [""])[0]
        with STATE.cond:
            if worker_id not in STATE.workers:
                return self._send_json(409, {"error": "unknown worker, re-register"})
            w = STATE.workers[worker_id]
            STATE.touch_worker(worker_id)
            deadline = time.time() + POLL_WAIT_SECONDS
            while True:
                controls = list(w["control"])
                w["control"].clear()
                job = STATE.claim_job(worker_id)
                if controls or job:
                    if job:
                        print(f"[dispatch] job={job['id']} -> {worker_id} "
                              f"({job['label'] or job['command']!r})", flush=True)
                    for c in controls:
                        print(f"[control] {c.get('type')} -> {worker_id}", flush=True)
                    payload = {"controls": controls, "job": None}
                    if job:
                        payload["job"] = {k: job[k] for k in (
                            "id", "command", "shell", "cwd", "env", "timeout",
                            "binary_stdout", "binary_out_name", "binary_out_type",
                            "upload_paths")}
                    return self._send_json(200, payload)
                remaining = deadline - time.time()
                if remaining <= 0:
                    return self._send_json(200, {"controls": [], "job": None})
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
            if data.get("output_chunk"):
                job["output"] += data["output_chunk"]
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
        self._send_json(200, {"ok": True})

    # --- admin: jobs ---------------------------------------------------
    def _handle_submit(self):
        if not self._auth("admin"):
            return
        data = self._read_json()
        if not data.get("command"):
            return self._send_json(400, {"error": "command required"})
        job_id = STATE.submit_job(
            command=data["command"], shell=bool(data.get("shell", True)),
            cwd=data.get("cwd"), env=data.get("env"), timeout=data.get("timeout"),
            target=data.get("target"), label=data.get("label"),
            binary_stdout=data.get("binary_stdout"),
            binary_out_name=data.get("binary_out_name"),
            binary_out_type=data.get("binary_out_type"),
            upload_paths=data.get("upload_paths"))
        print(f"[submit] job={job_id} target={data.get('target')} "
              f"cmd={data['command']!r}", flush=True)
        self._send_json(200, {"job_id": job_id})

    def _handle_job_status(self, job_id):
        if not self._auth("admin"):
            return
        job = STATE.jobs.get(job_id)
        if not job:
            return self._send_json(404, {"error": "unknown job"})
        self._send_json(200, job)

    def _handle_jobs_list(self):
        if not self._auth("admin"):
            return
        jobs = sorted(STATE.jobs.values(), key=lambda j: j["created_at"], reverse=True)
        summary = [{k: j[k] for k in ("id", "label", "command", "target", "status",
                                      "worker_id", "exit_code", "artifacts",
                                      "created_at", "started_at", "finished_at")}
                   for j in jobs]
        self._send_json(200, {"jobs": summary})

    def _handle_workers(self):
        if not self._auth("admin"):
            return
        with STATE.cond:
            out = [{"id": w["id"], "name": w["name"], "caps": w["caps"],
                    "online": STATE.worker_online(w), "last_seen": w["last_seen"],
                    "current_job": w["current_job"]} for w in STATE.workers.values()]
        self._send_json(200, {"workers": out})

    # --- admin + worker: sessions -------------------------------------
    def _handle_session_create(self):
        if not self._auth("admin"):
            return
        data = self._read_json()
        target = data.get("target")
        if not target:
            return self._send_json(400, {"error": "target required"})
        with STATE.cond:
            w = STATE.find_worker(target)
            if not w or not STATE.worker_online(w):
                return self._send_json(404, {"error": "no online worker for target"})
            session_id = STATE.create_session(
                w["id"], data.get("command"), bool(data.get("shell", True)),
                data.get("cwd"), data.get("env"))
            STATE.enqueue_control(w["id"], {
                "type": "session_start", "session_id": session_id,
                "command": data.get("command"), "shell": bool(data.get("shell", True)),
                "cwd": data.get("cwd"), "env": data.get("env") or {}})
        print(f"[session] create {session_id} on {w['id']} "
              f"cmd={data.get('command')!r}", flush=True)
        self._send_json(200, {"session_id": session_id, "worker_id": w["id"]})

    def _handle_session_input(self, session_id):
        if not self._auth("admin"):
            return
        data = self._read_json()
        with STATE.cond:
            s = STATE.sessions.get(session_id)
            if not s:
                return self._send_json(404, {"error": "unknown session"})
            if s["status"] in ("closed", "error"):
                return self._send_json(409, {"error": f"session {s['status']}"})
            STATE.enqueue_control(s["worker_id"], {
                "type": "session_input", "session_id": session_id,
                "data": data.get("data", "")})
        self._send_json(200, {"ok": True})

    def _handle_session_close(self, session_id):
        if not self._auth("admin"):
            return
        with STATE.cond:
            s = STATE.sessions.get(session_id)
            if not s:
                return self._send_json(404, {"error": "unknown session"})
            STATE.enqueue_control(s["worker_id"],
                                  {"type": "session_close", "session_id": session_id})
        self._send_json(200, {"ok": True})

    def _handle_session_output(self):
        if not self._auth("worker"):
            return
        data = self._read_json()
        session_id = data.get("session_id")
        with STATE.cond:
            s = STATE.sessions.get(session_id)
            if not s:
                return self._send_json(404, {"error": "unknown session"})
            if data.get("started"):
                s["status"] = "open"
            if data.get("output_chunk"):
                s["output"] += data["output_chunk"]
            if data.get("done"):
                s["exit_code"] = data.get("exit_code")
                s["status"] = "error" if data.get("error") else "closed"
                print(f"[session] {session_id} closed exit={s['exit_code']}", flush=True)
            STATE.cond.notify_all()
        self._send_json(200, {"ok": True})

    def _handle_sessions_list(self):
        if not self._auth("admin"):
            return
        with STATE.cond:
            out = [{k: s[k] for k in ("id", "worker_id", "command", "status",
                                      "exit_code", "created_at")}
                   for s in sorted(STATE.sessions.values(),
                                   key=lambda s: s["created_at"], reverse=True)]
        self._send_json(200, {"sessions": out})

    def _handle_session_get(self, session_id, qs):
        if not self._auth("admin"):
            return
        s = STATE.sessions.get(session_id)
        if not s:
            return self._send_json(404, {"error": "unknown session"})
        since = int((qs.get("since") or ["0"])[0])
        out = {k: s[k] for k in ("id", "worker_id", "command", "status",
                                 "exit_code", "created_at")}
        out["output"] = s["output"][since:]
        out["output_len"] = len(s["output"])
        self._send_json(200, out)

    # --- admin: pull a file from a worker ------------------------------
    def _handle_pull(self):
        if not self._auth("admin"):
            return
        data = self._read_json()
        target, path = data.get("target"), data.get("path")
        if not target or not path:
            return self._send_json(400, {"error": "target and path required"})
        with STATE.cond:
            w = STATE.find_worker(target)
            if not w or not STATE.worker_online(w):
                return self._send_json(404, {"error": "no online worker for target"})
            artifact_id = STATE.create_artifact(
                data.get("name") or os.path.basename(path),
                data.get("content_type"), worker=w["id"], status="pending")
            STATE.enqueue_control(w["id"], {
                "type": "pull", "path": path, "artifact_id": artifact_id})
        print(f"[pull] {path} from {w['id']} -> artifact {artifact_id}", flush=True)
        self._send_json(200, {"artifact_id": artifact_id, "worker_id": w["id"]})

    # --- worker + admin: artifacts ------------------------------------
    def _handle_artifact_create(self):
        if not self._auth("worker"):
            return
        data = self._read_json()
        artifact_id = STATE.create_artifact(
            data.get("name"), data.get("content_type"),
            job_id=data.get("job_id"), session_id=data.get("session_id"))
        self._send_json(200, {"artifact_id": artifact_id})

    def _handle_artifact_chunk(self, artifact_id, qs):
        if not self._auth("worker"):
            return
        body = self._read_body()
        final = (qs.get("final") or ["0"])[0] == "1"
        error = (qs.get("error") or ["0"])[0] == "1"
        with STATE.cond:
            a = STATE.artifacts.get(artifact_id)
            if not a:
                return self._send_json(404, {"error": "unknown artifact"})
            if error:
                a["status"], a["error"] = "error", "worker reported error"
                STATE.cond.notify_all()
                return self._send_json(200, {"ok": True})
            if len(a["data"]) + len(body) > MAX_ARTIFACT_BYTES:
                a["status"], a["error"] = "error", "exceeds max artifact size"
                STATE.cond.notify_all()
                return self._send_json(413, {"error": "artifact too large"})
            a["data"].extend(body)
            a["size"] = len(a["data"])
            a["status"] = "complete" if final else "uploading"
            STATE.cond.notify_all()
        self._send_json(200, {"ok": True})

    def _handle_artifacts_list(self):
        if not self._auth("admin"):
            return
        with STATE.cond:
            out = [STATE.artifact_meta(a) for a in
                   sorted(STATE.artifacts.values(),
                          key=lambda a: a["created_at"], reverse=True)]
        self._send_json(200, {"artifacts": out})

    def _handle_artifact_get(self, artifact_id, qs):
        if not self._auth("admin"):
            return
        a = STATE.artifacts.get(artifact_id)
        if not a:
            return self._send_json(404, {"error": "unknown artifact"})
        if (qs.get("meta") or ["0"])[0] == "1":
            return self._send_json(200, STATE.artifact_meta(a))
        if a["status"] != "complete":
            return self._send_json(409, {"error": f"artifact {a['status']}",
                                         "status": a["status"]})
        self._send_bytes(200, bytes(a["data"]), a["content_type"])


HELP_PAGE = """compute-relay coordinator is running.

Self-hosted distributed task runner: one-shot jobs, PTY-backed interactive
sessions, and binary artifacts (screenshots, recordings). See the repository
README. Web dashboard: /ui (open a sign-in link: `client.py ui`). Endpoints:

  GET  /healthz                         liveness
  GET  /ui                              web dashboard               (public page)
  POST /api/ui-link                     mint a one-time sign-in link (admin)
  POST /api/session                     redeem a link for a cookie  (link token)
  POST /api/logout                      end the dashboard session   (cookie)
  GET  /install.sh  /agent.py           worker bootstrap            (public)
  POST /api/register                    worker enroll               (worker)
  GET  /api/poll                        worker control channel      (worker)
  POST /api/result                      one-shot job output         (worker)
  POST /api/jobs                        submit a job                (admin)
  GET  /api/jobs  /api/jobs/<id>        job list / status           (admin)
  GET  /api/workers                     list workers                (admin)
  GET  /api/enroll                      enroll command + worker tok (admin)
  POST /api/sessions                    open a session              (admin)
  POST /api/sessions/<id>/input         send stdin                  (admin)
  POST /api/sessions/<id>/close         close a session             (admin)
  GET  /api/sessions  /api/sessions/<id>  list / read (?since=)     (admin)
  POST /api/session_output              session output              (worker)
  POST /api/pull                        pull a file from a worker   (admin)
  POST /api/artifacts                   create an artifact          (worker)
  POST /api/artifacts/<id>/chunk        upload a chunk              (worker)
  GET  /api/artifacts  /api/artifacts/<id>[?meta=1]  list / fetch   (admin)
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
    print("Give the worker token (with your server URL) to machines you enroll.", flush=True)
    print("Keep the admin token private: it can submit jobs and open sessions.", flush=True)
    public = PUBLIC_URL or \
        f"http://{'127.0.0.1' if host in ('0.0.0.0', '') else host}:{port}"
    print(f"Web dashboard sign-in link (one-time, expires in {UI_LINK_TTL // 60} min):", flush=True)
    print(f"  {public}/ui#login={STATE.mint_ui_link()}", flush=True)
    print("  Get a fresh link any time with: python3 client.py ui", flush=True)
    print("=" * 64, flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down", flush=True)
        httpd.shutdown()


if __name__ == "__main__":
    main()
