#!/usr/bin/env python3
"""End-to-end simulation of compute-relay on localhost.

Starts a coordinator, launches a worker agent as a subprocess, submits jobs
through the client API, and checks the results. No external network, no real
remote machines: this is the internal sanity check.

Run: python3 test_local.py
"""

import base64
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("TEST_PORT", "8799"))
BASE = f"http://127.0.0.1:{PORT}"
WORKER_TOKEN = "test-worker-token"
ADMIN_TOKEN = "test-admin-token"


def api(method, path, body=None, token=ADMIN_TOKEN):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{BASE}{path}", data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, timeout=60)
    raw = resp.read()
    return resp.getcode(), (json.loads(raw) if raw else {})


def wait_for(predicate, timeout, msg):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for: {msg}")


def main():
    env = os.environ.copy()
    env.update({
        "RELAY_PORT": str(PORT),
        "RELAY_HOST": "127.0.0.1",
        "RELAY_WORKER_TOKEN": WORKER_TOKEN,
        "RELAY_ADMIN_TOKEN": ADMIN_TOKEN,
        "RELAY_TOKENS_FILE": "/tmp/relay-test-tokens.json",
    })

    server = subprocess.Popen([sys.executable, os.path.join(HERE, "server.py")],
                              env=env)
    worker = None
    failures = []
    try:
        wait_for(lambda: urllib.request.urlopen(f"{BASE}/healthz", timeout=2).getcode() == 200,
                 10, "server up")
        print("OK: server is up")

        # Auth must be enforced.
        try:
            urllib.request.urlopen(urllib.request.Request(f"{BASE}/api/workers"), timeout=5)
            failures.append("unauthenticated /api/workers should have failed")
        except urllib.error.HTTPError as e:
            assert e.code == 401, f"expected 401, got {e.code}"
            print("OK: unauthenticated request rejected (401)")

        # Wrong token must be rejected.
        try:
            api("GET", "/api/workers", token="wrong")
            failures.append("wrong token should have failed")
        except urllib.error.HTTPError as e:
            assert e.code == 401
            print("OK: wrong token rejected (401)")

        # Start a worker agent.
        wenv = os.environ.copy()
        wenv.update({
            "RELAY_SERVER": BASE,
            "RELAY_WORKER_TOKEN": WORKER_TOKEN,
            "RELAY_WORKER_NAME": "sim-worker",
        })
        worker = subprocess.Popen([sys.executable, os.path.join(HERE, "agent.py")],
                                  env=wenv)

        wait_for(lambda: any(w["name"] == "sim-worker" and w["online"]
                             for w in api("GET", "/api/workers")[1]["workers"]),
                 15, "worker to enroll")
        print("OK: worker enrolled and online")

        # Submit a simple job and wait for the result.
        _, data = api("POST", "/api/jobs",
                      {"command": "echo hello-from-worker && echo second-line",
                       "label": "smoke"})
        job_id = data["job_id"]

        def job_done():
            _, job = api("GET", f"/api/jobs/{job_id}")
            return job["status"] in ("done", "error")

        wait_for(job_done, 20, "job to finish")
        _, job = api("GET", f"/api/jobs/{job_id}")
        assert job["status"] == "done", f"status={job['status']}"
        assert job["exit_code"] == 0, f"exit={job['exit_code']}"
        assert "hello-from-worker" in job["output"], f"output={job['output']!r}"
        assert "second-line" in job["output"]
        print("OK: job ran and output captured")

        # Non-zero exit should be reported as error.
        _, data = api("POST", "/api/jobs", {"command": "exit 3", "label": "fail"})
        jid2 = data["job_id"]
        wait_for(lambda: api("GET", f"/api/jobs/{jid2}")[1]["status"] in ("done", "error"),
                 20, "failing job to finish")
        _, job2 = api("GET", f"/api/jobs/{jid2}")
        assert job2["status"] == "error", f"status={job2['status']}"
        assert job2["exit_code"] == 3, f"exit={job2['exit_code']}"
        print("OK: non-zero exit reported as error")

        # Targeting a nonexistent worker should leave the job pending.
        _, data = api("POST", "/api/jobs",
                      {"command": "echo nope", "target": "ghost", "label": "pin"})
        jid3 = data["job_id"]
        time.sleep(3)
        _, job3 = api("GET", f"/api/jobs/{jid3}")
        assert job3["status"] == "pending", f"status={job3['status']}"
        print("OK: job targeted at absent worker stays pending")

        # Targeting the real worker by name should run.
        _, data = api("POST", "/api/jobs",
                      {"command": "echo targeted-ok", "target": "sim-worker"})
        jid4 = data["job_id"]
        wait_for(lambda: api("GET", f"/api/jobs/{jid4}")[1]["status"] == "done",
                 20, "targeted job to finish")
        _, job4 = api("GET", f"/api/jobs/{jid4}")
        assert "targeted-ok" in job4["output"]
        print("OK: job targeted by worker name ran on that worker")

        # Timeout handling.
        _, data = api("POST", "/api/jobs",
                      {"command": "sleep 30", "timeout": 2, "label": "timeout"})
        jid5 = data["job_id"]
        wait_for(lambda: api("GET", f"/api/jobs/{jid5}")[1]["status"] in ("done", "error"),
                 20, "timeout job to finish")
        _, job5 = api("GET", f"/api/jobs/{jid5}")
        assert job5["status"] == "error", f"status={job5['status']}"
        print("OK: job exceeding timeout was terminated")

        # --- binary stdout artifact (stands in for `adb exec-out screencap`) ---
        _, data = api("POST", "/api/jobs",
                      {"command": "head -c 20000 /dev/urandom", "binary_stdout": True,
                       "binary_out_name": "blob.bin", "target": "sim-worker"})
        jid6 = data["job_id"]
        wait_for(lambda: api("GET", f"/api/jobs/{jid6}")[1]["status"] in ("done", "error"),
                 20, "binary job to finish")
        _, job6 = api("GET", f"/api/jobs/{jid6}")
        assert job6["status"] == "done", f"status={job6['status']}"
        assert job6["artifacts"], "no artifact recorded on job"
        art_id = job6["artifacts"][0]
        wait_for(lambda: api("GET", f"/api/artifacts/{art_id}?meta=1")[1]["status"] == "complete",
                 10, "artifact to complete")
        _, meta = api("GET", f"/api/artifacts/{art_id}?meta=1")
        assert meta["size"] == 20000, f"size={meta['size']}"
        # Download raw bytes and verify length.
        req = urllib.request.Request(f"{BASE}/api/artifacts/{art_id}")
        req.add_header("Authorization", f"Bearer {ADMIN_TOKEN}")
        raw = urllib.request.urlopen(req, timeout=10).read()
        assert len(raw) == 20000, f"downloaded {len(raw)} bytes"
        print("OK: binary stdout captured and downloaded as artifact (20000 bytes)")

        # --- file pull from a worker ---
        marker = f"pull-{int(time.time())}"
        with open("/tmp/relay-pull-src.txt", "w") as f:
            f.write(marker * 100)
        _, data = api("POST", "/api/pull",
                      {"target": "sim-worker", "path": "/tmp/relay-pull-src.txt"})
        pull_art = data["artifact_id"]
        wait_for(lambda: api("GET", f"/api/artifacts/{pull_art}?meta=1")[1]["status"] == "complete",
                 10, "pulled artifact to complete")
        req = urllib.request.Request(f"{BASE}/api/artifacts/{pull_art}")
        req.add_header("Authorization", f"Bearer {ADMIN_TOKEN}")
        pulled = urllib.request.urlopen(req, timeout=10).read().decode()
        assert pulled == marker * 100, "pulled content mismatch"
        print("OK: file pulled from worker as artifact")

        # --- interactive session: stateful across commands ---
        _, data = api("POST", "/api/sessions",
                      {"target": "sim-worker", "command": "sh"})
        sess = data["session_id"]
        wait_for(lambda: api("GET", f"/api/sessions/{sess}")[1]["status"] == "open",
                 10, "session to open")
        # Set a variable in one command, read it in the next -> proves state persists.
        api("POST", f"/api/sessions/{sess}/input", {"data": "MARKER=relay_stateful_123\n"})
        time.sleep(0.5)
        api("POST", f"/api/sessions/{sess}/input", {"data": "echo val=$MARKER\n"})

        def saw_marker():
            _, s = api("GET", f"/api/sessions/{sess}")
            return "val=relay_stateful_123" in s["output"]

        wait_for(saw_marker, 10, "session to echo the persisted variable")
        print("OK: session preserved state across separate inputs")

        api("POST", f"/api/sessions/{sess}/close", {})
        wait_for(lambda: api("GET", f"/api/sessions/{sess}")[1]["status"] in ("closed", "error"),
                 10, "session to close")
        print("OK: session closed on request")

        # --- web dashboard ---------------------------------------------
        resp = urllib.request.urlopen(f"{BASE}/ui", timeout=5)
        page = resp.read()
        assert resp.headers["Content-Type"].startswith("text/html")
        csp = resp.headers["Content-Security-Policy"]
        script = re.search(rb"<script>(.*?)</script>", page, re.S).group(1)
        digest = base64.b64encode(hashlib.sha256(script).digest()).decode()
        assert f"script-src 'sha256-{digest}'" in csp, f"csp={csp!r}"
        assert "connect-src 'self'" in csp and "frame-ancestors 'none'" in csp
        print("OK: /ui served with a CSP pinning its inline script")

        _, data = api("GET", "/api/jobs")
        listed = {j["id"]: j for j in data["jobs"]}[job_id]
        assert listed["command"] and "finished_at" in listed and "target" in listed
        print("OK: job list includes command, target and timestamps")

    except Exception as e:
        failures.append(f"{type(e).__name__}: {e}")
    finally:
        for p in (worker, server):
            if p and p.poll() is None:
                p.send_signal(signal.SIGINT)
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    import urllib.error
    main()
