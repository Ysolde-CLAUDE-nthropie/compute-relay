#!/usr/bin/env python3
"""compute-relay worker agent.

Runs on a machine you own or are authorized to manage. It connects OUT to a
coordinator you specify, authenticates with the worker token, and then pulls
and runs compute jobs, streaming their output back.

It is deliberately transparent: it prints the coordinator it talks to and every
command it runs. It does not hide itself, install persistence, or evade
anything. Stop it with Ctrl-C (or stop the systemd unit). Only point it at a
coordinator you trust, since by design it runs the commands that coordinator
sends.

Configuration (environment variables):
  RELAY_SERVER         coordinator base URL, e.g. https://relay.example.com
  RELAY_WORKER_TOKEN   shared worker token from the coordinator
  RELAY_WORKER_NAME    optional display name (default: hostname)

Standard library only; works on Python 3.8+.
"""

import json
import os
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SERVER = os.environ.get("RELAY_SERVER", "").rstrip("/")
TOKEN = os.environ.get("RELAY_WORKER_TOKEN", "")
NAME = os.environ.get("RELAY_WORKER_NAME") or socket.gethostname()

OUTPUT_FLUSH_SECONDS = 1.0
OUTPUT_FLUSH_BYTES = 4096


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _request(method, path, body=None, expect_json=True, timeout=60):
    url = f"{SERVER}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, timeout=timeout)
    status = resp.getcode()
    raw = resp.read()
    if expect_json and raw:
        return status, json.loads(raw)
    return status, {}


def detect_caps():
    caps = {
        "platform": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    # Best-effort GPU note; never fails the agent if the tool is absent.
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0 and out.stdout.strip():
            caps["gpus"] = [g.strip() for g in out.stdout.strip().splitlines()]
    except (OSError, subprocess.SubprocessError):
        pass
    return caps


def register():
    while True:
        try:
            _, data = _request("POST", "/api/register",
                               {"name": NAME, "caps": detect_caps()})
            worker_id = data["worker_id"]
            log(f"registered with coordinator as '{NAME}' (id={worker_id})")
            return worker_id
        except (urllib.error.URLError, OSError, KeyError) as e:
            log(f"register failed ({e}); retrying in 5s")
            time.sleep(5)


def post_result(job_id, output_chunk=None, done=False, exit_code=None, error=False):
    body = {"job_id": job_id}
    if output_chunk:
        body["output_chunk"] = output_chunk
    if done:
        body["done"] = True
        body["exit_code"] = exit_code
        body["error"] = error
    try:
        _request("POST", "/api/result", body, expect_json=False, timeout=30)
    except (urllib.error.URLError, OSError) as e:
        log(f"failed to post result for job {job_id}: {e}")


def run_job(job):
    job_id = job["id"]
    command = job["command"]
    use_shell = job.get("shell", True)
    cwd = job.get("cwd") or None
    timeout = job.get("timeout")
    extra_env = job.get("env") or {}

    env = os.environ.copy()
    env.update({str(k): str(v) for k, v in extra_env.items()})

    log(f"running job {job_id}: {command!r} (shell={use_shell}, cwd={cwd or '.'})")
    try:
        if use_shell:
            args = command if isinstance(command, str) else " ".join(command)
        else:
            args = command if isinstance(command, list) else command.split()
        proc = subprocess.Popen(
            args, shell=use_shell, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
            # Own process group so a timeout can kill the whole job tree
            # (the shell and any children), not just the top process.
            start_new_session=True,
        )
    except (OSError, ValueError) as e:
        log(f"job {job_id} failed to start: {e}")
        post_result(job_id, output_chunk=f"failed to start: {e}\n",
                    done=True, exit_code=-1, error=True)
        return

    # Stream output back in small batches.
    buf = []
    buf_len = [0]
    last_flush = [time.time()]
    lock = threading.Lock()
    stop = threading.Event()

    def flush():
        with lock:
            if not buf:
                return
            chunk = "".join(buf)
            buf.clear()
            buf_len[0] = 0
            last_flush[0] = time.time()
        post_result(job_id, output_chunk=chunk)

    def flusher():
        while not stop.wait(OUTPUT_FLUSH_SECONDS):
            if time.time() - last_flush[0] >= OUTPUT_FLUSH_SECONDS:
                flush()

    t = threading.Thread(target=flusher, daemon=True)
    t.start()

    killed = False

    def kill_tree():
        """Kill the job's whole process group, falling back to the process."""
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except OSError:
                pass

    def watchdog():
        nonlocal killed
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            killed = True
            log(f"job {job_id} exceeded timeout {timeout}s; terminating")
            kill_tree()

    wd = threading.Thread(target=watchdog, daemon=True) if timeout else None
    if wd:
        wd.start()

    try:
        for line in proc.stdout:
            with lock:
                buf.append(line)
                buf_len[0] += len(line)
                flush_now = buf_len[0] >= OUTPUT_FLUSH_BYTES
            if flush_now:
                flush()
    finally:
        proc.stdout.close()
        exit_code = proc.wait()
        stop.set()
        flush()

    if killed:
        post_result(job_id, output_chunk=f"\n[terminated after timeout {timeout}s]\n",
                    done=True, exit_code=exit_code, error=True)
        log(f"job {job_id} terminated (timeout)")
    else:
        post_result(job_id, done=True, exit_code=exit_code,
                    error=(exit_code != 0))
        log(f"job {job_id} finished with exit code {exit_code}")


def poll_loop(worker_id):
    backoff = 1
    while True:
        try:
            status, data = _request(
                "GET", f"/api/poll?worker_id={worker_id}",
                expect_json=True, timeout=120,
            )
            backoff = 1
            if status == 200 and data.get("job"):
                run_job(data["job"])
            # 204 => no work; loop again immediately.
        except urllib.error.HTTPError as e:
            if e.code == 409:
                log("coordinator forgot us; re-registering")
                return  # trigger re-register in outer loop
            log(f"poll HTTP error {e.code}; backing off {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except (urllib.error.URLError, OSError) as e:
            log(f"poll failed ({e}); backing off {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    if not SERVER or not TOKEN:
        print("error: set RELAY_SERVER and RELAY_WORKER_TOKEN", file=sys.stderr)
        sys.exit(2)

    print("=" * 64, flush=True)
    print("compute-relay worker agent", flush=True)
    print(f"  coordinator : {SERVER}", flush=True)
    print(f"  worker name : {NAME}", flush=True)
    print("This agent runs compute jobs the coordinator sends. Stop with Ctrl-C.", flush=True)
    print("=" * 64, flush=True)

    try:
        while True:
            worker_id = register()
            poll_loop(worker_id)
    except KeyboardInterrupt:
        log("stopped by user")


if __name__ == "__main__":
    main()
