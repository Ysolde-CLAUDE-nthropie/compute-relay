#!/usr/bin/env python3
"""compute-relay admin client.

Submit jobs, drive interactive sessions, and pull artifacts (screenshots,
recordings) from the pool. Uses the admin token.

Configuration (environment variables):
  RELAY_SERVER        coordinator base URL, e.g. https://relay.example.com
  RELAY_ADMIN_TOKEN   admin token from the coordinator

Examples:
  client.py workers
  client.py run "nvidia-smi"
  client.py run --target gpu-box "python train.py"

  # interactive, stateful session (e.g. an adb shell on a specific node)
  client.py session shell --target pixel-host --cmd "adb shell"

  # one-off stateful commands against the same live emulator/daemon:
  client.py run --target pixel-host "adb shell input tap 500 1200"
  client.py run --target pixel-host "adb shell am start -n com.x/.MainActivity"

  # binary feedback
  client.py screenshot --target pixel-host --out shot.png
  client.py record --target pixel-host --seconds 10 --out demo.mp4
  client.py pull --target pixel-host --out log.bin /sdcard/trace.bin
"""

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

SERVER = os.environ.get("RELAY_SERVER", "").rstrip("/")
TOKEN = os.environ.get("RELAY_ADMIN_TOKEN", "")


def _request(method, path, body=None, timeout=60, raw_response=False):
    url = f"{SERVER}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        payload = resp.read()
        if raw_response:
            return resp.getcode(), payload, resp.headers.get("Content-Type")
        return resp.getcode(), (json.loads(payload) if payload else {})
    except urllib.error.HTTPError as e:
        payload = e.read()
        if raw_response:
            return e.code, payload, e.headers.get("Content-Type")
        try:
            return e.code, json.loads(payload)
        except json.JSONDecodeError:
            return e.code, {"error": payload.decode(errors="replace")}


def die(msg):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


# --- workers / jobs ----------------------------------------------------
def cmd_workers(args):
    _, data = _request("GET", "/api/workers")
    workers = data.get("workers", [])
    if not workers:
        print("no workers enrolled")
        return
    for w in workers:
        caps = w.get("caps", {})
        gpus = ", ".join(caps.get("gpus", [])) or "none"
        busy = f" busy:{w['current_job']}" if w.get("current_job") else ""
        print(f"{w['name']:<20} {'online' if w['online'] else 'OFFLINE':<8} "
              f"id={w['id']} cpu={caps.get('cpu_count')} gpu=[{gpus}]{busy}")


def cmd_jobs(args):
    _, data = _request("GET", "/api/jobs")
    for j in data.get("jobs", []):
        ts = time.strftime("%H:%M:%S", time.localtime(j["created_at"]))
        arts = f" artifacts={j['artifacts']}" if j.get("artifacts") else ""
        print(f"{j['id']}  {j['status']:<8} exit={j['exit_code']} "
              f"worker={j['worker_id']} {ts} {j.get('label') or ''}{arts}")


def _submit(args, binary_stdout=False, binary_out_name=None, binary_out_type=None):
    body = {"command": args.command, "shell": not getattr(args, "no_shell", False),
            "label": getattr(args, "label", None)}
    for attr, key in (("target", "target"), ("cwd", "cwd"), ("timeout", "timeout")):
        if getattr(args, attr, None):
            body[key] = getattr(args, attr)
    if getattr(args, "env", None):
        body["env"] = dict(p.split("=", 1) for p in args.env if "=" in p)
    if getattr(args, "artifact", None):
        body["upload_paths"] = args.artifact
    if binary_stdout:
        body.update({"binary_stdout": True, "binary_out_name": binary_out_name,
                     "binary_out_type": binary_out_type})
    code, data = _request("POST", "/api/jobs", body)
    if code != 200:
        die(data.get("error"))
    return data["job_id"]


def _wait_job(job_id, stream=True):
    shown = 0
    while True:
        code, job = _request("GET", f"/api/jobs/{job_id}")
        if code != 200:
            die(job.get("error"))
        if stream:
            out = job.get("output", "")
            if len(out) > shown:
                sys.stdout.write(out[shown:]); sys.stdout.flush(); shown = len(out)
        if job["status"] in ("done", "error"):
            return job
        time.sleep(0.4)


def cmd_submit(args):
    print(_submit(args))


def cmd_run(args):
    if getattr(args, "binary_out", None):
        name = os.path.basename(args.binary_out)
        job_id = _submit(args, binary_stdout=True, binary_out_name=name)
        print(f"[job {job_id} submitted]", file=sys.stderr)
        job = _wait_job(job_id, stream=False)
        if job.get("output"):
            print(job["output"], file=sys.stderr, end="")
        if job["status"] != "done":
            die(f"job failed (exit {job['exit_code']})")
        if not job.get("artifacts"):
            die("no binary output produced")
        _download_artifact(job["artifacts"][0], args.binary_out)
        return
    job_id = _submit(args)
    print(f"[job {job_id} submitted]", file=sys.stderr)
    job = _wait_job(job_id, stream=True)
    print(f"\n[job {job_id} {job['status']}, exit={job['exit_code']}]", file=sys.stderr)
    sys.exit(job["exit_code"] if job["exit_code"] is not None else 1)


def cmd_status(args):
    code, job = _request("GET", f"/api/jobs/{args.job_id}")
    if code != 200:
        die(job.get("error"))
    print(f"job {job['id']}  status={job['status']}  exit={job['exit_code']}  "
          f"worker={job['worker_id']}  artifacts={job.get('artifacts')}")
    if job.get("output"):
        print("--- output ---")
        print(job["output"], end="")


# --- artifacts ---------------------------------------------------------
def _download_artifact(artifact_id, out_path, wait=True, timeout=600):
    deadline = time.time() + timeout
    while True:
        code, meta = _request("GET", f"/api/artifacts/{artifact_id}?meta=1")
        if code != 200:
            die(meta.get("error"))
        if meta["status"] == "complete":
            break
        if meta["status"] == "error":
            die(f"artifact failed: {meta.get('error')}")
        if not wait or time.time() > deadline:
            die(f"artifact not ready (status={meta['status']})")
        time.sleep(0.4)
    code, body, _ = _request("GET", f"/api/artifacts/{artifact_id}", raw_response=True)
    if code != 200:
        die(f"download failed ({code})")
    with open(out_path, "wb") as f:
        f.write(body)
    print(f"saved {len(body)} bytes -> {out_path} ({meta['content_type']})",
          file=sys.stderr)


def cmd_artifacts(args):
    _, data = _request("GET", "/api/artifacts")
    for a in data.get("artifacts", []):
        ts = time.strftime("%H:%M:%S", time.localtime(a["created_at"]))
        print(f"{a['id']}  {a['status']:<9} {a['size']:>10}B  {a['content_type']:<24} "
              f"{ts}  {a['name']}")


def cmd_fetch(args):
    _download_artifact(args.artifact_id, args.out, wait=False)


def cmd_pull(args):
    code, data = _request("POST", "/api/pull",
                          {"target": args.target, "path": args.path})
    if code != 200:
        die(data.get("error"))
    _download_artifact(data["artifact_id"], args.out)


def cmd_screenshot(args):
    name = os.path.basename(args.out)
    args.command = args.cmd
    args.no_shell = False
    job_id = _submit(args, binary_stdout=True, binary_out_name=name,
                     binary_out_type="image/png")
    job = _wait_job(job_id, stream=False)
    if job.get("output"):
        print(job["output"], file=sys.stderr, end="")
    if job["status"] != "done":
        die(f"capture command failed (exit {job['exit_code']})")
    if not job.get("artifacts"):
        die("no artifact produced")
    _download_artifact(job["artifacts"][0], args.out)


def cmd_record(args):
    # Record on the device to a file, then pull it back.
    remote = f"/sdcard/relay-rec-{int(time.time())}.mp4"
    rec_cmd = (f"adb shell screenrecord --time-limit {args.seconds} {remote}")
    args.command = rec_cmd
    args.no_shell = False
    args.artifact = None
    print(f"[recording {args.seconds}s on {args.target}]", file=sys.stderr)
    job_id = _submit(args)
    job = _wait_job(job_id, stream=False)
    if job["status"] != "done":
        if job.get("output"):
            print(job["output"], file=sys.stderr, end="")
        die(f"screenrecord failed (exit {job['exit_code']})")
    code, data = _request("POST", "/api/pull", {"target": args.target, "path": remote})
    if code != 200:
        die(data.get("error"))
    _download_artifact(data["artifact_id"], args.out)
    # best-effort cleanup of the on-device file
    args.command = f"adb shell rm -f {remote}"
    _submit(args)


# --- sessions ----------------------------------------------------------
def cmd_session_open(args):
    body = {"target": args.target, "command": args.cmd, "shell": True}
    if args.cwd:
        body["cwd"] = args.cwd
    code, data = _request("POST", "/api/sessions", body)
    if code != 200:
        die(data.get("error"))
    print(data["session_id"])


def cmd_session_list(args):
    _, data = _request("GET", "/api/sessions")
    for s in data.get("sessions", []):
        ts = time.strftime("%H:%M:%S", time.localtime(s["created_at"]))
        print(f"{s['id']}  {s['status']:<8} worker={s['worker_id']} "
              f"exit={s['exit_code']} {ts}  {s.get('command') or '(shell)'}")


def cmd_session_send(args):
    text = args.text if args.raw else args.text + "\n"
    code, data = _request("POST", f"/api/sessions/{args.session_id}/input",
                          {"data": text})
    if code != 200:
        die(data.get("error"))


def cmd_session_close(args):
    code, data = _request("POST", f"/api/sessions/{args.session_id}/close")
    if code != 200:
        die(data.get("error"))
    print(f"[closed {args.session_id}]", file=sys.stderr)


def _attach(session_id):
    """Stream session output and forward typed lines until closed/EOF."""
    stop = threading.Event()

    def pump_output():
        since = 0
        while not stop.is_set():
            code, s = _request("GET", f"/api/sessions/{session_id}?since={since}")
            if code != 200:
                print(f"\n[error: {s.get('error')}]", file=sys.stderr)
                stop.set(); return
            if s.get("output"):
                sys.stdout.write(s["output"]); sys.stdout.flush()
                since = s["output_len"]
            if s["status"] in ("closed", "error"):
                print(f"\n[session {s['status']}, exit={s['exit_code']}]",
                      file=sys.stderr)
                stop.set(); return
            time.sleep(0.3)

    t = threading.Thread(target=pump_output, daemon=True)
    t.start()
    print(f"[attached to {session_id}; type to send, Ctrl-D to detach, "
          f"Ctrl-C to close]", file=sys.stderr)
    try:
        for line in sys.stdin:
            if stop.is_set():
                break
            _request("POST", f"/api/sessions/{session_id}/input", {"data": line})
    except KeyboardInterrupt:
        _request("POST", f"/api/sessions/{session_id}/close")
        print(f"\n[closing {session_id}]", file=sys.stderr)
    stop.set()
    t.join(timeout=2)


def cmd_session_attach(args):
    _attach(args.session_id)


def cmd_session_shell(args):
    body = {"target": args.target, "command": args.cmd, "shell": True}
    if args.cwd:
        body["cwd"] = args.cwd
    code, data = _request("POST", "/api/sessions", body)
    if code != 200:
        die(data.get("error"))
    print(f"[session {data['session_id']} on {data['worker_id']}]", file=sys.stderr)
    _attach(data["session_id"])


# --- arg parsing -------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(description="compute-relay admin client")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("workers", help="list enrolled workers").set_defaults(fn=cmd_workers)
    sub.add_parser("jobs", help="list recent jobs").set_defaults(fn=cmd_jobs)

    ps = sub.add_parser("status", help="show a job's status/output")
    ps.add_argument("job_id"); ps.set_defaults(fn=cmd_status)

    def add_submit_args(sp):
        sp.add_argument("command", help="command to run on the worker")
        sp.add_argument("--target", help="pin to a worker name or id")
        sp.add_argument("--cwd", help="working directory on the worker")
        sp.add_argument("--timeout", type=int, help="kill after N seconds")
        sp.add_argument("--env", action="append", metavar="K=V", help="extra env var")
        sp.add_argument("--artifact", action="append", metavar="PATH",
                        help="file on the worker to upload after the job (repeatable)")
        sp.add_argument("--no-shell", action="store_true",
                        help="exec argv directly instead of via shell")
        sp.add_argument("--label", help="human label for the job")

    sp = sub.add_parser("submit", help="submit a job, print its id")
    add_submit_args(sp); sp.set_defaults(fn=cmd_submit)
    sp = sub.add_parser("run", help="submit a job and stream output")
    add_submit_args(sp)
    sp.add_argument("--binary-out", metavar="FILE",
                    help="capture the command's stdout as binary into FILE")
    sp.set_defaults(fn=cmd_run)

    # artifacts
    sub.add_parser("artifacts", help="list artifacts").set_defaults(fn=cmd_artifacts)
    sp = sub.add_parser("fetch", help="download a ready artifact")
    sp.add_argument("artifact_id"); sp.add_argument("--out", required=True)
    sp.set_defaults(fn=cmd_fetch)
    sp = sub.add_parser("pull", help="pull a file from a worker")
    sp.add_argument("path", help="path on the worker")
    sp.add_argument("--target", required=True); sp.add_argument("--out", required=True)
    sp.set_defaults(fn=cmd_pull)
    sp = sub.add_parser("screenshot", help="capture a screenshot from a device")
    sp.add_argument("--target", required=True)
    sp.add_argument("--out", default="screenshot.png")
    sp.add_argument("--cmd", default="adb exec-out screencap -p",
                    help="command whose stdout is the PNG")
    sp.set_defaults(fn=cmd_screenshot)
    sp = sub.add_parser("record", help="record a device screen and pull it back")
    sp.add_argument("--target", required=True)
    sp.add_argument("--seconds", type=int, default=10)
    sp.add_argument("--out", default="recording.mp4")
    sp.set_defaults(fn=cmd_record)

    # sessions
    sps = sub.add_parser("session", help="interactive stateful sessions")
    ssub = sps.add_subparsers(dest="scmd", required=True)
    sp = ssub.add_parser("open", help="open a session, print its id")
    sp.add_argument("--target", required=True)
    sp.add_argument("--cmd", help="command to run (default: login shell)")
    sp.add_argument("--cwd"); sp.set_defaults(fn=cmd_session_open)
    ssub.add_parser("list", help="list sessions").set_defaults(fn=cmd_session_list)
    sp = ssub.add_parser("send", help="send a line of input")
    sp.add_argument("session_id"); sp.add_argument("text")
    sp.add_argument("--raw", action="store_true", help="do not append a newline")
    sp.set_defaults(fn=cmd_session_send)
    sp = ssub.add_parser("attach", help="stream output and type input")
    sp.add_argument("session_id"); sp.set_defaults(fn=cmd_session_attach)
    sp = ssub.add_parser("close", help="close a session")
    sp.add_argument("session_id"); sp.set_defaults(fn=cmd_session_close)
    sp = ssub.add_parser("shell", help="open + attach in one step")
    sp.add_argument("--target", required=True)
    sp.add_argument("--cmd", help="command to run (default: login shell)")
    sp.add_argument("--cwd"); sp.set_defaults(fn=cmd_session_shell)
    return p


def main():
    if not SERVER or not TOKEN:
        print("error: set RELAY_SERVER and RELAY_ADMIN_TOKEN", file=sys.stderr)
        sys.exit(2)
    args = build_parser().parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
