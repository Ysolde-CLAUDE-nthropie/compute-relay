#!/usr/bin/env python3
"""compute-relay admin client.

Submit jobs to the coordinator and inspect workers/results. Uses the admin
token.

Configuration (environment variables):
  RELAY_SERVER        coordinator base URL, e.g. https://relay.example.com
  RELAY_ADMIN_TOKEN   admin token from the coordinator

Examples:
  client.py workers
  client.py run "nvidia-smi"
  client.py run --target gpu-box "python train.py"
  client.py submit "long_job.sh"          # returns a job id, does not wait
  client.py status <job_id>
  client.py jobs
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

SERVER = os.environ.get("RELAY_SERVER", "").rstrip("/")
TOKEN = os.environ.get("RELAY_ADMIN_TOKEN", "")


def _request(method, path, body=None, timeout=60):
    url = f"{SERVER}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        raw = resp.read()
        return resp.getcode(), (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"error": raw.decode(errors="replace")}


def cmd_workers(args):
    _, data = _request("GET", "/api/workers")
    workers = data.get("workers", [])
    if not workers:
        print("no workers enrolled")
        return
    for w in workers:
        state = "online" if w["online"] else "OFFLINE"
        caps = w.get("caps", {})
        gpus = ", ".join(caps.get("gpus", [])) or "none"
        busy = f" busy:{w['current_job']}" if w.get("current_job") else ""
        print(f"{w['name']:<20} {state:<8} id={w['id']} "
              f"cpu={caps.get('cpu_count')} gpu=[{gpus}]{busy}")


def cmd_jobs(args):
    _, data = _request("GET", "/api/jobs")
    for j in data.get("jobs", []):
        ts = time.strftime("%H:%M:%S", time.localtime(j["created_at"]))
        label = j.get("label") or ""
        print(f"{j['id']}  {j['status']:<8} exit={j['exit_code']} "
              f"worker={j['worker_id']} {ts} {label}")


def _submit(args):
    body = {
        "command": args.command,
        "shell": not args.no_shell,
        "label": args.label,
    }
    if args.target:
        body["target"] = args.target
    if args.cwd:
        body["cwd"] = args.cwd
    if args.timeout:
        body["timeout"] = args.timeout
    if args.env:
        env = {}
        for pair in args.env:
            k, _, v = pair.partition("=")
            env[k] = v
        body["env"] = env
    code, data = _request("POST", "/api/jobs", body)
    if code != 200:
        print(f"error: {data.get('error')}", file=sys.stderr)
        sys.exit(1)
    return data["job_id"]


def cmd_submit(args):
    job_id = _submit(args)
    print(job_id)


def cmd_status(args):
    code, job = _request("GET", f"/api/jobs/{args.job_id}")
    if code != 200:
        print(f"error: {job.get('error')}", file=sys.stderr)
        sys.exit(1)
    print(f"job {job['id']}  status={job['status']}  exit={job['exit_code']}  "
          f"worker={job['worker_id']}")
    if job.get("output"):
        print("--- output ---")
        print(job["output"], end="")


def cmd_run(args):
    """Submit and stream output until the job finishes."""
    job_id = _submit(args)
    print(f"[job {job_id} submitted]", file=sys.stderr)
    shown = 0
    while True:
        code, job = _request("GET", f"/api/jobs/{job_id}")
        if code != 200:
            print(f"error: {job.get('error')}", file=sys.stderr)
            sys.exit(1)
        out = job.get("output", "")
        if len(out) > shown:
            sys.stdout.write(out[shown:])
            sys.stdout.flush()
            shown = len(out)
        if job["status"] in ("done", "error"):
            print(f"\n[job {job_id} {job['status']}, exit={job['exit_code']}]",
                  file=sys.stderr)
            sys.exit(job["exit_code"] if job["exit_code"] is not None else 1)
        time.sleep(0.5)


def main():
    if not SERVER or not TOKEN:
        print("error: set RELAY_SERVER and RELAY_ADMIN_TOKEN", file=sys.stderr)
        sys.exit(2)

    p = argparse.ArgumentParser(description="compute-relay admin client")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("workers", help="list enrolled workers")
    sub.add_parser("jobs", help="list recent jobs")

    ps = sub.add_parser("status", help="show a job's status and output")
    ps.add_argument("job_id")

    def add_submit_args(sp):
        sp.add_argument("command", help="command to run on the worker")
        sp.add_argument("--target", help="pin to a worker name or id")
        sp.add_argument("--cwd", help="working directory on the worker")
        sp.add_argument("--timeout", type=int, help="kill after N seconds")
        sp.add_argument("--env", action="append", metavar="K=V",
                        help="extra env var (repeatable)")
        sp.add_argument("--no-shell", action="store_true",
                        help="exec argv directly instead of via shell")
        sp.add_argument("--label", help="human label for the job")

    add_submit_args(sub.add_parser("submit", help="submit a job, print its id"))
    add_submit_args(sub.add_parser("run", help="submit a job and stream output"))

    args = p.parse_args()
    {
        "workers": cmd_workers,
        "jobs": cmd_jobs,
        "status": cmd_status,
        "submit": cmd_submit,
        "run": cmd_run,
    }[args.cmd](args)


if __name__ == "__main__":
    main()
