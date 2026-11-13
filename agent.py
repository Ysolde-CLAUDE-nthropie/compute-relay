#!/usr/bin/env python3
"""compute-relay worker agent.

Runs on a machine you own or are authorized to manage. Connects OUT to a
coordinator you specify, authenticates with the worker token, then handles:

  - one-shot jobs : run a command, stream text output, report the exit code
  - sessions      : a long-lived PTY-backed process (e.g. `adb shell`) you feed
                    input to and read output from, stateful across commands
  - artifacts     : upload binary files back (screenshots, recordings), either
                    a job's binary stdout, declared files after a job, or an
                    ad-hoc file pull

It is deliberately transparent: it prints the coordinator it talks to and the
work it runs. It does not hide itself, install persistence, or evade anything.
Stop it with Ctrl-C (or stop the systemd unit). Only point it at a coordinator
you trust, since by design it runs the work that coordinator sends.

Configuration (environment variables):
  RELAY_SERVER         coordinator base URL, e.g. https://relay.example.com
  RELAY_WORKER_TOKEN   shared worker token from the coordinator
  RELAY_WORKER_NAME    optional display name (default: hostname)

Standard library only. Runs on Linux, macOS, and Windows (Python 3.8+). On
POSIX, sessions use a real PTY; on Windows (which has no `termios`/`pty`), they
fall back to a pipe-backed process, which is fine for line-oriented tools like
`adb shell`.
"""

import json
import mimetypes
import os
import platform
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

IS_WINDOWS = os.name == "nt"
if not IS_WINDOWS:
    import signal

# A PTY gives real terminal semantics but is POSIX-only (it needs termios).
# On Windows, or when RELAY_NO_PTY is set (handy for testing the fallback),
# sessions use plain pipes instead. `pty` itself is imported lazily below so
# this module imports cleanly on Windows.
USE_PTY = (not IS_WINDOWS) and not os.environ.get("RELAY_NO_PTY")

SERVER = os.environ.get("RELAY_SERVER", "").rstrip("/")
TOKEN = os.environ.get("RELAY_WORKER_TOKEN", "")
NAME = os.environ.get("RELAY_WORKER_NAME") or socket.gethostname()

OUTPUT_FLUSH_SECONDS = 1.0
OUTPUT_FLUSH_BYTES = 4096
ARTIFACT_CHUNK_BYTES = 256 * 1024

sessions = {}            # session_id -> {"master": fd, "proc": Popen, "lock": Lock}
sessions_lock = threading.Lock()
oneshot_q = queue.Queue()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _request(method, path, body=None, raw=None, ctype=None, expect_json=True, timeout=60):
    url = f"{SERVER}{path}"
    if raw is not None:
        data = raw
    elif body is not None:
        data = json.dumps(body).encode()
        ctype = ctype or "application/json"
    else:
        data = None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    if data is not None:
        req.add_header("Content-Type", ctype or "application/octet-stream")
    resp = urllib.request.urlopen(req, timeout=timeout)
    payload = resp.read()
    if expect_json and payload:
        return resp.getcode(), json.loads(payload)
    return resp.getcode(), {}


# --- artifact helpers --------------------------------------------------
def create_artifact(name, content_type, job_id=None, session_id=None):
    _, data = _request("POST", "/api/artifacts",
                       {"name": name, "content_type": content_type,
                        "job_id": job_id, "session_id": session_id})
    return data["artifact_id"]


def put_chunk(artifact_id, data_bytes, final=False, error=False):
    path = f"/api/artifacts/{artifact_id}/chunk?final={int(final)}&error={int(error)}"
    _request("POST", path, raw=data_bytes, ctype="application/octet-stream",
             expect_json=False, timeout=120)


def upload_file(path, artifact_id=None, name=None, content_type=None,
                job_id=None, session_id=None):
    name = name or os.path.basename(path)
    ctype = content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    if artifact_id is None:
        artifact_id = create_artifact(name, ctype, job_id, session_id)
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(ARTIFACT_CHUNK_BYTES)
                if not chunk:
                    break
                put_chunk(artifact_id, chunk)
        put_chunk(artifact_id, b"", final=True)
        log(f"uploaded artifact {artifact_id} <- {path}")
    except OSError as e:
        put_chunk(artifact_id, b"", error=True)
        log(f"artifact upload failed for {path}: {e}")
    return artifact_id


# --- enrollment --------------------------------------------------------
def detect_caps():
    caps = {"platform": platform.system(), "release": platform.release(),
            "machine": platform.machine(), "python": platform.python_version(),
            "cpu_count": os.cpu_count()}
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=5)
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
            log(f"registered with coordinator as '{NAME}' (id={data['worker_id']})")
            return data["worker_id"]
        except (urllib.error.URLError, OSError, KeyError) as e:
            log(f"register failed ({e}); retrying in 5s")
            time.sleep(5)


# --- one-shot jobs -----------------------------------------------------
def post_result(job_id, output_chunk=None, done=False, exit_code=None, error=False):
    body = {"job_id": job_id}
    if output_chunk:
        body["output_chunk"] = output_chunk
    if done:
        body.update({"done": True, "exit_code": exit_code, "error": error})
    try:
        _request("POST", "/api/result", body, expect_json=False, timeout=30)
    except (urllib.error.URLError, OSError) as e:
        log(f"failed to post result for job {job_id}: {e}")


def _build_args(command, use_shell):
    if use_shell:
        return command if isinstance(command, str) else " ".join(command)
    return command if isinstance(command, list) else command.split()


def _group_kwargs():
    """Popen kwargs so a job/session can be killed as a whole tree."""
    if IS_WINDOWS:
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def kill_tree(proc):
    try:
        if IS_WINDOWS:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


def run_job(job):
    job_id = job["id"]
    if job.get("binary_stdout"):
        return run_job_binary(job)

    command = job["command"]
    use_shell = job.get("shell", True)
    cwd = job.get("cwd") or None
    timeout = job.get("timeout")
    env = os.environ.copy()
    env.update({str(k): str(v) for k, v in (job.get("env") or {}).items()})

    log(f"running job {job_id}: {command!r} (shell={use_shell}, cwd={cwd or '.'})")
    try:
        proc = subprocess.Popen(
            _build_args(command, use_shell), shell=use_shell, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            encoding="utf-8", errors="replace", bufsize=1, **_group_kwargs())
    except (OSError, ValueError) as e:
        log(f"job {job_id} failed to start: {e}")
        return post_result(job_id, output_chunk=f"failed to start: {e}\n",
                           done=True, exit_code=-1, error=True)

    buf, buf_len, last_flush = [], [0], [time.time()]
    lock, stop = threading.Lock(), threading.Event()

    def flush():
        with lock:
            if not buf:
                return
            chunk = "".join(buf)
            buf.clear(); buf_len[0] = 0; last_flush[0] = time.time()
        post_result(job_id, output_chunk=chunk)

    def flusher():
        while not stop.wait(OUTPUT_FLUSH_SECONDS):
            if time.time() - last_flush[0] >= OUTPUT_FLUSH_SECONDS:
                flush()

    threading.Thread(target=flusher, daemon=True).start()

    killed = [False]
    if timeout:
        def watchdog():
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                killed[0] = True
                log(f"job {job_id} exceeded timeout {timeout}s; terminating")
                kill_tree(proc)
        threading.Thread(target=watchdog, daemon=True).start()

    try:
        for line in proc.stdout:
            with lock:
                buf.append(line); buf_len[0] += len(line)
                over = buf_len[0] >= OUTPUT_FLUSH_BYTES
            if over:
                flush()
    finally:
        proc.stdout.close()
        exit_code = proc.wait()
        stop.set(); flush()

    if job.get("upload_paths"):
        for p in job["upload_paths"]:
            upload_file(p, job_id=job_id)

    if killed[0]:
        post_result(job_id, output_chunk=f"\n[terminated after timeout {timeout}s]\n",
                    done=True, exit_code=exit_code, error=True)
    else:
        post_result(job_id, done=True, exit_code=exit_code, error=(exit_code != 0))
    log(f"job {job_id} finished with exit code {exit_code}")


def run_job_binary(job):
    """Capture the command's stdout as a binary artifact (e.g. a screenshot)."""
    job_id = job["id"]
    command = job["command"]
    use_shell = job.get("shell", True)
    cwd = job.get("cwd") or None
    timeout = job.get("timeout")
    name = job.get("binary_out_name") or "stdout.bin"
    ctype = job.get("binary_out_type") or mimetypes.guess_type(name)[0] \
        or "application/octet-stream"
    env = os.environ.copy()
    env.update({str(k): str(v) for k, v in (job.get("env") or {}).items()})

    log(f"running binary job {job_id}: {command!r} -> artifact {name!r}")
    artifact_id = create_artifact(name, ctype, job_id=job_id)
    try:
        proc = subprocess.Popen(
            _build_args(command, use_shell), shell=use_shell, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, **_group_kwargs())
    except (OSError, ValueError) as e:
        put_chunk(artifact_id, b"", error=True)
        return post_result(job_id, output_chunk=f"failed to start: {e}\n",
                           done=True, exit_code=-1, error=True)

    killed = [False]
    if timeout:
        def watchdog():
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                killed[0] = True
                kill_tree(proc)
        threading.Thread(target=watchdog, daemon=True).start()

    while True:
        chunk = proc.stdout.read(ARTIFACT_CHUNK_BYTES)
        if not chunk:
            break
        put_chunk(artifact_id, chunk)
    put_chunk(artifact_id, b"", final=True)
    exit_code = proc.wait()
    stderr = proc.stderr.read().decode(errors="replace")
    post_result(job_id, output_chunk=stderr, done=True, exit_code=exit_code,
                error=(exit_code != 0 or killed[0]))
    log(f"binary job {job_id} finished with exit code {exit_code}")


def oneshot_worker():
    """Single consumer so one-shot jobs run sequentially, sessions aside."""
    while True:
        job = oneshot_q.get()
        try:
            run_job(job)
        except Exception as e:  # never let one job kill the consumer
            log(f"job {job.get('id')} crashed: {e}")
            post_result(job.get("id"), output_chunk=f"worker error: {e}\n",
                        done=True, exit_code=-1, error=True)


# --- sessions (PTY-backed) --------------------------------------------
def post_session(session_id, output_chunk=None, started=False, done=False,
                 exit_code=None, error=False):
    body = {"session_id": session_id}
    if started:
        body["started"] = True
    if output_chunk:
        body["output_chunk"] = output_chunk
    if done:
        body.update({"done": True, "exit_code": exit_code, "error": error})
    try:
        _request("POST", "/api/session_output", body, expect_json=False, timeout=30)
    except (urllib.error.URLError, OSError) as e:
        log(f"failed to post session output for {session_id}: {e}")


def _session_failed(session_id, exc):
    log(f"session {session_id} failed to start: {exc}")
    post_session(session_id, output_chunk=f"failed to start: {exc}\n",
                 done=True, exit_code=-1, error=True)


def _session_argv(command, env):
    """Build the argv to launch a session's process on this platform."""
    if IS_WINDOWS:
        shell_bin = os.environ.get("COMSPEC", "cmd.exe")
        return [shell_bin, "/c", command] if command else [shell_bin]
    shell_bin = env.get("SHELL") or "/bin/bash"
    return [shell_bin, "-c", command] if command else [shell_bin]


def _session_encoding():
    """Bytes a session's output should be decoded with.

    POSIX terminals emit UTF-8 in practice. On Windows, cmd.exe writes in the
    console OEM code page (cp936 on Chinese systems), so decoding as UTF-8
    garbles every non-ASCII character. Prefer the active console code page and
    fall back to UTF-8.
    """
    if not IS_WINDOWS:
        return "utf-8"
    try:
        import ctypes
        cp = ctypes.windll.kernel32.GetConsoleOutputCP()
        if cp:
            return "cp%d" % cp
    except Exception:
        pass
    return "mbcs"


def _make_decoder(encoding):
    """Incremental decoder that survives multi-byte chars split across reads."""
    try:
        import codecs
        dec = codecs.getincrementaldecoder(encoding)(errors="replace")
    except (LookupError, TypeError):
        import codecs
        dec = codecs.getincrementaldecoder("utf-8")(errors="replace")
    return dec


def start_session(ctl):
    session_id = ctl["session_id"]
    command = ctl.get("command")
    cwd = ctl.get("cwd") or None
    env = os.environ.copy()
    env.update({str(k): str(v) for k, v in (ctl.get("env") or {}).items()})
    argv = _session_argv(command, env)
    log(f"opening session {session_id}: {command or argv[0]!r} "
        f"({'pty' if USE_PTY else 'pipe'})")

    if USE_PTY:
        import pty  # POSIX-only; imported lazily so Windows never loads it
        master_fd, slave_fd = pty.openpty()
        try:
            proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=slave_fd,
                                    stdout=slave_fd, stderr=slave_fd,
                                    close_fds=True, **_group_kwargs())
        except (OSError, ValueError) as e:
            os.close(master_fd); os.close(slave_fd)
            return _session_failed(session_id, e)
        os.close(slave_fd)
        read_fd = master_fd
        read_stream = None

        def writer(data):
            os.write(master_fd, data)

        def cleanup():
            try:
                os.close(master_fd)
            except OSError:
                pass
    else:
        try:
            proc = subprocess.Popen(argv, cwd=cwd, env=env,
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, bufsize=0,
                                    **_group_kwargs())
        except (OSError, ValueError) as e:
            return _session_failed(session_id, e)
        read_stream = proc.stdout
        # On Windows an anonymous pipe's fd is not usable with os.read(); use the
        # file object's own read(). POSIX keeps os.read() on the raw fd.
        read_fd = None if IS_WINDOWS else proc.stdout.fileno()

        def writer(data):
            proc.stdin.write(data)
            proc.stdin.flush()

        def cleanup():
            for stream in (proc.stdin, proc.stdout):
                try:
                    stream.close()
                except OSError:
                    pass

    with sessions_lock:
        sessions[session_id] = {"proc": proc, "writer": writer,
                                "encoding": _session_encoding(),
                                "lock": threading.Lock()}
    post_session(session_id, started=True)

    def reader():
        dec = _make_decoder(_session_encoding())
        try:
            while True:
                try:
                    if read_stream is not None:
                        data = read_stream.read(4096)
                    else:
                        data = os.read(read_fd, 4096)
                except (OSError, ValueError):
                    data = b""
                if not data:
                    # flush any dangling partial multi-byte sequence
                    tail = dec.decode(b"", final=True)
                    if tail:
                        post_session(session_id, output_chunk=tail)
                    break
                text = dec.decode(data)
                if text:
                    post_session(session_id, output_chunk=text)
        finally:
            exit_code = proc.wait()
            with sessions_lock:
                sessions.pop(session_id, None)
            cleanup()
            post_session(session_id, done=True, exit_code=exit_code,
                         error=(exit_code != 0))
            log(f"session {session_id} ended (exit {exit_code})")

    threading.Thread(target=reader, daemon=True).start()


def session_input(ctl):
    session_id = ctl["session_id"]
    with sessions_lock:
        s = sessions.get(session_id)
    if not s:
        return
    try:
        with s["lock"]:
            # Encode with the same code page we decode output with, so typing
            # non-ASCII into a Windows console session arrives intact.
            s["writer"](ctl.get("data", "").encode(s.get("encoding") or "utf-8",
                                                   errors="replace"))
    except OSError as e:
        log(f"session {session_id} input failed: {e}")


def session_close(ctl):
    session_id = ctl["session_id"]
    with sessions_lock:
        s = sessions.get(session_id)
    if not s:
        return
    log(f"closing session {session_id}")
    kill_tree(s["proc"])


def do_pull(ctl):
    upload_file(ctl["path"], artifact_id=ctl["artifact_id"])


def handle_control(ctl):
    kind = ctl.get("type")
    if kind == "session_start":
        start_session(ctl)
    elif kind == "session_input":
        session_input(ctl)
    elif kind == "session_close":
        session_close(ctl)
    elif kind == "pull":
        threading.Thread(target=do_pull, args=(ctl,), daemon=True).start()
    else:
        log(f"ignoring unknown control message: {kind}")


# --- main control loop -------------------------------------------------
def poll_loop(worker_id):
    backoff = 1
    while True:
        try:
            status, data = _request("GET", f"/api/poll?worker_id={worker_id}",
                                     expect_json=True, timeout=120)
            backoff = 1
            for ctl in data.get("controls", []):
                handle_control(ctl)
            if data.get("job"):
                oneshot_q.put(data["job"])
        except urllib.error.HTTPError as e:
            if e.code == 409:
                log("coordinator forgot us; re-registering")
                return
            log(f"poll HTTP error {e.code}; backing off {backoff}s")
            time.sleep(backoff); backoff = min(backoff * 2, 30)
        except (urllib.error.URLError, OSError) as e:
            log(f"poll failed ({e}); backing off {backoff}s")
            time.sleep(backoff); backoff = min(backoff * 2, 30)


def main():
    if not SERVER or not TOKEN:
        print("error: set RELAY_SERVER and RELAY_WORKER_TOKEN", file=sys.stderr)
        sys.exit(2)
    print("=" * 64, flush=True)
    print("compute-relay worker agent", flush=True)
    print(f"  coordinator : {SERVER}", flush=True)
    print(f"  worker name : {NAME}", flush=True)
    print("Runs jobs and sessions the coordinator sends. Stop with Ctrl-C.", flush=True)
    print("=" * 64, flush=True)

    threading.Thread(target=oneshot_worker, daemon=True).start()
    try:
        while True:
            worker_id = register()
            poll_loop(worker_id)
    except KeyboardInterrupt:
        log("stopped by user")


if __name__ == "__main__":
    main()
