# compute-relay

A tiny self-hosted **distributed compute task runner**. Run one coordinator on a
host with a public domain; enroll worker machines **you own or are authorized to
manage** (your own boxes, friends' HPC rigs they've agreed to lend). Workers pull
jobs and run them — GPU scripts, batches of Android emulators, long simulations —
and stream output back to you.

It runs three kinds of work:

- **one-shot jobs** — run a command, stream its text output, get the exit code.
- **sessions** — a long-lived PTY-backed process on a chosen worker (e.g. an
  `adb shell` or an interactive debugger) that you feed input to and read output
  from. **Stateful across commands**, which is what APK joint-debugging needs.
- **artifacts** — binary files pulled back from a worker: screenshots, screen
  recordings, logs. Uploaded in chunks, so large recordings are fine.

Think "a minimal self-hosted CI-runner pool," not magic. It is:

- **Single-file, zero-dependency** — pure Python 3.8+ standard library on both ends.
- **One-command deploy** on the server, **one-line enroll** on each worker.
- **NAT-friendly** — workers only make outbound HTTPS requests (long-poll); no
  inbound ports on the worker side.
- **Authenticated** — a worker token to join the pool, a separate admin token to
  submit jobs. It is never an open relay.
- **Transparent** — the agent prints the coordinator it connects to and every
  command it runs. No stealth, no persistence tricks, no self-propagation.

> **Use it only on machines you own or have explicit permission to run on.** By
> design the agent executes the commands the coordinator sends it, so only point
> a worker at a coordinator you control, and only hand the worker token to people
> whose machines you're authorized to enroll.

## Architecture

```
  you ──(admin token)──▶ ┌─────────────┐ ◀──(worker token, outbound long-poll)── worker A
   client.py             │ coordinator │ ◀───────────────────────────────────────  worker B
                         │  server.py  │ ◀───────────────────────────────────────  worker C
                         └─────────────┘
```

- `server.py` — the coordinator. Holds the job queue and worker registry in
  memory, serves the worker bootstrap, and exposes the admin API.
- `agent.py` — the worker. Registers, long-polls for jobs, runs them, streams
  output back. Reconnects with backoff.
- `client.py` — your CLI to submit jobs and inspect workers/results.
- `ui.html` — the web dashboard, served by the coordinator at `/ui`.
- `install.sh` — the worker bootstrap the coordinator serves at `/install.sh`.

## 1. Deploy the coordinator (server side)

On your public host:

```sh
git clone <this-repo> compute-relay && cd compute-relay
python3 server.py
```

That's it. It listens on `0.0.0.0:8787` and prints a generated **worker token**
and **admin token** (persisted to `relay-tokens.json`, so restarts keep them).
To set them yourself instead:

```sh
RELAY_WORKER_TOKEN=... RELAY_ADMIN_TOKEN=... RELAY_PORT=8787 python3 server.py
```

Put it behind your domain with TLS. Any reverse proxy works; e.g. Caddy gives
you HTTPS in two lines:

```
relay.example.com {
    reverse_proxy 127.0.0.1:8787
}
```

Server environment variables:

| var | default | meaning |
|-----|---------|---------|
| `RELAY_HOST` | `0.0.0.0` | bind address |
| `RELAY_PORT` / `PORT` | `8787` | bind port |
| `RELAY_WORKER_TOKEN` | generated | token workers use to join |
| `RELAY_ADMIN_TOKEN` | generated | token you use to submit jobs |
| `RELAY_TOKENS_FILE` | `./relay-tokens.json` | where generated tokens persist |
| `RELAY_PUBLIC_URL` | `http://127.0.0.1:<port>` | base URL used in the printed dashboard link |
| `RELAY_UI_LINK_TTL` | `600` | seconds a one-time dashboard sign-in link stays valid |
| `RELAY_UI_SESSION_TTL` | `604800` | seconds a dashboard session cookie stays valid |

## 2. Enroll a worker (node side)

One line on each machine you want to lend to the pool:

```sh
curl -fsSL https://relay.example.com/install.sh \
  | RELAY_SERVER=https://relay.example.com RELAY_WORKER_TOKEN=YOUR_WORKER_TOKEN sh
```

It downloads `agent.py` into `~/.compute-relay` and runs it in the foreground,
logging what it does. To run it as a managed service instead, see
[`deploy/compute-relay-worker.service`](deploy/compute-relay-worker.service).

Worker environment variables: `RELAY_SERVER`, `RELAY_WORKER_TOKEN`,
`RELAY_WORKER_NAME` (default: hostname), `RELAY_DIR` (default: `~/.compute-relay`).

## 3. Submit jobs (your machine)

```sh
export RELAY_SERVER=https://relay.example.com
export RELAY_ADMIN_TOKEN=YOUR_ADMIN_TOKEN

python3 client.py workers                      # see who's online + their GPUs
python3 client.py run "nvidia-smi"             # run + stream output live
python3 client.py run --target gpu-box "python train.py --epochs 50"
python3 client.py submit "./long_batch.sh"     # fire-and-forget, prints a job id
python3 client.py status <job_id>              # check it later
python3 client.py jobs                          # recent jobs
```

Useful flags on `run`/`submit`:

- `--target <name|id>` pin the job to a specific worker (else any eligible one runs it)
- `--cwd <path>` working directory on the worker
- `--timeout <seconds>` kill the job (and its whole process tree) after N seconds
- `--env K=V` (repeatable) extra environment variables
- `--artifact <path>` (repeatable) a file on the worker to upload back after the job
- `--no-shell` exec the argv directly instead of through a shell
- `--label <text>` a human label for the job

### Web dashboard

The coordinator also serves a browser dashboard at **`/ui`**. There is no
login form: you open it from a **one-time sign-in link**, which the browser
trades for a session cookie. `server.py` prints a link at startup; mint a
fresh one any time with:

```sh
python3 client.py ui          # prints https://relay.example.com/ui#login=...
python3 client.py ui --open   # ...and opens it in your browser
```

From the dashboard you can:

- **Workers**: who's online or busy, their platform, CPUs and GPUs, plus the
  enroll one-liner for new machines.
- **Jobs**: submit commands (target, timeout, env, cwd, file uploads, binary
  stdout), watch output stream live, filter and search history, and re-run a
  job with edits.
- **Sessions**: open a PTY session on a worker and drive it from a terminal
  pane, with line history, `^C`, `^D` and Tab.
- **Artifacts**: pull files, take screenshots, and preview images, video and
  text inline or download them.

How sign-in works:

- A link works **once** and expires after 10 minutes (`RELAY_UI_LINK_TTL`).
  Its token sits in the URL fragment (`#login=...`), which browsers never send
  to the server. That keeps it out of proxy access logs and `Referer` headers,
  and the page removes it from the address bar as soon as it loads.
- The page swaps the token for a random session id held in an `HttpOnly`,
  `SameSite=Strict` cookie, marked `Secure` when your proxy sends
  `X-Forwarded-Proto: https`. Page scripts can't read it, and the admin token
  itself never reaches the browser.
- Sessions last 7 days (`RELAY_UI_SESSION_TTL`) and end when you sign out or
  the coordinator restarts (they're held in memory, like everything else).
- A cookie only authenticates requests that also carry an `X-Relay-UI` header.
  Cross-site forms can't set that header, which blocks CSRF. Bearer-token auth
  for the CLI and workers is unchanged.

It's a single static file (`ui.html`) with no build step and no external
requests. The coordinator serves it with a strict Content-Security-Policy (the
page's script pinned by hash, same-origin requests only), so text reported by
workers can only ever be displayed, never executed.

## 4. Stateful sessions (interactive debugging)

A **session** is a long-lived PTY-backed process on a specific worker. Input you
send and output you read persist across calls, so it behaves like a real
terminal on that machine — ideal for `adb shell` or a debugger during APK
bring-up.

```sh
# open + attach in one step (type into it, Ctrl-D detaches, Ctrl-C closes)
python3 client.py session shell --target pixel-host --cmd "adb shell"

# or script it without a live terminal:
SID=$(python3 client.py session open --target pixel-host --cmd "adb shell")
python3 client.py session send $SID "getprop ro.product.model"
python3 client.py session send $SID "input tap 540 1200"
python3 client.py session list
python3 client.py session close $SID
```

Note: because tools like `adb` are clients of a persistent daemon, even plain
one-shot jobs targeting the same node (`run --target pixel-host "adb shell ..."`)
hit the same running emulator. A session adds a single stateful shell you drive
incrementally (shell variables, `cd`, a REPL, an interactive debugger).

## 5. Screenshots, recordings, and file pulls

Binary feedback comes back as **artifacts**:

```sh
# capture a screenshot (command's stdout is the PNG)
python3 client.py screenshot --target pixel-host --out shot.png

# record the device screen for 10s and pull the mp4 back
python3 client.py record --target pixel-host --seconds 10 --out demo.mp4

# pull any file off a worker
python3 client.py pull --target pixel-host --out trace.bin /sdcard/trace.bin

# capture arbitrary binary stdout from any command
python3 client.py run --target node --binary-out out.bin "my-tool --emit-binary"

python3 client.py artifacts                 # list what's been collected
python3 client.py fetch <artifact_id> --out file.bin
```

`screenshot` defaults its capture command to `adb exec-out screencap -p`; pass
`--cmd` to override for a different device/tool. `record` uses
`adb shell screenrecord`. Both are thin conveniences over the generic
binary-stdout and pull mechanisms, so non-adb workflows work too. Artifacts are
uploaded in chunks and held in memory on the coordinator (cap set by
`RELAY_MAX_ARTIFACT_BYTES`, default 512 MB). list

## HTTP API

All `/api/*` endpoints require `Authorization: Bearer <token>` (worker or admin
as noted). Admin endpoints also accept the dashboard's session cookie together
with an `X-Relay-UI: 1` header.

| method & path | token | purpose |
|---------------|-------|---------|
| `GET /healthz` | — | liveness |
| `GET /ui` | — | web dashboard page |
| `POST /api/ui-link` | admin | mint a one-time dashboard sign-in link |
| `POST /api/session` | link token (body) | redeem a link for a session cookie |
| `POST /api/logout` | session cookie | end the dashboard session |
| `GET /install.sh`, `GET /agent.py` | — | worker bootstrap |
| `POST /api/register` | worker | enroll, returns `worker_id` |
| `GET /api/poll?worker_id=` | worker | long-poll for the next job |
| `POST /api/result` | worker | stream output / report completion |
| `POST /api/jobs` | admin | submit a job |
| `GET /api/jobs` | admin | list jobs |
| `GET /api/jobs/<id>` | admin | job status + accumulated output |
| `GET /api/workers` | admin | list workers |

## Security notes

- Serve the coordinator over **HTTPS** so tokens aren't sent in the clear.
- The **admin token can run commands on every worker** — keep it secret; the
  worker token only lets a machine *join* and receive jobs.
- Run the agent as an **unprivileged user**; the sample systemd unit adds basic
  sandboxing you can tighten per workload.
- State is in memory: restarting the coordinator clears the job history and
  workers re-register on their next poll. Swap in a datastore if you need
  durable history.

## Platform notes

The agent runs on **Linux, macOS, and Windows**. On POSIX, sessions use a real
PTY (full terminal semantics). Windows has no `termios`/`pty`, so there sessions
fall back to a **pipe-backed** process — no prompt echo or TTY-only behavior,
but line-oriented tools like `adb shell` work fine. Set `RELAY_NO_PTY=1` to force
the pipe path on POSIX too. One-shot jobs, artifacts, and the server/client are
fully cross-platform.

## Limitations

- In-memory state (no persistence across coordinator restarts).
- At-most-once dispatch, no automatic retry of a job whose worker dies mid-run.
- Suited to a trusted pool of a handful to a few dozen workers, not a public
  multi-tenant service.

## License

MIT — see [LICENSE](LICENSE).
