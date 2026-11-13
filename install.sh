#!/bin/sh
# compute-relay worker bootstrap.
#
# Downloads the worker agent from your coordinator and runs it. Intended for
# machines you own or are authorized to manage. The agent is transparent: it
# logs the coordinator it connects to and every command it runs.
#
# Usage (one line):
#   curl -fsSL https://relay.example.com/install.sh \
#     | RELAY_SERVER=https://relay.example.com RELAY_WORKER_TOKEN=YOUR_TOKEN sh
#
# Optional env:
#   RELAY_WORKER_NAME   display name for this worker (default: hostname)
#   RELAY_DIR           install dir (default: $HOME/.compute-relay)
set -eu

: "${RELAY_SERVER:?set RELAY_SERVER to your coordinator URL}"
: "${RELAY_WORKER_TOKEN:?set RELAY_WORKER_TOKEN to the worker token}"

RELAY_DIR="${RELAY_DIR:-$HOME/.compute-relay}"
mkdir -p "$RELAY_DIR"

echo "compute-relay: downloading agent from $RELAY_SERVER/agent.py"
if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$RELAY_SERVER/agent.py" -o "$RELAY_DIR/agent.py"
elif command -v wget >/dev/null 2>&1; then
    wget -qO "$RELAY_DIR/agent.py" "$RELAY_SERVER/agent.py"
else
    echo "error: need curl or wget" >&2
    exit 1
fi

PY=python3
command -v "$PY" >/dev/null 2>&1 || PY=python

echo "compute-relay: starting agent (Ctrl-C to stop)"
exec env \
    RELAY_SERVER="$RELAY_SERVER" \
    RELAY_WORKER_TOKEN="$RELAY_WORKER_TOKEN" \
    RELAY_WORKER_NAME="${RELAY_WORKER_NAME:-$(hostname)}" \
    "$PY" "$RELAY_DIR/agent.py"
