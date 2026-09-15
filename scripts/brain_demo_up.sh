#!/usr/bin/env bash
# Oraklion Brain v2 — LOKAL DEMO (aldri prod). Starter backend (tools/brain_local_demo.py) +
# frontend (vite dev, VITE_BRAIN_V2=true) på Macen. Ctrl-C stopper begge.
#   bash /Users/don/sesomnod-api/scripts/brain_demo_up.sh
set -euo pipefail
API_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_ROOT="${BRAIN_DEMO_WEB_ROOT:-$(dirname "$API_ROOT")/sesomnod}"
API_PORT="${BRAIN_DEMO_PORT:-8100}"
WEB_PORT="${BRAIN_DEMO_WEB_PORT:-5173}"
LOG_DIR="${TMPDIR:-/tmp}/brain_demo"; mkdir -p "$LOG_DIR"
URL="http://localhost:${WEB_PORT}/oraklion.html#/brain-v2"

say() { printf '\033[1;33m[demo]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[demo] FEIL:\033[0m %s\n' "$*" >&2; exit 1; }

[ -d "$WEB_ROOT/node_modules" ] || die "fant ikke frontend-repo med node_modules: $WEB_ROOT (sett BRAIN_DEMO_WEB_ROOT)"
for p in "$API_PORT" "$WEB_PORT"; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then die "port $p er opptatt (lsof -nP -iTCP:$p). Sett BRAIN_DEMO_PORT / BRAIN_DEMO_WEB_PORT."; fi
done
if ! command -v node >/dev/null 2>&1; then [ -s "$HOME/.nvm/nvm.sh" ] && . "$HOME/.nvm/nvm.sh"; fi
command -v node >/dev/null 2>&1 || die "node ikke funnet (nvm?)"
PY="${PYTHON:-python3}"; command -v "$PY" >/dev/null 2>&1 || die "python3 ikke funnet"

# ── backend ──
cd "$API_ROOT"
[ -x .brain_demo_venv/bin/python ] || "$PY" -m venv .brain_demo_venv
. .brain_demo_venv/bin/activate
say "installerer asyncpg fastapi uvicorn pgserver i .brain_demo_venv (kun første gang tar tid)…"
python -m pip install -q --disable-pip-version-check asyncpg fastapi "uvicorn[standard]" pgserver
BRAIN_DEMO_PORT="$API_PORT" python tools/brain_local_demo.py > "$LOG_DIR/backend.log" 2>&1 &
API_PID=$!
for i in $(seq 1 90); do
  sleep 1
  curl -s -m 2 "http://127.0.0.1:${API_PORT}/health" | grep -q '"mode":"DEMO"' && break
  kill -0 "$API_PID" 2>/dev/null || { tail -40 "$LOG_DIR/backend.log"; die "backend døde — se $LOG_DIR/backend.log"; }
  [ "$i" -eq 90 ] && { tail -40 "$LOG_DIR/backend.log"; die "backend svarte ikke innen 90 s"; }
done
say "backend DEMO oppe: http://127.0.0.1:${API_PORT}/public/oraklion/brain  (logg: $LOG_DIR/backend.log)"

# ── frontend ──
cd "$WEB_ROOT"
VITE_BRAIN_V2=true VITE_API_BASE="http://127.0.0.1:${API_PORT}" \
  node node_modules/vite/bin/vite.js --port "$WEB_PORT" --strictPort > "$LOG_DIR/frontend.log" 2>&1 &
WEB_PID=$!
for i in $(seq 1 60); do
  sleep 1
  curl -s -m 2 -o /dev/null "http://localhost:${WEB_PORT}/oraklion.html" && break
  kill -0 "$WEB_PID" 2>/dev/null || { tail -40 "$LOG_DIR/frontend.log"; kill "$API_PID" 2>/dev/null; die "frontend døde — se $LOG_DIR/frontend.log"; }
  [ "$i" -eq 60 ] && { kill "$API_PID" 2>/dev/null; die "frontend svarte ikke innen 60 s"; }
done
say "frontend oppe (logg: $LOG_DIR/frontend.log)"
echo
say "ÅPNE I CHROME:  $URL"
say "Siden viser gult DEMO-banner. Ingen prod-skriving, ingen Telegram, ingen push/deploy."
say "Ctrl-C her stopper backend + frontend."
trap 'say "stopper…"; kill "$API_PID" "$WEB_PID" 2>/dev/null; wait 2>/dev/null; exit 0' INT TERM
wait
