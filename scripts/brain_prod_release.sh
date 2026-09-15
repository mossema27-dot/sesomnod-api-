#!/usr/bin/env bash
# Oraklion Brain v2 — PROD-RELEASE av M1 (autorisert av Don 2026-09-16). Kjøres på Macen.
#   bash /Users/don/sesomnod-api/scripts/brain_prod_release.sh            # alle faser i rekkefølge, stopper ved feil
#   bash /Users/don/sesomnod-api/scripts/brain_prod_release.sh <fase>     # én fase: preflight|push|wait-backend|migrate|activate|wait-tick|frontend|verify|status
# Prinsipper: ingen `git add .`, ingen reset, ingen sletting. Hver fase er idempotent og kan kjøres på nytt.
set -euo pipefail
API_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_ROOT="${BRAIN_WEB_ROOT:-$(dirname "$API_ROOT")/sesomnod}"
API="https://sesomnod-api-production.up.railway.app"
SITE="https://sesomnod.com"
PG_SERVICE="${RAILWAY_PG_SERVICE:-Postgres}"
API_SERVICE="${RAILWAY_API_SERVICE:-sesomnod-api-}"
EXPECTED_ORIGIN_MAIN="${EXPECTED_ORIGIN_MAIN:-ccbb6b1336b73e78e87dde170f419f1b898a088f}"
PY="$API_ROOT/.brain_demo_venv/bin/python"
LOG="${TMPDIR:-/tmp}/brain_release_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$LOG") 2>&1
say()  { printf '\n\033[1;33m[release %s]\033[0m %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die()  { printf '\n\033[1;31m[release] STOPP:\033[0m %s\n(logg: %s)\n' "$*" "$LOG" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "mangler verktøy: $1 $2"; }
brain() { curl -s -m 20 "$API/public/oraklion/brain"; }
jsonf() { "$PY" -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

if ! command -v node >/dev/null 2>&1; then [ -s "$HOME/.nvm/nvm.sh" ] && . "$HOME/.nvm/nvm.sh"; fi

detect_pg_service() {  # finn Postgres-tjenestens navn i det linkede Railway-prosjektet (uten å skrive ut hemmeligheter)
  railway status --json 2>/dev/null | "$PY" -c '
import sys, json, re
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
names = []
def walk(o):
    if isinstance(o, dict):
        n = o.get("name")
        if isinstance(n, str): names.append(n)
        for v in o.values(): walk(v)
    elif isinstance(o, list):
        for v in o: walk(v)
walk(d)
for n in names:
    if re.search(r"postgres|pg", n, re.I): print(n); break
' 2>/dev/null || true
}

pg_python() {  # kjør tools/brain_prod_migrate.py med prod-DSN injisert av Railway, uten å skrive ut DSN
  local mode="$1"
  if [ -n "${BRAIN_PROD_DSN:-}" ]; then "$PY" "$API_ROOT/tools/brain_prod_migrate.py" "$mode"; return; fi
  if railway run -s "$PG_SERVICE" -- "$PY" "$API_ROOT/tools/brain_prod_migrate.py" "$mode"; then return 0; fi
  local detected; detected="$(detect_pg_service)"
  if [ -n "$detected" ] && [ "$detected" != "$PG_SERVICE" ]; then
    say "prøver Postgres-tjeneste '$detected' (fra railway status)"; PG_SERVICE="$detected"
    if railway run -s "$PG_SERVICE" -- "$PY" "$API_ROOT/tools/brain_prod_migrate.py" "$mode"; then return 0; fi
  fi
  say "railway run feilet — prøver DATABASE_PUBLIC_URL fra 'railway variables' (verdien vises ikke)"
  local dsn; dsn="$(railway variables -s "$PG_SERVICE" --kv 2>/dev/null | sed -n 's/^DATABASE_PUBLIC_URL=//p' | head -1 || true)"
  [ -n "$dsn" ] || die "fant ikke DATABASE_PUBLIC_URL for service '$PG_SERVICE'. Alternativer: (a) RAILWAY_PG_SERVICE=<navn> bash scripts/brain_prod_release.sh $([ "$mode" = --up ] && echo migrate || echo preflight)  (b) BRAIN_PROD_DSN=<public postgres-url> ...  (c) psql-veien i docs/MIGRATION_HOWTO.md (railway connect)."
  BRAIN_PROD_DSN="$dsn" "$PY" "$API_ROOT/tools/brain_prod_migrate.py" "$mode"
}

phase_preflight() {
  say "PREFLIGHT — verktøy, git-grunnlag, rollback-punkter, prod-baseline, DB-sjekk (kun lesing)"
  need git; need railway "(brew install railway / npm i -g @railway/cli, så 'railway login')"; need node; need curl
  [ -x "$PY" ] || die "$PY mangler — kjør 'bash scripts/brain_demo_up.sh' én gang (lager venv med asyncpg) og avbryt med Ctrl-C"
  "$PY" -c "import asyncpg" 2>/dev/null || "$PY" -m pip install -q asyncpg
  cd "$API_ROOT"
  [ "$(git rev-parse --abbrev-ref HEAD)" = "feat/brain-v2" ] || die "ikke på feat/brain-v2 (git checkout feat/brain-v2)"
  git diff --quiet -- main.py services/oraklion_brain tools migrations || die "ucommittede endringer i M1-filer — commit først"
  HEAD_SHA="$(git rev-parse --short HEAD)"; say "feat/brain-v2 HEAD = $HEAD_SHA"
  git fetch -q origin main
  OM="$(git rev-parse origin/main)"
  [ "$OM" = "$EXPECTED_ORIGIN_MAIN" ] || die "origin/main er $OM, forventet $EXPECTED_ORIGIN_MAIN — noen har pushet. Integrer først (git log origin/main), sett EXPECTED_ORIGIN_MAIN og kjør på nytt."
  git merge-base --is-ancestor origin/main feat/brain-v2 || die "feat/brain-v2 bygger ikke på origin/main — rebase/merge kreves"
  say "ROLLBACK backend = origin/main $(git rev-parse --short origin/main) (Railway: redeploy forrige deployment, eller git revert + push)"
  say "prod-baseline:"; curl -s -m 20 "$API/health" | head -c 200; echo
  printf 'brain-endepunkt nå (FORVENTET 404 før deploy): '; curl -s -m 20 -o /dev/null -w '%{http_code}\n' "$API/public/oraklion/brain"
  printf 'telemetry nå (200 = gammel kode uten 8c46de7): ';  curl -s -m 20 -o /dev/null -w '%{http_code}\n' "$API/public/oraklion/telemetry"
  say "ROLLBACK frontend = forrige Cloudflare Pages-deployment (øverste rad under):"
  (cd "$WEB_ROOT" && npx --yes wrangler pages deployment list --project-name=sesomnod 2>/dev/null | head -6) || say "wrangler-liste utilgjengelig (logg inn: npx wrangler login) — rollback via Cloudflare-dashboard er fortsatt mulig"
  say "DB-preflight (kun lesing, service '$PG_SERVICE')"
  pg_python --preflight || die "preflight feilet — se over (manglende kolonner = STOPP)"
}

phase_push() {
  say "PUSH — feat/brain-v2 → origin/main (Railway bygger automatisk)"
  cd "$API_ROOT"
  git push origin feat/brain-v2:main
  git fetch -q origin main && git branch -f main origin/main && say "lokal main flyttet til origin/main $(git rev-parse --short main)"
}

phase_wait_backend() {
  say "VENTER på Railway-deploy (Docker-bygg, typisk 3–8 min) — kriterium: /public/oraklion/brain svarer 200"
  for i in $(seq 1 90); do
    code="$(curl -s -m 20 -o /dev/null -w '%{http_code}' "$API/public/oraklion/brain" || true)"
    if [ "$code" = "200" ]; then break; fi
    sleep 10; [ $((i % 6)) -eq 0 ] && printf '  %s: fortsatt %s\n' "$(date -u +%H:%M:%S)" "$code"
    [ "$i" -eq 90 ] && die "backend svarte ikke 200 på /public/oraklion/brain innen 15 min — sjekk Railway → Deployments (gammel deploy ligger fortsatt live)"
  done
  say "ny backend live:"; curl -s -m 20 "$API/health" | head -c 160; echo
  printf 'brain før migrering (FORVENTET source=schema_missing): '; brain | jsonf "d['source']+' degraded='+str(d['degraded'])+' engine='+d['data'].get('engine_state','?')"
  printf 'telemetry (FORVENTET 401/403 — operator-gate fra 8c46de7): '; curl -s -m 20 -o /dev/null -w '%{http_code}\n' "$API/public/oraklion/telemetry"
  printf 'state (Sniper-motoren uendret): '; curl -s -m 20 "$API/public/oraklion/state" | head -c 220; echo
}

phase_migrate() {
  say "MIGRERING — kun additiv, oraklion.brain_* (godkjent ALTER oraklion v2)"
  pg_python --up || die "migrering feilet — ingenting annet er rørt; se logg"
  say "venter på cache (60 s) og verifiserer tom tilstand + offentlig feltfilter"
  sleep 65
  brain > /tmp/brain_after_migrate.json
  jsonf "'source='+d['source']+' engine='+str(d['data'].get('engine_state'))+' detail='+str(d['data'].get('engine_detail'))+' decision='+str(d['data'].get('decision_state'))+' hold='+str(d['data'].get('hold_reason'))+' flat='+str(d['data'].get('flat'))+' latest='+str(d['data'].get('latest_decision'))" < /tmp/brain_after_migrate.json
  if grep -qiE 'railway|pinnacle|postgres|DATABASE|rlwy' /tmp/brain_after_migrate.json; then die "offentlig svar lekker interne navn — STOPP før aktivering"; fi
  curl -s -m 20 "$API/public/oraklion/brain/ledger.json?limit=5" | head -c 200; echo
  say "FORVENTET nå: source=live, engine=SCANNER_DOWN (NO_HEARTBEAT), decision=HOLD, flat=None, latest=None — jobben er ikke aktivert ennå"
}

phase_activate() {
  say "AKTIVERING — BRAIN_V2_JOBS=on på service '$API_SERVICE' (Railway redeployer; første tick 90 s etter oppstart)"
  if ! railway variables -s "$API_SERVICE" --set "BRAIN_V2_JOBS=on"; then
    say "CLI-sett feilet. Sett manuelt: Railway → sesomnod-api → Variables → BRAIN_V2_JOBS = on (redeploy skjer automatisk). Trykk Enter når det er gjort."
    read -r _
  fi
}

phase_wait_tick() {
  say "VENTER på første produksjons-tick (heartbeat) — inntil 12 min"
  for i in $(seq 1 72); do
    out="$(brain | jsonf "str(d['data'].get('engine_state'))+'|'+str(d['data'].get('hold_reason'))+'|'+str(d['data'].get('last_scan_ok_utc'))" 2>/dev/null || echo 'na|na|na')"
    eng="${out%%|*}"; rest="${out#*|}"; hold="${rest%%|*}"; last="${rest#*|}"
    if [ "$eng" = "RUNNING" ] || { [ "$hold" != "NO_HEARTBEAT" ] && [ "$last" != "None" ]; }; then break; fi
    sleep 10; [ $((i % 6)) -eq 0 ] && printf '  %s: engine=%s hold=%s\n' "$(date -u +%H:%M:%S)" "$eng" "$hold"
    [ "$i" -eq 72 ] && die "ingen heartbeat innen 12 min — sjekk Railway-logg for '[Brain]'. Deaktiver med BRAIN_V2_JOBS=off om nødvendig."
  done
  say "FØRSTE PRODUKSJONSSKANNING OBSERVERT:"
  brain | jsonf "'as_of='+d['as_of']+' engine='+str(d['data'].get('engine_state'))+' '+str(d['data'].get('engine_detail'))+' decision='+str(d['data'].get('decision_state'))+' hold='+str(d['data'].get('hold_reason'))+' scanned='+str(d['data'].get('scanned_n'))+' passed='+str(d['data'].get('passed_n'))+' skips='+str(d['data'].get('skip_counts'))+' last_scan_ok='+str(d['data'].get('last_scan_ok_utc'))+' chain_head='+str(d['data'].get('chain_head'))"
}

phase_frontend() {
  say "FRONTEND — bygg (VITE_BRAIN_V2 fra .env.production) og deploy til Cloudflare Pages"
  cd "$WEB_ROOT"
  grep -q '^VITE_BRAIN_V2=true' .env.production || die ".env.production mangler VITE_BRAIN_V2=true"
  grep -q '^VITE_API_BASE=https://sesomnod-api-production' .env.production || die ".env.production peker ikke på prod-API"
  npm run build
  ls dist/assets/ | grep -E '^oraklion-.*\.js$' >/dev/null || die "dist mangler oraklion-bundle"
  grep -l 'brain-v2' dist/assets/*.js >/dev/null || die "bygget inneholder ikke brain-v2-ruten (VITE_BRAIN_V2 ikke lest?)"
  npx --yes wrangler pages deploy dist --project-name=sesomnod --branch=main
}

phase_verify() {
  say "VERIFISERING — offentlig side + API"
  local served built
  served="$(curl -s -m 20 "$SITE/oraklion" | grep -oE 'oraklion-[A-Za-z0-9_-]+\.js' | head -1 || true)"
  built="$(ls "$WEB_ROOT/dist/assets/" 2>/dev/null | grep -E '^oraklion-.*\.js$' | head -1 || true)"
  printf 'servert bundle: %s · bygget: %s → %s\n' "$served" "$built" "$([ -n "$served" ] && [ "$served" = "$built" ] && echo SAMME || echo 'AVVIK (CDN-cache? vent 1–2 min og kjør verify igjen)')"
  curl -s -m 20 "$SITE/assets/$served" | grep -q 'brain-v2' && echo 'brain-v2-ruten er i den serverte bundelen' || echo 'brain-v2 IKKE funnet i servert bundle'
  printf 'API brain: '; brain | jsonf "'source='+d['source']+' engine='+str(d['data'].get('engine_state'))+' decision='+str(d['data'].get('decision_state'))+' hold='+str(d['data'].get('hold_reason'))"
  say "OFFENTLIG ADRESSE: $SITE/oraklion#/brain-v2"
}

phase_status() { say "DB-STATUS (kun lesing) — kjør igjen etter ≥15 min for å bevise at gjentatt tick ikke dupliserer"; pg_python --status; }

case "${1:-all}" in
  preflight) phase_preflight ;; push) phase_push ;; wait-backend) phase_wait_backend ;; migrate) phase_migrate ;;
  activate) phase_activate ;; wait-tick) phase_wait_tick ;; frontend) phase_frontend ;; verify) phase_verify ;; status) phase_status ;;
  all) phase_preflight; phase_push; phase_wait_backend; phase_migrate; phase_activate; phase_wait_tick; phase_frontend; phase_verify; phase_status ;;
  *) die "ukjent fase: $1" ;;
esac
say "FERDIG (logg: $LOG)"
