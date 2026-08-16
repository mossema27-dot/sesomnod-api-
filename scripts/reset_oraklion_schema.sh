#!/usr/bin/env bash
# reset_oraklion_schema.sh — DROP + recreate oraklion-schemaet.
#
# Bruk KUN før første ekte event når du trenger tom chain, eller
# etter test-seed som har brutt chain_hash-integriteten.
#
# Rekkefølge er kritisk: mentor-tabellene ligger i samme skjema,
# så DROP CASCADE fjerner dem også. Vi ma re-kjore begge DDL-er:
#   1. oraklion_brain_schema.sql (Don sin fil)
#   2. services/oraklion_mentor_ddl.sql (safe_sessions + mentor_log +
#      audit_log + rate_limiter)
#
# Krever: railway CLI linket til Postgres-service med DATABASE_PUBLIC_URL.

set -euo pipefail

BRAIN_SCHEMA_PATH="${BRAIN_SCHEMA_PATH:-/Users/don/Downloads/oraklion_brain_schema.sql}"
MENTOR_DDL_PATH="${MENTOR_DDL_PATH:-/Users/don/sesomnod-api/services/oraklion_mentor_ddl.sql}"

if [ ! -f "$BRAIN_SCHEMA_PATH" ]; then
    echo "FEIL: brain-schema mangler pa $BRAIN_SCHEMA_PATH" >&2
    exit 1
fi
if [ ! -f "$MENTOR_DDL_PATH" ]; then
    echo "FEIL: mentor DDL mangler pa $MENTOR_DDL_PATH" >&2
    exit 1
fi

echo "==== DROP SCHEMA oraklion CASCADE ===="
railway run bash -c 'psql "$DATABASE_PUBLIC_URL" -c "DROP SCHEMA IF EXISTS oraklion CASCADE;"'

echo "==== Rebuild brain schema ===="
railway run bash -c "psql \"\$DATABASE_PUBLIC_URL\" -f \"$BRAIN_SCHEMA_PATH\"" | tail -5

echo "==== Rebuild mentor DDL ===="
railway run bash -c "psql \"\$DATABASE_PUBLIC_URL\" -f \"$MENTOR_DDL_PATH\"" | tail -5

echo "==== Verify chain empty ===="
railway run bash -c 'psql "$DATABASE_PUBLIC_URL" -c "SELECT * FROM oraklion.verify_chain();"'

echo "==== Verify all 11 tables present ===="
railway run bash -c 'psql "$DATABASE_PUBLIC_URL" -c "SELECT table_name FROM information_schema.tables WHERE table_schema = '"'"'oraklion'"'"' ORDER BY table_name;"'
