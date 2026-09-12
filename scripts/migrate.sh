#!/usr/bin/env bash
# Migration ledger runner (spec #52, B4 / US19): applies every pending
# migration per service database inside one transaction each, recording the
# version in schema_migrations so a partial apply cannot silently replay and
# rollback/forward is machine-verifiable.
#
# Usage: scripts/migrate.sh <psql-cmd>
#   e.g. scripts/migrate.sh "docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres"
#   or   scripts/migrate.sh "psql -h localhost -U postgres"
#
# Replaces the raw CI glob: an interrupted apply now leaves the partial
# version recorded-or-not (transactional), and a recorded version is never
# re-applied.
set -euo pipefail

PSQL="${1:?usage: scripts/migrate.sh <psql-cmd>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Service -> database, matching CI's mapping order. MIGRATE_SERVICES
# overrides (colon pairs, space-separated) for testing or one-off runs.
if [[ -n "${MIGRATE_SERVICES:-}" ]]; then
  read -ra SERVICES <<<"$MIGRATE_SERVICES"
else
  SERVICES=(stock:taper_db reservation:reservation_db order:order_db channelsync:channelsync_db)
fi

ensure_ledger() {
  local db="$1"
  $PSQL -d "$db" -q <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
SQL
}

applied_versions() {
  local db="$1"
  $PSQL -d "$db" -At -c "SELECT version FROM schema_migrations ORDER BY version;" || true
}

for pair in "${SERVICES[@]}"; do
  svc="${pair%%:*}"; db="${pair##*:}"
  echo "== $svc -> $db"

  ensure_ledger "$db"
  mapfile -t done < <(applied_versions "$db")
  declare -A seen=()
  for v in "${done[@]}"; do seen["$v"]=1; done

  shopt -s nullglob
  for up in "$ROOT"/db/migrations/"$svc"/*.up.sql; do
    base="$(basename "$up" .up.sql)"          # e.g. 000004_indexes_constraints
    version="${base%%_*}"                     # e.g. 000004
    if [[ -n "${seen[${version}]:-}" ]]; then
      echo "   already applied: $base"
      continue
    fi
    echo "   applying: $base"
    # One transaction per migration: the SQL and the ledger insert commit
    # together or not at all, so a partial apply cannot replay silently.
    {
      cat "$up"
      echo
      echo "INSERT INTO schema_migrations (version) VALUES ('$version');"
    } | $PSQL -d "$db" -v ON_ERROR_STOP=1 -q
  done

  # Down files are never auto-run, but a version without a matching down
  # pair is a repo defect: flag it.
  for up in "$ROOT"/db/migrations/"$svc"/*.up.sql; do
    down="${up%.up.sql}.down.sql"
    if [[ ! -f "$down" ]]; then
      echo "   WARNING: missing down migration for $(basename "$up")" >&2
      exit 1
    fi
  done
done

echo "migrations complete"
