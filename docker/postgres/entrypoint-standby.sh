#!/bin/sh
# Bootstrap a physical streaming standby from the primary, then start postgres.
# Idempotent: an existing PGDATA (named volume reused) is started directly.
set -eu

PRIMARY_HOST="${PRIMARY_HOST:-db-primary}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
SLOT_NAME="${REPLICATION_SLOT:-standby1_slot}"
APP_NAME="${REPLICATION_APP_NAME:-standby1}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[standby] empty PGDATA: taking base backup from ${PRIMARY_HOST}:${PRIMARY_PORT}"
    # postgres image entrypoint chowns PGDATA to the postgres user; run the
    # backup as that user to keep ownership consistent.
    chown -R postgres:postgres "$PGDATA"
    chmod 0700 "$PGDATA"
    su-exec postgres pg_basebackup \
        -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" \
        -U replicator -D "$PGDATA" -Fp -Xs -P -R \
        --slot="$SLOT_NAME"
    echo "[standby] base backup complete; standby.signal + slot written"
fi

# Keep the primary connection settings explicit (pg_basebackup -R already
# writes standby.signal and primary_conninfo; re-asserting them keeps the
# standby working after compose recreations). Append only when missing so we
# never erase other settings living in postgresql.auto.conf.
AUTO="$PGDATA/postgresql.auto.conf"
if ! grep -q "^primary_conninfo" "$AUTO" 2>/dev/null; then
    echo "primary_conninfo = 'host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=replicator password=${PGPASSWORD} application_name=${APP_NAME}'" >> "$AUTO"
fi
if ! grep -q "^primary_slot_name" "$AUTO" 2>/dev/null; then
    echo "primary_slot_name = '${SLOT_NAME}'" >> "$AUTO"
fi

exec docker-entrypoint.sh "$@"
