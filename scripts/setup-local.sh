#!/usr/bin/env bash
# Ferryman phase 0, no-Docker path: two local postgres clusters on 5433/5434.
# Run as root on a Debian/Ubuntu host with postgresql-16 installed (incl. WSL).
#   sudo bash scripts/setup-local.sh
# Docker users want `docker compose up -d` instead; both produce the same DSNs:
#   postgres://ferryman:ferryman@127.0.0.1:5433/ferryman   (source)
#   postgres://ferryman:ferryman@127.0.0.1:5434/ferryman   (target)
# On WSL, localhost forwarding may not cover these ports; from Windows use the
# address from `wsl hostname -I` in place of 127.0.0.1.
set -euo pipefail

VER=16
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

make_cluster() {
    local name=$1 port=$2
    if ! pg_lsclusters -h | awk '{print $2}' | grep -qx "$name"; then
        pg_createcluster "$VER" "$name" -p "$port" >/dev/null
    fi
    local conf="/etc/postgresql/$VER/$name/postgresql.conf"
    # ponytail: append-and-restart. Last assignment wins in postgresql.conf, so
    # re-running this script is harmless even though it appends again.
    cat >> "$conf" <<EOF

# ferryman
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
listen_addresses = '*'
EOF
    echo "host all all 0.0.0.0/0 scram-sha-256" >> "/etc/postgresql/$VER/$name/pg_hba.conf"
    pg_ctlcluster "$VER" "$name" restart
}

make_cluster source 5433
make_cluster target 5434

for port in 5433 5434; do
    su postgres -c "psql -p $port -v ON_ERROR_STOP=1 -q" <<'EOF'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ferryman') THEN
        CREATE ROLE ferryman LOGIN SUPERUSER REPLICATION PASSWORD 'ferryman';
    END IF;
END $$;
EOF
    su postgres -c "psql -p $port -tAc \"SELECT 1 FROM pg_database WHERE datname='ferryman'\"" \
        | grep -q 1 || su postgres -c "createdb -p $port -O ferryman ferryman"
    su postgres -c "psql -p $port -d ferryman -v ON_ERROR_STOP=1 -q -f '$ROOT/sql/schema.sql'"
done

# seed source only; target starts empty and is filled by the backfill engine (phase 3)
su postgres -c "psql -p 5433 -d ferryman -v ON_ERROR_STOP=1 -q -f '$ROOT/sql/seed.sql'"

su postgres -c "psql -p 5433 -d ferryman -v ON_ERROR_STOP=1 -v expect_seed=1 -f '$ROOT/sql/verify.sql'"
su postgres -c "psql -p 5434 -d ferryman -v ON_ERROR_STOP=1 -v expect_seed=0 -f '$ROOT/sql/verify.sql'"

echo "phase 0 ready: source=5433 target=5434"
