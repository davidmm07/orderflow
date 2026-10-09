#!/usr/bin/env bash
# Runs the storage adapter tests that need real services (marked #[ignore])
# against throwaway Postgres and Valkey containers on side ports, then
# removes the containers. The database password is random and never leaves
# this shell.
#
#   scripts/integration-test.sh
#   PG_PORT=55440 REDIS_PORT=56390 scripts/integration-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

pg_port=${PG_PORT:-55432}
redis_port=${REDIS_PORT:-56379}
postgres="orderflow-it-postgres-$$"
valkey="orderflow-it-valkey-$$"
password=$(openssl rand -hex 16)

cleanup() {
  docker rm -f "$postgres" "$valkey" > /dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "$postgres" \
  -e POSTGRES_USER=pricefeed -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB=pricefeed \
  -p "127.0.0.1:${pg_port}:5432" postgres:18.6-alpine > /dev/null
docker run -d --name "$valkey" -p "127.0.0.1:${redis_port}:6379" valkey/valkey:8.1.10-alpine > /dev/null

# The image runs a temporary server during initialization; checking over TCP
# waits for the real one.
for _ in $(seq 1 60); do
  if docker exec "$postgres" pg_isready -h 127.0.0.1 -U pricefeed -d pricefeed > /dev/null 2>&1; then
    break
  fi
  sleep 1
done

PRICEFEED_TEST_DATABASE_URL="postgres://pricefeed:${password}@127.0.0.1:${pg_port}/pricefeed" \
PRICEFEED_TEST_REDIS_URL="redis://127.0.0.1:${redis_port}" \
  cargo test -p orderflow-storage -- --ignored
