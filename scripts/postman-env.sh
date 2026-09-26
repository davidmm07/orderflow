#!/usr/bin/env bash
# Writes the "Orderflow local" Postman environment (Native Git YAML format)
# with the first two API credentials from .env, so the scenario flows in
# postman/collections can sign requests. The Postman app lists it
# automatically when the repository folder is open in Local View.
#
# The output holds secrets. It is git-ignored; never commit or share it.
#
#   ENV_FILE       source of the credentials (default: .env)
#   OUTPUT         environment file to write (default: postman/environments/orderflow-local.environment.yaml)
#   ORDERFLOW_URL  server base URL           (default: from ORDERFLOW_BIND_ADDR)
set -euo pipefail

cd "$(dirname "$0")/.."

env_file=${ENV_FILE:-.env}
output=${OUTPUT:-postman/environments/orderflow-local.environment.yaml}

if [[ ! -f "$env_file" ]]; then
  echo "No $env_file found; run make env first." >&2
  exit 1
fi

value() {
  grep -E "^$1=" "$env_file" | head -n1 | cut -d= -f2- || true
}

IFS=',' read -r -a entries <<<"$(value ORDERFLOW_API_CREDENTIALS)"
if [[ ${#entries[@]} -lt 2 ]]; then
  echo "The flows need two traders but $env_file has ${#entries[@]}." >&2
  echo "Run scripts/bootstrap-env.sh --add-trader, restart the server, then retry." >&2
  exit 1
fi

bind=$(value ORDERFLOW_BIND_ADDR)
base_url=${ORDERFLOW_URL:-http://${bind:-127.0.0.1:8080}}

field() {
  local entry=$1 index=$2
  case $index in
    key) printf '%s' "${entry%%:*}" ;;
    secret) printf '%s' "${entry#*:*:}" ;;
  esac
}

# Key ids and secrets are restricted to characters that need no YAML quoting
# beyond the single quotes used here.
mkdir -p "$(dirname "$output")"
(umask 077 && cat >"$output" <<YAML
name: Orderflow local
values:
  - key: base_url
    value: '${base_url}'
  - key: trader_a_key
    value: '$(field "${entries[0]}" key)'
  - key: trader_a_secret
    value: '$(field "${entries[0]}" secret)'
  - key: trader_b_key
    value: '$(field "${entries[1]}" key)'
  - key: trader_b_secret
    value: '$(field "${entries[1]}" secret)'
YAML
)
echo "Wrote $output for $base_url."
echo "In Postman, select the \"Orderflow local\" environment; from a terminal, run make flows."
