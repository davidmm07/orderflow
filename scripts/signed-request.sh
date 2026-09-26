#!/usr/bin/env bash
# Sends one signed request to a running Orderflow server.
#
# Usage: scripts/signed-request.sh METHOD PATH [JSON_BODY]
#
# Credentials come from ORDERFLOW_KEY_ID and ORDERFLOW_SECRET, or from the
# first entry of ORDERFLOW_API_CREDENTIALS in .env. The secret is passed to
# the signer through the environment, never as a command line argument.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -lt 2 ]]; then
  echo "usage: $0 METHOD PATH [JSON_BODY]" >&2
  exit 1
fi
method=$1
path=$2
body=${3:-}
url=${ORDERFLOW_URL:-http://127.0.0.1:8080}

if [[ -z "${ORDERFLOW_KEY_ID:-}" || -z "${ORDERFLOW_SECRET:-}" ]]; then
  if [[ ! -f .env ]]; then
    echo "No credentials: set ORDERFLOW_KEY_ID and ORDERFLOW_SECRET, or run make env." >&2
    exit 1
  fi
  entry=$(grep -E '^ORDERFLOW_API_CREDENTIALS=' .env | head -n1 | cut -d= -f2- | cut -d, -f1)
  ORDERFLOW_KEY_ID=${entry%%:*}
  ORDERFLOW_SECRET=${entry#*:*:}
fi
export ORDERFLOW_SECRET

timestamp=$(date +%s)
signature=$(cargo run -q -p orderflow-api --example sign -- "$timestamp" "$method" "$path" "$body")

args=(-sS -X "$method" "${url}${path}"
  -H "content-type: application/json"
  -H "x-orderflow-key: ${ORDERFLOW_KEY_ID}"
  -H "x-orderflow-timestamp: ${timestamp}"
  -H "x-orderflow-signature: ${signature}"
  -w '\nHTTP %{http_code}\n')
if [[ -n "$body" ]]; then
  args+=(--data "$body")
fi
curl "${args[@]}"
