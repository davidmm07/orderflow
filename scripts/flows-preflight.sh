#!/usr/bin/env bash
# Checks that the services the scenario flows call are running before the
# Postman CLI starts. A request to a service that is down only reports
# "connect ECONNREFUSED" and runs none of its tests, so this script says
# which service is missing and the command that starts it.
#
#   scripts/flows-preflight.sh ENV_FILE [FLOW]
#
# ENV_FILE is the Postman environment (Native Git YAML). FLOW is one flow
# folder such as "06 Stop orders"; without it every flow is checked. Which
# services a flow needs is read from the URLs in its request files, so a new
# flow needs no change here.
set -euo pipefail

cd "$(dirname "$0")/.."

env_file=${1:?usage: flows-preflight.sh ENV_FILE [FLOW]}
flow=${2:-}
collection="postman/collections/Orderflow scenarios"

if [[ ! -f "$env_file" ]]; then
  echo "No Postman environment at $env_file." >&2
  echo "  Create it from your .env:  make postman-env" >&2
  exit 1
fi

target="$collection"
if [[ -n "$flow" ]]; then
  target="$collection/$flow"
  if [[ ! -d "$target" ]]; then
    echo "No flow named \"$flow\". The flows are:" >&2
    find "$collection" -mindepth 1 -maxdepth 1 -type d ! -name '.*' -printf '  %f\n' | sort >&2
    exit 1
  fi
fi

# Value of one variable in the environment file; only URLs are read, the
# trader secrets in the same file are never touched.
value() {
  awk -v key="$1" '
    $0 ~ "key: " key "$" { found = 1; next }
    found && /value:/ { sub(/^[^:]*: */, ""); gsub(/\x27/, ""); print; exit }
  ' "$env_file"
}

uses() {
  grep -rqsF --include='*.request.yaml' "{{$1}}" "$target"
}

# Prints the HTTP status of URL, or 000 when nothing answers.
status() {
  curl -s -o /dev/null -m 3 -w '%{http_code}' "$1" || true
}

missing=0

if uses base_url; then
  url=$(value base_url)
  url=${url:-http://127.0.0.1:8080}
  code=$(status "$url/health/ready")
  if [[ "$code" != "200" ]]; then
    missing=1
    echo "Orderflow is not ready at $url (HTTP $code; 000 means nothing is listening)." >&2
    echo "  Start it:               make run" >&2
    echo "  Or the whole stack:     make up" >&2
  fi
fi

if uses pricefeed_url; then
  url=$(value pricefeed_url)
  url=${url:-http://127.0.0.1:8090}
  code=$(status "$url/health/ready")
  if [[ "$code" != "200" ]]; then
    missing=1
    echo "The price feed is not ready at $url (HTTP $code; 000 means nothing is listening)." >&2
    echo "  Flow \"09 Market data\" calls it." >&2
    echo "  Start it:               make run-pricefeed" >&2
    echo "  Without internet:       PRICEFEED_SOURCES=none make run-pricefeed" >&2
    echo "  Or the whole stack:     make up" >&2
    if [[ -z "$flow" ]]; then
      echo "  To run only one flow:   make flows FLOW=\"02 Limit order lifecycle\"" >&2
    fi
  fi
fi

exit "$missing"
