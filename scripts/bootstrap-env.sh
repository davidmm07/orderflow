#!/usr/bin/env bash
# Creates a local .env from .env.example with two traders, each with a
# random API secret. Two accounts are needed to see trades, because an
# account never trades with itself.
#
#   scripts/bootstrap-env.sh               create .env (refuses to overwrite)
#   scripts/bootstrap-env.sh --add-trader  append one more trader to .env
set -euo pipefail

cd "$(dirname "$0")/.."

credential() {
  printf '%s-key:%s:%s' "$1" "$1" "$(openssl rand -hex 32)"
}

if [[ "${1:-}" == "--add-trader" ]]; then
  if [[ ! -f .env ]]; then
    echo "No .env yet; run this script without arguments first." >&2
    exit 1
  fi
  count=$(grep -E '^ORDERFLOW_API_CREDENTIALS=' .env | cut -d= -f2- | tr ',' '\n' | grep -c . || true)
  name="trader-$(printf "\\x$(printf '%x' $((97 + count)))")"
  entry=$(credential "$name")
  # Appends to the existing value, or fills it when it is empty.
  sed -i -E "s|^(ORDERFLOW_API_CREDENTIALS=)(.+)$|\1\2,${entry}|; s|^(ORDERFLOW_API_CREDENTIALS=)$|\1${entry}|" .env
  echo "Added API key id '${name}-key' for account '${name}' to .env. Restart the server to load it."
  exit 0
fi

if [[ -e .env ]]; then
  echo ".env already exists; delete it first, or use --add-trader to add an account." >&2
  exit 1
fi

credentials="$(credential trader-a),$(credential trader-b)"

# umask keeps the file private to the current user from the moment it exists.
(umask 077 && sed "s|^ORDERFLOW_API_CREDENTIALS=.*|ORDERFLOW_API_CREDENTIALS=${credentials}|" \
  .env.example > .env)

echo "Created .env with API key ids 'trader-a-key' and 'trader-b-key'."
echo "Secrets stay in .env; scripts/signed-request.sh and scripts/postman-env.sh read them from there."
