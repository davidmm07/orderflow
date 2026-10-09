#!/usr/bin/env bash
# Creates a local .env from .env.example with two traders, each with a
# random API secret. Two accounts are needed to see trades, because an
# account never trades with itself.
#
#   scripts/bootstrap-env.sh                  create .env (refuses to overwrite)
#   scripts/bootstrap-env.sh --add-trader     append one more trader to .env
#   scripts/bootstrap-env.sh --add-pricefeed  add the price feed database password
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

if [[ "${1:-}" == "--add-pricefeed" ]]; then
  if [[ ! -f .env ]]; then
    echo "No .env yet; run this script without arguments first." >&2
    exit 1
  fi
  if grep -qE '^PRICEFEED_DB_PASSWORD=.+' .env; then
    echo ".env already has PRICEFEED_DB_PASSWORD; nothing to do."
    exit 0
  fi
  password=$(openssl rand -hex 24)
  if grep -qE '^PRICEFEED_DB_PASSWORD=' .env; then
    sed -i -E "s|^PRICEFEED_DB_PASSWORD=.*$|PRICEFEED_DB_PASSWORD=${password}|" .env
  else
    printf '\n# Password of the compose Postgres used by the price feed.\nPRICEFEED_DB_PASSWORD=%s\n' "$password" >> .env
  fi
  echo "Added PRICEFEED_DB_PASSWORD to .env. Run make up to start the price feed stack."
  exit 0
fi

if [[ -e .env ]]; then
  echo ".env already exists; delete it first, or use --add-trader / --add-pricefeed." >&2
  exit 1
fi

credentials="$(credential trader-a),$(credential trader-b)"

# umask keeps the file private to the current user from the moment it exists.
(umask 077 && sed -e "s|^ORDERFLOW_API_CREDENTIALS=.*|ORDERFLOW_API_CREDENTIALS=${credentials}|" \
  -e "s|^PRICEFEED_DB_PASSWORD=.*|PRICEFEED_DB_PASSWORD=$(openssl rand -hex 24)|" \
  .env.example > .env)

echo "Created .env with API key ids 'trader-a-key' and 'trader-b-key'."
echo "Secrets stay in .env; scripts/signed-request.sh and scripts/postman-env.sh read them from there."
