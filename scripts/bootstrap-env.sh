#!/usr/bin/env bash
# Creates a local .env from .env.example with a random API secret.
# Refuses to overwrite an existing .env so real values are never lost.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -e .env ]]; then
  echo ".env already exists; delete it first if you want a new one." >&2
  exit 1
fi

secret=$(openssl rand -hex 32)
credentials="local-key:local-account:${secret}"

# umask keeps the file private to the current user from the moment it exists.
(umask 077 && sed "s|^ORDERFLOW_API_CREDENTIALS=.*|ORDERFLOW_API_CREDENTIALS=${credentials}|" \
  .env.example > .env)

echo "Created .env with API key id 'local-key' for account 'local-account'."
echo "The secret stays in .env; scripts/signed-request.sh reads it from there."
