#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing .env file: $ENV_FILE" >&2
    exit 1
fi

echo "Cleaning database tables..."
docker compose \
    --project-directory "$ROOT_DIR" \
    --env-file "$ENV_FILE" \
    -f "$ROOT_DIR/docker-compose.yml" \
    exec -T mysql sh -ec 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -u root "$MYSQL_DATABASE"' <<'SQL'
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fact_predictions;
TRUNCATE TABLE raw_happiness_events;
TRUNCATE TABLE dim_country;
TRUNCATE TABLE dim_date;
SET FOREIGN_KEY_CHECKS = 1;
SQL

echo "Database tables cleaned successfully."