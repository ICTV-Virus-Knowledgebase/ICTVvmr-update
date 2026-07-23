#!/usr/bin/env bash

set -euo pipefail

readonly DBNAME="ictv_taxonomy"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly BACKUP_SQL="$SCRIPT_DIR/backup_species_isolates_table.sql"

if [[ $# -ne 0 ]]; then
    echo "Usage: $(basename "$0")" >&2
    exit 2
fi

if ! command -v mariadb >/dev/null 2>&1; then
    echo "Error: mariadb command was not found." >&2
    exit 1
fi

if [[ ! -f "$BACKUP_SQL" ]]; then
    echo "Error: backup SQL file does not exist: $BACKUP_SQL" >&2
    exit 1
fi

echo "Backing up species_isolates in database $DBNAME"
mariadb -D "$DBNAME" -vvv --show-warnings < "$BACKUP_SQL"
echo "Backup completed successfully."
