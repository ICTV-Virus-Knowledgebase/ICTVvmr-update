#!/usr/bin/env bash

set -euo pipefail

readonly DBNAME="ictv_taxonomy"

usage() {
    echo "Usage: $(basename "$0") SQL_DIRECTORY" >&2
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

sql_dir=$1

if [[ ! -d "$sql_dir" ]]; then
    echo "Error: SQL directory does not exist: $sql_dir" >&2
    exit 1
fi

if ! command -v mariadb >/dev/null 2>&1; then
    echo "Error: mariadb command was not found." >&2
    exit 1
fi

sql_files=()
while IFS= read -r -d "" sql_file; do
    filename=${sql_file##*/}
    if [[ $filename =~ ^vmr_[0-9]+.*\.sql$ ]]; then
        sql_files+=("$sql_file")
    fi
done < <(find "$sql_dir" -maxdepth 1 -type f -name "vmr_*.sql" -print0 | sort -zV)

if (( ${#sql_files[@]} == 0 )); then
    echo "Error: no numbered vmr_*.sql files found in: $sql_dir" >&2
    exit 1
fi

echo "Updating database $DBNAME with SQL files from: $sql_dir"

for sql_file in "${sql_files[@]}"; do
    echo "Executing: ${sql_file##*/}"
    mariadb -D "$DBNAME" -vvv --show-warnings < "$sql_file"
done

echo "Database update completed successfully."
