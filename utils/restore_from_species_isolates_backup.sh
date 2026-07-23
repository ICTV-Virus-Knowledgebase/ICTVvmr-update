#!/usr/bin/env bash

set -euo pipefail

readonly DBNAME="ictv_taxonomy"
readonly TARGET_TABLE="species_isolates"

usage() {
    echo "Usage: $(basename "$0") species_isolates_YYYYMMDD" >&2
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

backup_table=$1

if [[ ! $backup_table =~ ^species_isolates_[0-9]{8}$ ]]; then
    echo "Error: backup table must match species_isolates_YYYYMMDD." >&2
    exit 2
fi

if ! command -v mariadb >/dev/null 2>&1; then
    echo "Error: mariadb command was not found." >&2
    exit 1
fi

echo "Restoring $TARGET_TABLE from $backup_table in database $DBNAME"

mariadb -D "$DBNAME" -vvv --show-warnings <<SQL
SET @backup_row_count = (SELECT COUNT(*) FROM $backup_table);

START TRANSACTION;

DELETE FROM $TARGET_TABLE;

INSERT INTO $TARGET_TABLE (
    isolate_id,
    taxnode_id,
    species_sort,
    isolate_sort,
    species_name,
    isolate_type,
    isolate_names,
    isolate_abbrevs,
    isolate_designation,
    genbank_accessions,
    refseq_accessions,
    genome_coverage,
    molecule,
    host_source,
    refseq_organism,
    refseq_taxids,
    update_change,
    update_prev_species,
    update_prev_taxnode_id,
    update_change_proposal,
    notes
)
SELECT
    isolate_id,
    taxnode_id,
    species_sort,
    isolate_sort,
    species_name,
    isolate_type,
    isolate_names,
    isolate_abbrevs,
    isolate_designation,
    genbank_accessions,
    refseq_accessions,
    genome_coverage,
    molecule,
    host_source,
    refseq_organism,
    refseq_taxids,
    update_change,
    update_prev_species,
    update_prev_taxnode_id,
    update_change_proposal,
    notes
FROM $backup_table;

SET @restored_row_count = ROW_COUNT();

SELECT
    '$backup_table' AS backup_table,
    @backup_row_count AS backup_row_count,
    '$TARGET_TABLE' AS restored_table,
    @restored_row_count AS restored_row_count;

COMMIT;

ALTER TABLE $TARGET_TABLE AUTO_INCREMENT = 1;
SQL

echo "Restore completed successfully."
