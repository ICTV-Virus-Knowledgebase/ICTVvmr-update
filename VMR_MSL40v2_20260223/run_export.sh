#!/usr/bin/env bash
#
# Export VMR.xlsx for VMR_VMR40v2_20260223 (date VMR data hit prod website)
#
# (note: based on VMR_VMR40v2_20260202 file from VMR editor, plus post-hoc edits discussed over emails)
#
# exports from ICTVdatabase/data/ directory
#
DATA_SOURCE="./ICTVdatabase_20260223/data"
DATA_MASK="../db_mask.tsv"
TEMPLATE="VMRs/template-VMR_MSL40.v2.20251013.editor_dbs_20260202_v2.xlsx"
OUTPUT="VMR_MSL40.v2.20260223.editor.xlsx"
EXPECTED="VMRs/template-VMR_MSL40.v2.20251013.editor_dbs_20260202_v2.xlsx"

cat <<EOF
#
# run export from '$DATA_SOURCE'
#
../vmr_export.py --keep-going --verbose --data_source "$DATA_SOURCE" --mask "$DATA_MASK" --template "$TEMPLATE" --output "$OUTPUT"
EOF
../vmr_export.py --keep-going --verbose --data_source "$DATA_SOURCE" --mask "$DATA_MASK" --template "$TEMPLATE" --output "$OUTPUT"

if [[ $? -ne 0 ]]; then
    echo "FAIL"
else
    echo "SUCCESS: $OUTPUT"
fi
