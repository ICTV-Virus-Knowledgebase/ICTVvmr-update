#!/usr/bin/env bash
#
# Export VMR.xlsx for VMR_VMR41v1_20260320 (date MSL41.v1 data hit prod website)
#
# exports from ICTVdatabase/data/ directory
#
DATA_SOURCE="./MSL41v1/data"
DATA_MASK="../db_mask.tsv"
TEMPLATE="template-VMR_MSL41.v1.20260625.editor.xlsx"
OUTPUT="VMR_MSL41.v1.20260625.editor.xlsx"

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
