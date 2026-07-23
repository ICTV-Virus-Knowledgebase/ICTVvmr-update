#!/usr/bin/env bash
#
# Export VMR.xlsx for VMR_VMR41v1_20260320 (date MSL41.v1 data hit prod website)
#
# exports from ICTVdatabase/data/ directory
#
# Usage: ./run_export.sh [PREVIOUS_EDITOR_XLSX]
#   PREVIOUS_EDITOR_XLSX - optional path to the prior release's *.editor.xlsx;
#                          if given, README.editor/CHANGELOG.editor are copied from it.
#
DATA_SOURCE="./MSL41v1/data"
DATA_MASK="../db_mask.tsv"
TEMPLATE="template-VMR_MSL41.v1.20260625.editor.xlsx"
OUTPUT="VMR_MSL41.v1.20260625.editor.xlsx"
PREVIOUS_EDITOR="$1"

COPY_ARGS=()
if [[ -n "$PREVIOUS_EDITOR" ]]; then
    COPY_ARGS=(--copy-from-editor "$PREVIOUS_EDITOR")
fi

cat <<EOF
#
# run export from '$DATA_SOURCE'
#
../vmr_export.py --keep-going --verbose --data_source "$DATA_SOURCE" --mask "$DATA_MASK" --template "$TEMPLATE" --output "$OUTPUT" "${COPY_ARGS[@]}"
EOF
../vmr_export.py --keep-going --verbose --data_source "$DATA_SOURCE" --mask "$DATA_MASK" --template "$TEMPLATE" --output "$OUTPUT" "${COPY_ARGS[@]}"

if [[ $? -ne 0 ]]; then
    echo "FAIL"
else
    echo "SUCCESS: $OUTPUT"
fi