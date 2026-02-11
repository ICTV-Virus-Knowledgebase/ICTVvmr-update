# Agent instructions

This repository contains 3 independent programs:

## Program "vmr_update.py"
- Location: ./
- Entry point: ./vmr_update.py
- Test data: test_data/update/
- Test results: test_out/update/
- Validation command: ```make regression-update```

## Program "vmr_export.py"
- Location: ./
- Entry point: ./vmr_export.py
- Test Data: test_data/export/
- Tests: test_out/export/
- Validation command: ```make regression-export```
   - If successful, "test_out/export/results.txt" will contain "EDITOR:SUCCESS;PUB:SUCCESS"

## Program "scripts/xlsx_diff" (python)
- Location: ./
- Entry point: ./scripts/xlsx_diff
- Test Data: test_data/xlsx_diff/
- Tests: test_out/xlsx_diff/
- Validation command: ```make regression-xlsx_diff```
   - If successful, "test_out/xlsx_diff/results.txt" will contain "SUCCESS", which is the same as "test_out/xlsx_diff/expected-vs-out.diff.txt" being empty.

## Important
The agent must work on **only one program at a time**.
Do not modify files outside the selected program entry point.

## Environment
- Python conda environment and test-data submodules are initialized in the Codex Cloud setup script.
- Do not reinitialize submodules in agent code.