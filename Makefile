#
# regresion test makefile for CODEX
#
# ----------------------------------------------------------------------
#
# XML_DIFF
#
# ----------------------------------------------------------------------

XLSX_DIFF_OLD=./test_data/xlsx_diff/old.xlsx
XLSX_DIFF_NEW=./test_data/xlsx_diff/new.xlsx
XLSX_DIFF_EXPECTED=./test_data/xlsx_diff/expected.txt

XLSX_DIFF_OUT=./test_out/xlsx_diff/out.txt
XLSX_DIFF_OUT_DIFF=./test_out/xlsx_diff/expected-vs-out.diff.txt
XLSX_DIFF_RESULTS=./test_out/xlsx_diff/results.txt

regression-xlsx_diff: $(XLSX_DIFF_RESULTS)

# easier to type
xlsx_diff: $(XLSX_DIFF_RESULTS)

# convert diff status to a string
$(XLSX_DIFF_RESULTS): $(XLSX_DIFF_OUT_DIFF)
	@echo "## ANALYSIS ##"
	@if [[ ! -s "$<" ]]; then echo "SUCCESS"; else echo "FAIL"; fi

# compare test output to expected output
$(XLSX_DIFF_OUT_DIFF): $(XLSX_DIFF_EXPECTED) $(XLSX_DIFF_OUT) 
	@echo "## DIFF out vs expected ##"
	diff $(word 1,$^) $(word 2,$^) | tee "$@"

# run regression test
$(XLSX_DIFF_OUT): $(XLSX_DIFF_OLD) $(XLSX_DIFF_NEW) 
	@echo "## XLSX_DIFF XLSX FILES ##"
	mkdir -p $$(dirname $@)
	./scripts/xlsx_diff --no-formatting $(word 1,$^) $(word 2,$^) | tee $@


# ----------------------------------------------------------------------
#
# VMR_EXPORT
#
# ----------------------------------------------------------------------

EXPORT_TEMPLATE=./test_data/export/VMR_MSL40.v2.20251013.editor.hacked.xlsx
EXPORT_FLATFILE_SRC=./test_data/export/ICTVdatabase/data
EXPORT_EXPECTED_EDITOR=./test_data/export/expected-VMR.editor.xlsx
EXPORT_EXPECTED_PUB=./test_data/export/expected-VMR.xlsx

EXPORT_OUT_EDITOR=./test_out/export/VMR.editor.xlsx
EXPORT_OUT_PUB=./test_out/export/VMR.xlsx
EXPORT_RESULTS_EDITOR=./test_out/export/results_editor.txt
EXPORT_RESULTS_PUB=./test_out/export/results_pub.txt
EXPORT_RESULTS=./test_out/export/results.txt

regression_export: $(EXPORT_RESULTS)

$(EXPORT_RESULTS_EDITOR): $(EXPORT_OUT_EDITOR) 
	@echo "## XLSX_DIFF: EDITOR  results ##"
	./scripts/xlsx_diff --no-formatting $(EXPORT_OUT_EDITOR) $(EXPORT_EXPECTED_EDITOR) | tee $(EXPORT_RESULTS_EDITOR)

$(EXPORT_RESULTS_PUB): $(EXPORT_OUT_PUB) 
	@echo "## XLSX_DIFF: PUB  results ##"
	./scripts/xlsx_diff --no-formatting $(EXPORT_OUT_PUB) $(EXPORT_EXPECTED_PUB) | tee $(EXPORT_RESULTS_PUB)

$(EXPORT_RESULTS): $(EXPORT_RESULTS_EDITOR) $(EXPORT_RESULTS_PUB) 
	@echo "## ANALYSIS: $(word 1,$^) ##"
	@if [[ "$$(cat $(word 1,$^))" == "No differences found." ]]; then printf "EDITOR:SUCCESS" | tee "$@"; else printf "EDITOR:FAIL"|tee -a "$@"; fi
	@echo "## ANALYSIS: $(word 2,$^) ##"
	@if [[ "$$(cat $(word 2,$^))" == "No differences found." ]]; then echo  ";PUB:SUCCESS" | tee -a "$@"; else echo ";PUB:FAIL"|tee -a "$@"; fi

export: $(EXPORT_OUT)

$(EXPORT_OUT_EDITOR): $(EXPORT_TEMPLATE)
	./vmr_export.py --data_source $(EXPORT_FLATFILE_SRC) --template "$<" --output "$@"

# ----------------------------------------------------------------------
#
# VMR_UPDATE
#
# ----------------------------------------------------------------------

regression_update:
	@echo "ERROR: not yet implemented"

update:
	@echo "ERROR: not yet implemented"


