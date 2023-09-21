#!/usr/bin/env bash
#
# Create conda env 
#
conda create \
	-p ./conda/vmr_openpyxl3 \
	-c bioconda -c conda-forge \
	pandas openpyxl=3 numpy biopython blast xlrd

