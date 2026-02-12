#!/usr/bin/env bash
# Create conda env.
#
# NOTE (Ubuntu): conda-forge must be listed before bioconda or solves can hang.
# If ordering isn't enough, add channels exactly in the order shown below 
# (from top to bottom) and set strict priority:

# conda config --add channels defaults
# conda config --add channels conda-forge
# conda config --add channels bioconda
# conda config --set channel_priority strict

conda create -v -y \
	-p ./conda/vmr_openpyxl3 \
	-c conda-forge -c bioconda \
	pandas pyarrow \
	xlrd openpyxl=3 numpy \
	biopython \
	bioframe \
	natsort \
	pymysql
