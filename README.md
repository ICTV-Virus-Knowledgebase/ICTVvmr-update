---
editor_options: 
  markdown: 
    wrap: 72
---

# ICTV VMR update

## Conda environment notes (Ubuntu)

On Ubuntu, environment solves can fail unless `conda-forge` is listed
before `bioconda`. If ordering alone is not sufficient, add channels
exactly in the order shown below (from top to bottom) and set channel
priority to strict:

``` bash
conda config --add channels defaults
conda config --add channels conda-forge
conda config --add channels bioconda
conda config --set channel_priority strict
```

## VMR update protocol

Logan - can you write this?

## VMR export protocol \*\*DRAFT\*\*

This exports from the .tsv files from the
[ICTVdatabase](https://github.com/ICTV-Virus-Knowledgebase/ICTVdatabase)
repository into Excel files.

to fix before using

-   standarize filenames/directory names to correct format

-   test code snippets

Because not all data in the VMR.xlsx is uploaded to the database there
are several things that must be carried forward by hand, notably: \*
worksheet: README.editor \* worksheet: CHANGELOG.editor Also, the
VMR.editor.xlsx can get it's conditional formatting and other formatting
messed up during the editing process, so we don't want to use it as the
basis for the next release. Instead, we maintain a template
VMR.editor.xslx that is used.

Steps:

1.  Create a new release directory for your release

```         
REL_DIR=VMR_MSL##v#.YYYYMMDD
mkdir $REL_DIR
```

2.  From the previous release directory:

    1.  copy `template-VMR_MSL##v#.YYYYMMDD.editor.xlsx` (the date comes from the day you receive the VMR xlsx)

        1.  updating the name to the current release

        2.  update `Version` worksheet

        3.  (if new MSL release) update name of `VMR MSL##`

        4.  update `README.editor` worksheet, if changes have been made
            to the current one

        5.  replace `CHANGELOG.editor` with the one from the
            VMR.editor.xlsx that was used for the update

        6.  git add the template file

3.  clone ICTVdatabase as a submodule in the new release dir named
    `VMR##v#`

    ```         
    REL_DIR=VMR_MSL##v#.YYYYMMDD
    git submodule add \
    git@github.com:ICTV-Virus-Knowledgebase/ICTVdatabase.git \
    ${REL_DIR}/MSL41v1
    git add .gitmodules ${REL_DIR}
    ```

4.  copy run_export.sh

    1.  update filenames, versions and paths

    2.  `git add run_export.sh`

5.  run the export

    ``` bash
    conda activate ./conda//vmr_openpyxl
    cd ${REL_DIR}
    ./run_export.sh
    ```

6.  Double check the two (editor and public) sheets it will produce.

### Possible Improvements

1.  Enhance `vmr_export.py` so that we can give run_export.sh an
    argument pointing to the current editor .xlsx, and it will copy the
    one or two worksheets over: `README.editor` , `CHANGELOG.editor`

2.  Change `vmr_update_sql.py` and `vmr_export.py` to store
    `CHANGELOG.editor` rows in the in the db, and have a `vmr_toc` (or
    `species_isolates_toc`), so that round-trip through the database is
    lossless.
