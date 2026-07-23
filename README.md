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

1. Backup species_isolates table using [utils/backup_species_isolates_table.sql](utils/backup_species_isolates_table.sql)
    - [utils/run_backup_species_isolates_table.sh](utils/run_backup_species_isolates_table.sh) executes `backup_species_isolates_table.sql`
    - This is useful if you are developing on the server where the test database lives

2. Build and activate conda evironment
    - ./create_conda_env_openpyxl3.sh
    - conda activate ./conda/vmr_openpyxl3

3. Get exports of VMR associated tables
    - Follow steps 1-2 in `VMR export protocol` below to clone ICTVdatabase with the needed data files
    - Or sftp the export files over

4. Run vmr_update_sql.py
    - python vmr_update_sql.py \
        --vmr-export ~/ICTVdatabase/data/vmr_export.utf8.txt \
        --taxonomy-genome-coverage ~/ICTVdatabase/data/taxonomy_genome_coverage.utf8.txt \
        --taxonomy-molecule ~/ICTVdatabase/data/taxonomy_molecule.utf8.txt \
        --taxonomy-host-source ~/ICTVdatabase/data/taxonomy_host_source.utf8.txt \
        --keep-going
    - Check ./errors.xlsx and follow up with authors

5. Run generated .sql files on test database
    - The generated files are `vmr_0_cv_inserts.sql`, `vmr_1_deletes.sql`, `vmr_2_update.sql`, `vmr_3_inserts.sql`, `vmr_4_update_sorts.sql`, and `vmr_5_exec_qc_sps.sql`
    - `update_vmr_tables.sh` can take an input directory to run the generated sql to directly update the test database

6. QC resulting database
    - Check the output of `sp.QC_run_modules(NULL);` (this is the SQL inside `vmr_5_exec_qc_sps.sql`)
    - IN the web browser look over the updates to make sure they are there


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

        4. git add the template file

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

5.  run the export, pointing at the previous release's `*.editor.xlsx`
    so that `README.editor` and `CHANGELOG.editor` are copied forward
    automatically:

    ``` bash
    conda activate ./conda//vmr_openpyxl
    cd ${REL_DIR}
    ./run_export.sh ../VMR_MSL##v#.YYYYMMDD/VMR_MSL##v#.YYYYMMDD.editor.xlsx
    ```

    If `README.editor` needs actual content changes for this
    release, edit that worksheet in the newly generated output
    afterward

6.  Double check the two (editor and public) sheets it will produce.

### Possible Improvements

1.  Enhance `vmr_export.py` so that we can give run_export.sh an
    argument pointing to the current editor .xlsx, and it will copy the
    one or two worksheets over: `README.editor` , `CHANGELOG.editor` [implemented]

2.  Change `vmr_update_sql.py` and `vmr_export.py` to store
    `CHANGELOG.editor` rows in the in the db, and have a `vmr_toc` (or
    `species_isolates_toc`), so that round-trip through the database is
    lossless.
