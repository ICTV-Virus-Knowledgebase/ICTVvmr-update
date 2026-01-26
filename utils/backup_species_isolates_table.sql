-- 
-- Backup current contents of species_isolates to a new table
-- To be used before doing a VMR update. 
--
--
-- Replace `species_isolates` with your actual table name if different
-- Step 1: Generate the backup table name
SET @date := DATE_FORMAT(CURDATE(), '%Y%m%d'); 
SET @src_tab := 'species_isolates';
SET @dest_tab := CONCAT(@src_tab,'_', @date);

-- Step 2: generate SQL to do the backup and QC row counts
SET @sql := CONCAT('CREATE TABLE ', @dest_tab, ' AS SELECT * FROM ',@src_tab);
SET @sql_ct := CONCAT('select  @src_tab as tab_name, count(*) as ct from ', @src_tab, 
' union all ', 
' select  @dest_tab as tab_name, count(*) as ct from ',@dest_tab,';');

SELECT @date AS todays_iso_date, @src_tab as src_tab_name, @dest_tab as backup_table_name, @sql AS backup_Species_Isolates_table_SQL, @sql_ct AS qc_backup_table_row_count_SQL;

-- Step 3: Prepare and execute the SQL dynamically
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Step 4: check results
PREPARE stmt_qc FROM @sql_ct;
EXECUTE stmt_qc;
DEALLOCATE PREPARE stmt_qc;
