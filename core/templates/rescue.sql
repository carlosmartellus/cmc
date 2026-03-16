-- Migration: {{name}}
-- [CMC-RESCUE-TEMPLATE]
-- Template for restore parcial data from a backup
-- BEFORE ANY ACTION: Extract data from backup to a local CSV.
-- EXAMPLE: COPY (SELECT id, column FROM table) TO '/tmp/rescue_data.csv' CSV HEADER;

-- [CMC-UP]
BEGIN;

-- 1. (Optional) Recreate structure if was deleted
-- ALTER TABLE my_table ADD COLUMN my_column TEXT;

-- 2. Create a temporary table in memory
CREATE TEMP TABLE temp_rescue (
    id INT,
    my_column TEXT
);

-- 3. Load data from tmp CSV
-- COPY temp_rescue FROM '/tmp/rescue_data.csv' CSV HEADER;

-- 4. Marge with production data
-- UPDATE my_table t
-- SET my_column = tmp.my_column
-- FROM temp_rescue tmp
-- WHERE t.id = tmp.id;

-- 5. Clear temp table (Optional)
DROP TABLE temp_rescue;

COMMIT;