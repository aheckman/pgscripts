CREATE OR REPLACE PROCEDURE public.create_partitioned_index(_schema_name text, _table_name text, _index_suffix text, _index_columns text)
LANGUAGE plpgsql AS $proc$
DECLARE
    partition RECORD;
BEGIN
    -- Create an index on the base partitioned table with "ON ONLY"
    RAISE NOTICE 'CREATE INDEX IF NOT EXISTS % ON ONLY %.% (%);',  _table_name || '_' || _index_suffix, _schema_name, _table_name, _index_columns;
    RAISE NOTICE 'COMMIT;';
    
    -- Loop through all partitions of the base table
    FOR partition IN
        SELECT p.relname AS partition_name
        FROM pg_class c
        JOIN pg_inherits i ON i.inhparent = c.oid
        JOIN pg_class p ON p.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = _table_name
        AND n.nspname = _schema_name
    LOOP
        
        -- Create the index on the partition
        RAISE NOTICE 'CREATE INDEX CONCURRENTLY IF NOT EXISTS % ON %.% (%);', partition.partition_name || '_' || _index_suffix, _schema_name, partition.partition_name, _index_columns;
        RAISE NOTICE 'ALTER INDEX % ATTACH PARTITION %;', _table_name || '_' || _index_suffix, partition.partition_name || '_' || _index_suffix;
        RAISE NOTICE 'COMMIT;';
        
    END LOOP;
END;
$proc$;
 
CALL create_partitioned_index('public', 'sales', 'amount_idx', 'amount');
