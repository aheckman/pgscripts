create table customers (cust_id bigint, cust_name text);
insert into customers (cust_id, cust_name) values (1,"Customer 1");
insert into customers (cust_id, cust_name) values (2,"Customer 2");
insert into customers (cust_id, cust_name) values (77,"Customer 77");
insert into customers (cust_id, cust_name) values (123,"Customer 123");
commit;

alter table customers add PRIMARY KEY (cust_id);

select * from customers;

create table sales (
sale_id bigint,
cust_id bigint,
amount BIGINT) partition by list (cust_id);

create unique index on sales (sale_id, cust_id);

create index on sales(sale_id);

insert into sales (sale_id, cust_id, amount) values (1, 1, 1);

create table sales_default partition of sales default;
create table sales_1 partition of sales for values in (1);

create table sales_10 partition of sales for values in (10);

select * from sales;


select * from pg_partitioned_table;

select c.relname, pt.*, an.attname, an.atttypid, ptyp.*
from pg_class c
join pg_partitioned_table pt on (pt.partrelid = c.oid)
join pg_attribute an on (pt.partattrs[0] = an.attnum and pt.partrelid = an.attrelid)
join pg_type ptyp on (an.atttypid = ptyp.oid);


select * from pg_opclass;

SELECT unnest(ARRAY[1, 2, 3]) 
from (select unnest(array[4, 5, 6, 7])) foo;


WITH RECURSIVE inheritance_tree AS (
     SELECT   c.oid AS table_oid
            , c.relname  AS table_name
            , NULL::name AS table_parent_name
            , an.attname AS table_partition_column
            , c.relispartition AS is_partition
            , 1 as level
     FROM pg_class c
     JOIN pg_partitioned_table pt on (pt.partrelid = c.oid)
     JOIN pg_attribute an on (pt.partattrs[0] = an.attnum and pt.partrelid = an.attrelid)
     WHERE c.relkind = 'p'
     AND   pt.partnatts = 1
     AND   c.relispartition = false

     UNION ALL

     SELECT inh.inhrelid AS table_oid
          , c.relname AS table_name
          , cc.relname AS table_parent_name
          , it.table_partition_column AS table_partition_column
          , c.relispartition AS is_partition
          , it.level + 1 as level
     FROM inheritance_tree it
     JOIN pg_inherits inh ON inh.inhparent = it.table_oid
     JOIN pg_class c ON inh.inhrelid = c.oid
     JOIN pg_class cc ON it.table_oid = cc.oid
     LEFT JOIN pg_partitioned_table pt on (pt.partrelid = c.oid)
     LEFT JOIN pg_attribute an on (pt.partattrs[0] = an.attnum and pt.partrelid = an.attrelid)

)
SELECT
		  it.level
        , it.table_name
        , CASE p.partstrat
               WHEN 'l' THEN 'BY LIST'
               WHEN 'r' THEN 'BY RANGE'
               ELSE 'not partitioned'
          END AS partitionin_type
        , it.table_parent_name
        , it.table_partition_column
        , pg_get_partkeydef(c.oid)
        , pg_get_partkeydef(it.table_oid)
        , pg_get_expr( c.relpartbound, c.oid, true ) AS partitioning_values
        , regexp_extract_integer(pg_get_expr( c.relpartbound, c.oid, true )) as part_val
        , c.relpartbound::text
        , ptan.attname
        , pg_get_expr( p.partexprs, c.oid, true )    AS sub_partitioning_values
FROM inheritance_tree it
JOIN pg_class c ON c.oid = it.table_oid
LEFT JOIN pg_partitioned_table p ON p.partrelid = it.table_oid
LEFT JOIN pg_attribute ptan on (p.partattrs[0] = ptan.attnum and p.partrelid = ptan.attrelid)
ORDER BY 1,2;

CREATE OR REPLACE FUNCTION regexp_extract_integer(input_str TEXT)
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    -- Extract the first group of digits enclosed in single quotes
    SELECT (regexp_matches(input_str, '.*\(''(\d+)''\).*'))[1] INTO result;
    
    -- Convert the result to an integer
    RETURN result::INTEGER;
EXCEPTION WHEN others THEN
    -- In case of any error, return NULL or handle as needed
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

select * from pg_indexes where SCHEMANAME = 'public';

SELECT n.nspname AS "Schema",if i add a 
       t.relname AS "Table",
       c.relname AS "Index",
       pg_get_indexdef(c.oid) AS "Definition",
       i.indisvalid AS "IsValid"
FROM pg_catalog.pg_class c
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     JOIN pg_catalog.pg_class t ON i.indrelid = t.oid
WHERE c.relkind = 'i'
      AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
      AND pg_catalog.pg_table_is_visible(c.oid);
      
CREATE INDEX IF NOT EXISTS sales_amount_idx ON ONLY public.sales USING btree (amount);

DROP INDEX sales_amount_idx;


SELECT p.relname AS partition_name
FROM pg_class c
JOIN pg_inherits i ON i.inhparent = c.oid
JOIN pg_class p ON p.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'sales'
AND n.nspname = 'public'
ORDER BY p.relname;

drop function create_partitioned_index;

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

        SELECT p.relname AS partition_name
        FROM pg_class c
        JOIN pg_inherits i ON i.inhparent = c.oid
        JOIN pg_class p ON p.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'sales'
        AND n.nspname = 'public';
        


        SELECT format('DROP FUNCTION IF EXISTS %I.%I(%s);', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid))
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.proname = 'create_partitioned_index'; -- Replace 'your_function_name' with the target function name
      --    AND n.nspname = 'public' -- Adjust the schema name as needed;
      
DROP FUNCTION IF EXISTS public.create_partitioned_index(schema_name text, table_name text, index_suffix text, index_columns text);
DROP FUNCTION IF EXISTS public.create_partitioned_index(schema_name text, table_name text, index_suffix text, index_columns text, uniqueness text);


 CREATE INDEX IF NOT EXISTS sales_amount_idx ON ONLY public.sales (amount);
 COMMIT;
 CREATE INDEX CONCURRENTLY IF NOT EXISTS sales_default_amount_idx ON public.sales_default (amount);
 ALTER INDEX sales_amount_idx ATTACH PARTITION sales_default_amount_idx;
 COMMIT;
  CREATE INDEX CONCURRENTLY IF NOT EXISTS sales_1_amount_idx ON public.sales_1 (amount);
 ALTER INDEX sales_amount_idx ATTACH PARTITION sales_1_amount_idx;
  COMMIT;
  
  
  
  create table sales_10 partition of sales for values in (10);

create table sales_999 
(
sale_id bigint,
cust_id bigint,
amount BIGINT);

insert into sales_999(sale_id, cust_id, amount) values (900,999,442);
insert into sales_999(sale_id, cust_id, amount) values (901,999,435);
insert into sales_999(sale_id, cust_id, amount) values (902,999,234);
commit;

alter table sales attach partition sales_9991 for values in (9991);


drop  table sales_9991;
create table sales_9991 
(
sale_id bigint,
cust_id bigint,
amount BIGINT);

create index alana on sales_9991(amount, cust_id, sale_id);

insert into sales_9991(sale_id, cust_id, amount) values (900,9991,442);
insert into sales_9991(sale_id, cust_id, amount) values (901,9991,435);
insert into sales_9991(sale_id, cust_id, amount) values (902,999,234);
commit;

select * from sales_9991;
