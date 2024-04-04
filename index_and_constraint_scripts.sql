SELECT
    con.conname AS constraint_name,
    con.contype AS constraint_type,
    array_agg(att.attname) AS column_names
FROM
    pg_constraint con
JOIN
    pg_class cls ON cls.oid = con.conrelid
JOIN
    pg_namespace ns ON ns.oid = cls.relnamespace
JOIN
    pg_attribute att ON att.attrelid = cls.oid AND att.attnum = ANY(con.conkey)
WHERE
    cls.relname = 'your_table_name'
    AND ns.nspname = 'your_schema_name'
    AND con.contype IN ('u', 'p')
GROUP BY
    con.conname, con.contype
ORDER BY
    con.conname;

SELECT 
    idx.relname AS index_name
FROM 
    pg_class tbl,
    pg_class idx,
    pg_index ix,
    pg_namespace ns
WHERE 
    tbl.oid = ix.indrelid 
    AND idx.oid = ix.indexrelid 
    AND ns.oid = tbl.relnamespace 
    AND tbl.relname = 'your_table_name'
    AND ns.nspname = 'your_schema_name'
ORDER BY 
    idx.relname;
