-- Resolves the table the same way as TableExists (`to_regclass`, which falls
-- back to `search_path` when the name is unqualified) so that the two agree on
-- which table is being inspected.
--
-- name: ColumnExists :one
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute
    WHERE attrelid = to_regclass(@schema_and_table::text)
        AND attname = @column_name::text
        AND attnum > 0
        AND NOT attisdropped
);

-- name: IndexExists :one
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class
        JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE pg_class.relname = @index::text
        AND pg_namespace.nspname = coalesce(sqlc.narg('schema')::text, current_schema())
        AND pg_class.relkind = 'i'
);

-- name: IndexReindexArtifacts :many
WITH index_artifacts AS (
    SELECT
        c.relname::text AS index_name,
        substring(c.relname FROM length(@index::text) + 1) AS suffix
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = coalesce(sqlc.narg('schema')::text, current_schema())
        AND c.relkind = 'i'
        AND left(c.relname, length(@index::text)) = @index::text
)
SELECT index_name
FROM index_artifacts
WHERE suffix ~ '^_cc(new|old)[0-9]*$'
ORDER BY index_name;

-- name: IndexesExist :many
WITH index_names AS (
    SELECT unnest(@index_names::text[]) as index_name
)
SELECT index_names.index_name::text AS index_name,
       EXISTS (
         SELECT 1
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = coalesce(sqlc.narg('schema')::text, current_schema())
         AND c.relname = index_names.index_name
         AND c.relkind = 'i'
       ) AS exists
FROM index_names;

-- name: SchemaGetExpired :many
SELECT schema_name::text
FROM information_schema.schemata
WHERE schema_name LIKE @prefix
    AND schema_name < @before_name
ORDER BY schema_name;

-- name: TableExists :one
SELECT CASE WHEN to_regclass(@schema_and_table) IS NULL THEN false
            ELSE true END;
