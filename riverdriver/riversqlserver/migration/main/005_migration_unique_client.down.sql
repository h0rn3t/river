DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

--
-- Revert to a migration table based only on `version`. If any non-main
-- migrations are present, 005 is considered irreversible.
--

IF EXISTS (
    SELECT 1
    FROM /* TEMPLATE: schema */river_migration
    WHERE line <> N'main'
)
BEGIN
    THROW 50000, N'Found non-main migration lines in the database; version 005 migration is irreversible because it would result in loss of migration information.', 1;
END;

ALTER TABLE /* TEMPLATE: schema */river_migration
    DROP CONSTRAINT river_migration_pkey, river_migration_line_length;

ALTER TABLE /* TEMPLATE: schema */river_migration
    DROP COLUMN line;

CREATE UNIQUE INDEX river_migration_version_idx ON /* TEMPLATE: schema */river_migration (version);
