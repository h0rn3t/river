CREATE TABLE /* TEMPLATE: schema */river_migration (
    created_at datetime2(6) NOT NULL CONSTRAINT river_migration_created_at_default DEFAULT sysutcdatetime(),
    version bigint NOT NULL,
    CONSTRAINT river_migration_version_check CHECK (version >= 1)
);

CREATE UNIQUE INDEX river_migration_version_idx ON /* TEMPLATE: schema */river_migration (version);
