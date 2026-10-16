--
-- Normally, `river_job` gains the `pending` state and `river_leader.name` a
-- default here, but since SQL Server was added later, both are already part
-- of their tables as created in 002.
--

CREATE TABLE /* TEMPLATE: schema */river_queue (
    name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    created_at datetime2(6) NOT NULL CONSTRAINT river_queue_created_at_default DEFAULT sysutcdatetime(),
    metadata nvarchar(max) NOT NULL CONSTRAINT river_queue_metadata_default DEFAULT N'{}',
    paused_at datetime2(6) NULL,
    updated_at datetime2(6) NOT NULL CONSTRAINT river_queue_updated_at_default DEFAULT sysutcdatetime(),
    CONSTRAINT river_queue_pkey PRIMARY KEY (name),
    CONSTRAINT river_queue_metadata_is_json CHECK (ISJSON(metadata) = 1)
);
