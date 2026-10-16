DROP TABLE /* TEMPLATE: schema */river_notification;

--
-- Recreate `river_client` and `river_client_queue` as they were in 005.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    created_at datetime2(6) NOT NULL CONSTRAINT river_client_created_at_default DEFAULT sysutcdatetime(),
    metadata nvarchar(max) NOT NULL CONSTRAINT river_client_metadata_default DEFAULT N'{}',
    paused_at datetime2(6) NULL,
    updated_at datetime2(6) NOT NULL,
    CONSTRAINT river_client_pkey PRIMARY KEY (id),
    CONSTRAINT river_client_name_length CHECK (DATALENGTH(id) > 0 AND DATALENGTH(id) < 256)
);

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    created_at datetime2(6) NOT NULL CONSTRAINT river_client_queue_created_at_default DEFAULT sysutcdatetime(),
    max_workers bigint NOT NULL CONSTRAINT river_client_queue_max_workers_default DEFAULT 0,
    metadata nvarchar(max) NOT NULL CONSTRAINT river_client_queue_metadata_default DEFAULT N'{}',
    num_jobs_completed bigint NOT NULL CONSTRAINT river_client_queue_num_jobs_completed_default DEFAULT 0,
    num_jobs_running bigint NOT NULL CONSTRAINT river_client_queue_num_jobs_running_default DEFAULT 0,
    updated_at datetime2(6) NOT NULL,
    CONSTRAINT river_client_queue_pkey PRIMARY KEY (river_client_id, name),
    CONSTRAINT river_client_queue_river_client_id_fkey FOREIGN KEY (river_client_id)
        REFERENCES /* TEMPLATE: schema */river_client (id) ON DELETE CASCADE,
    CONSTRAINT river_client_queue_name_length CHECK (DATALENGTH(name) > 0 AND DATALENGTH(name) < 256),
    CONSTRAINT river_client_queue_num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT river_client_queue_num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);
