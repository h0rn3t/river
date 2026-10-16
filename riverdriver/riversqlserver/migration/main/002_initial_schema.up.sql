--
-- SQL Server was added well after the Postgres versions of these migrations,
-- so `river_job` and `river_leader` are created here in their final form
-- rather than being built up over versions 002 through 007.
--

-- A sequence instead of an identity column so that job IDs can be allocated
-- for batch inserts ahead of time and so that rows can be inserted with
-- explicit IDs.
CREATE SEQUENCE /* TEMPLATE: schema */river_job_id_seq AS bigint START WITH 1 INCREMENT BY 1;

CREATE TABLE /* TEMPLATE: schema */river_job (
    id bigint NOT NULL CONSTRAINT river_job_id_default DEFAULT (NEXT VALUE FOR /* TEMPLATE: schema */river_job_id_seq),
    args nvarchar(max) NOT NULL CONSTRAINT river_job_args_default DEFAULT N'{}',
    attempt smallint NOT NULL CONSTRAINT river_job_attempt_default DEFAULT 0,
    attempted_at datetime2(6) NULL,
    attempted_by nvarchar(max) NULL, -- JSON array of strings
    created_at datetime2(6) NOT NULL CONSTRAINT river_job_created_at_default DEFAULT sysutcdatetime(),
    errors nvarchar(max) NULL, -- JSON array of error objects
    finalized_at datetime2(6) NULL,
    kind nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    max_attempts smallint NOT NULL CONSTRAINT river_job_max_attempts_default DEFAULT 25,
    metadata nvarchar(max) NOT NULL CONSTRAINT river_job_metadata_default DEFAULT N'{}',
    priority smallint NOT NULL CONSTRAINT river_job_priority_default DEFAULT 1,
    queue nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT river_job_queue_default DEFAULT N'default',
    state nvarchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT river_job_state_default DEFAULT N'available',
    scheduled_at datetime2(6) NOT NULL CONSTRAINT river_job_scheduled_at_default DEFAULT sysutcdatetime(),
    tags nvarchar(max) NOT NULL CONSTRAINT river_job_tags_default DEFAULT N'[]', -- JSON array of strings
    unique_key varbinary(255) NULL,
    unique_states tinyint NULL,

    -- SQL Server's filtered indexes can't express Postgres'
    -- `river_job_state_in_bitmask` partial index predicate, so uniqueness is
    -- enforced through a unique index on this column instead. It holds the
    -- unique key (prefixed with 0x01) while the job is in one of its unique
    -- states, and otherwise a value derived from the job's ID (prefixed with
    -- 0x00) that can't collide with any other row.
    unique_key_enforced AS (
        CASE
            WHEN unique_key IS NOT NULL
                AND unique_states IS NOT NULL
                AND (unique_states & CASE state
                    WHEN N'available' THEN 1
                    WHEN N'cancelled' THEN 2
                    WHEN N'completed' THEN 4
                    WHEN N'discarded' THEN 8
                    WHEN N'pending'   THEN 16
                    WHEN N'retryable' THEN 32
                    WHEN N'running'   THEN 64
                    WHEN N'scheduled' THEN 128
                    ELSE 0
                END) <> 0
            THEN 0x01 + unique_key
            ELSE 0x00 + CAST(id AS binary(8))
        END
    ) PERSISTED,

    CONSTRAINT river_job_pkey PRIMARY KEY (id),
    CONSTRAINT river_job_args_is_json CHECK (ISJSON(args) = 1),
    CONSTRAINT river_job_attempted_by_is_json CHECK (ISJSON(attempted_by) = 1),
    CONSTRAINT river_job_errors_is_json CHECK (ISJSON(errors) = 1),
    CONSTRAINT river_job_finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN (N'cancelled', N'completed', N'discarded')) OR
        (finalized_at IS NOT NULL AND state IN (N'cancelled', N'completed', N'discarded'))
    ),
    CONSTRAINT river_job_kind_length CHECK (DATALENGTH(kind) > 0 AND DATALENGTH(kind) < 256),
    CONSTRAINT river_job_metadata_is_json CHECK (ISJSON(metadata) = 1),
    CONSTRAINT river_job_priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT river_job_queue_length CHECK (DATALENGTH(queue) > 0 AND DATALENGTH(queue) < 256),
    CONSTRAINT river_job_state_valid CHECK (state IN (N'available', N'cancelled', N'completed', N'discarded', N'pending', N'retryable', N'running', N'scheduled')),
    CONSTRAINT river_job_tags_is_json CHECK (ISJSON(tags) = 1)
);

CREATE INDEX river_job_kind ON /* TEMPLATE: schema */river_job (kind);
CREATE INDEX river_job_prioritized_fetching_index ON /* TEMPLATE: schema */river_job (state, queue, priority, scheduled_at, id);
CREATE INDEX river_job_state_and_finalized_at_index ON /* TEMPLATE: schema */river_job (state, finalized_at) WHERE finalized_at IS NOT NULL;
CREATE UNIQUE INDEX river_job_unique_idx ON /* TEMPLATE: schema */river_job (unique_key_enforced);

CREATE TABLE /* TEMPLATE: schema */river_leader (
    elected_at datetime2(6) NOT NULL,
    expires_at datetime2(6) NOT NULL,
    leader_id nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT river_leader_name_default DEFAULT N'default',
    CONSTRAINT river_leader_pkey PRIMARY KEY (name),
    CONSTRAINT river_leader_leader_id_length CHECK (DATALENGTH(leader_id) > 0 AND DATALENGTH(leader_id) < 256),
    CONSTRAINT river_leader_name_default_only CHECK (name = N'default')
);
