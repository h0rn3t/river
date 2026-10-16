--
-- Notification outbox. SQL Server clients run in poll only mode and don't
-- read from it, but the table exists for parity with the other drivers.
--

CREATE TABLE /* TEMPLATE: schema */river_notification (
    id bigint IDENTITY(1, 1) NOT NULL,
    created_at datetime2(6) NOT NULL CONSTRAINT river_notification_created_at_default DEFAULT sysutcdatetime(),
    payload nvarchar(max) NOT NULL,
    topic nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CONSTRAINT river_notification_pkey PRIMARY KEY (id),
    CONSTRAINT river_notification_topic_length CHECK (DATALENGTH(topic) > 0 AND DATALENGTH(topic) < 256)
);

CREATE INDEX river_notification_created_at_idx ON /* TEMPLATE: schema */river_notification (created_at);
CREATE INDEX river_notification_topic_id_idx ON /* TEMPLATE: schema */river_notification (topic, id);

--
-- Drop unused tables `river_client` and `river_client_queue`.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;
