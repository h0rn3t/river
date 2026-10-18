
--
-- Drop `river_job.unique_states` and its index.
--

-- Since 006, rows may share a `unique_key` as long as at most one of them is in
-- a state covered by its `unique_states`, which would prevent the unique index
-- on `(kind, unique_key)` below from being recreated. Clear the key on all but
-- one row of each duplicate group, preferring to keep it on a row where
-- uniqueness is still enforced, then on the newest row. This is a no-op when
-- there are no duplicates.
WITH duplicate_job AS (
    SELECT
        id,
        row_number() OVER (
            PARTITION BY kind, unique_key
            ORDER BY
                (unique_states IS NOT NULL AND /* TEMPLATE: schema */river_job_state_in_bitmask(unique_states, state)) DESC,
                id DESC
        ) AS duplicate_rank
    FROM /* TEMPLATE: schema */river_job
    WHERE unique_key IS NOT NULL
)
UPDATE /* TEMPLATE: schema */river_job
SET unique_key = NULL
FROM duplicate_job
WHERE river_job.id = duplicate_job.id
    AND duplicate_job.duplicate_rank > 1;

DROP INDEX /* TEMPLATE: schema */river_job_unique_idx;

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN unique_states;

CREATE UNIQUE INDEX IF NOT EXISTS river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job (kind, unique_key) WHERE unique_key IS NOT NULL;

--
-- Drop `river_job_state_in_bitmask` function.
--
DROP FUNCTION /* TEMPLATE: schema */river_job_state_in_bitmask;
