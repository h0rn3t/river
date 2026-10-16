--
-- Normally, this makes sure that job IDs are never reused, but `river_job`
-- has used a sequence for its IDs since it was created in 002, and sequences
-- never reuse values.
--

SELECT 1;
