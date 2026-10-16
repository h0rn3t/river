// Package riversqlserver provides a River driver implementation for Microsoft
// SQL Server 2022 or newer, and Azure SQL Database.
//
// This driver is currently in early testing. It's exercised by River's driver
// test suite, but has minimal real world use as of yet.
//
// It takes a database/sql pool opened with the "sqlserver" driver from
// github.com/microsoft/go-mssqldb:
//
//	dbPool, err := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=river")
//	if err != nil {
//		// handle error
//	}
//
//	riverClient, err := river.NewClient(riversqlserver.New(dbPool), &river.Config{...})
//
// Notable differences compared to the Postgres drivers:
//
//   - SQL Server has no equivalent of Postgres' LISTEN/NOTIFY, so clients run
//     in poll only mode. Jobs are picked up on each FetchPollInterval rather
//     than immediately after they're inserted.
//
//   - JobListParams.Metadata isn't supported, and SQL given to
//     JobListParams.Where must be T-SQL.
//
//   - River's tables use a filtered index and an index on a computed column,
//     so sessions must use the SET options SQL Server requires for them (like
//     QUOTED_IDENTIFIER and ANSI_NULLS on). These are go-mssqldb's defaults.
//
// Enabling READ_COMMITTED_SNAPSHOT on the database is recommended to reduce
// blocking between River's readers and writers.
package riversqlserver

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/savepointutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// Driver is an implementation of riverdriver.Driver for SQL Server.
type Driver struct {
	dbPool   *sql.DB
	replacer sqlctemplate.Replacer
}

// New returns a new SQL Server driver for use with River.
//
// It takes an sql.DB opened with the "sqlserver" driver name of
// github.com/microsoft/go-mssqldb. The pool must not be closed while associated
// River objects are running.
func New(dbPool *sql.DB) *Driver {
	return &Driver{
		dbPool: dbPool,
	}
}

// argPlaceholder makes positional arguments like `@p1`, which is the form
// go-mssqldb expects for arguments that aren't named.
const argPlaceholder = "@p"

func (d *Driver) ArgPlaceholder() string { return argPlaceholder }
func (d *Driver) DatabaseName() string   { return riverdriver.DatabaseNameSQLServer }

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, templateReplaceWrapper{d.dbPool, &d.replacer}, d, nil}
}

// GetListener panics because SQL Server has no LISTEN/NOTIFY equivalent.
// SupportsListener returns false so that clients run in poll only mode.
func (d *Driver) GetListener(params *riverdriver.GetListenenerParams) riverdriver.Listener {
	panic(riverdriver.ErrNotImplemented)
}

func (d *Driver) GetMigrationDefaultLines() []string { return []string{riverdriver.MigrationLineMain} }

func (d *Driver) GetMigrationFS(line string) fs.FS {
	if line == riverdriver.MigrationLineMain {
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}

func (d *Driver) GetMigrationLines() []string { return []string{riverdriver.MigrationLineMain} }

func (d *Driver) GetMigrationTruncateTables(line string, version int) []string {
	if line == riverdriver.MigrationLineMain {
		return riverdriver.MigrationLineMainTruncateTables(version)
	}
	panic("migration line does not exist: " + line)
}

func (d *Driver) PoolIsSet() bool { return d.dbPool != nil }

func (d *Driver) PoolSet(dbPool any) error { return riverdriver.ErrNotImplemented }

func (d *Driver) SQLFragmentColumnContainsAll(column, namedArg string, values []string) (string, any, error) {
	arg, err := json.Marshal(values)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(`NOT EXISTS (
    SELECT 1
    FROM OPENJSON(@%s) AS filter_value
    WHERE NOT EXISTS (
        SELECT 1
        FROM OPENJSON(%s) AS column_value
        WHERE column_value.value COLLATE Latin1_General_100_BIN2 = filter_value.value
    )
)`, namedArg, column), string(arg), nil
}

func (d *Driver) SQLFragmentColumnContainsAny(column, namedArg string, values []string) (string, any, error) {
	arg, err := json.Marshal(values)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf(`EXISTS (
    SELECT 1
    FROM OPENJSON(%s) AS column_value
    INNER JOIN OPENJSON(@%s) AS filter_value
        ON column_value.value COLLATE Latin1_General_100_BIN2 = filter_value.value
)`, column, namedArg), string(arg), nil
}

func (d *Driver) SQLFragmentColumnIn(column string, values any) (string, any, error) {
	arg, err := json.Marshal(values)
	if err != nil {
		return "", nil, err
	}

	// Strings are compared with the binary collation of River's string
	// columns. Other values (job IDs and priorities) are integers.
	valueSQL := "CAST(value AS bigint)"
	if _, ok := values.([]string); ok {
		valueSQL = "value COLLATE Latin1_General_100_BIN2"
	}

	return fmt.Sprintf("%s IN (SELECT %s FROM OPENJSON(@%s))", column, valueSQL, column), string(arg), nil
}

func (d *Driver) SupportsListener() bool       { return false }
func (d *Driver) SupportsListenNotify() bool   { return false }
func (d *Driver) TimePrecision() time.Duration { return time.Microsecond }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	// Allows UnwrapExecutor to be invoked even if driver is nil.
	var replacer *sqlctemplate.Replacer
	if d == nil {
		replacer = &sqlctemplate.Replacer{}
	} else {
		replacer = &d.replacer
	}

	executorTx := &ExecutorTx{tx: tx}
	executorTx.Executor = Executor{nil, templateReplaceWrapper{tx, replacer}, d, executorTx}

	return executorTx
}

func (d *Driver) UnwrapTx(execTx riverdriver.ExecutorTx) *sql.Tx {
	switch execTx := execTx.(type) {
	case *ExecutorSubTx:
		return execTx.tx
	case *ExecutorTx:
		return execTx.tx
	}
	panic("unhandled executor type")
}

// Executor is an implementation of riverdriver.Executor for SQL Server. It
// runs operations against a database pool or transaction.
type Executor struct {
	dbPool *sql.DB
	dbtx   templateReplaceWrapper
	driver *Driver
	execTx riverdriver.ExecutorTx
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if e.execTx != nil {
		return e.execTx.Begin(ctx)
	}

	tx, err := e.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return e.driver.UnwrapExecutor(tx), nil
}

func (e *Executor) ColumnExists(ctx context.Context, params *riverdriver.ColumnExistsParams) (bool, error) {
	const query = `
		SELECT CAST(CASE WHEN EXISTS (
			SELECT 1
			FROM sys.columns AS columns
			INNER JOIN sys.tables AS tables ON tables.object_id = columns.object_id
			WHERE tables.schema_id = SCHEMA_ID(` + schemaNameSQL + `)
				AND tables.name = @table
				AND columns.name = @column
		) THEN 1 ELSE 0 END AS bit)`

	var exists bool
	err := e.dbtx.QueryRowContext(ctx, query,
		sql.Named("column", params.Column),
		sql.Named("schema", params.Schema),
		sql.Named("table", params.Table),
	).Scan(&exists)
	return exists, interpretError(err)
}

func (e *Executor) Exec(ctx context.Context, query string, args ...any) error {
	_, err := e.dbtx.ExecContext(ctx, query, args...)
	return interpretError(err)
}

func (e *Executor) IndexDropIfExists(ctx context.Context, params *riverdriver.IndexDropIfExistsParams) error {
	// Index names in SQL Server are scoped to their table, which has to be
	// named to drop one, so look it up from the index name.
	const query = indexTableLookupSQL + `
		IF @table_name IS NOT NULL
		BEGIN
			DECLARE @statement nvarchar(max) = N'DROP INDEX ' + QUOTENAME(@index) + N' ON ' + QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name);
			EXEC sp_executesql @statement;
		END;`

	_, err := e.dbtx.ExecContext(ctx, query,
		sql.Named("index", params.Index),
		sql.Named("schema", params.Schema),
	)
	return interpretError(err)
}

func (e *Executor) IndexExists(ctx context.Context, params *riverdriver.IndexExistsParams) (bool, error) {
	const query = `
		SELECT CAST(CASE WHEN EXISTS (
			SELECT 1
			FROM sys.indexes AS indexes
			INNER JOIN sys.tables AS tables ON tables.object_id = indexes.object_id
			WHERE tables.schema_id = SCHEMA_ID(` + schemaNameSQL + `)
				AND indexes.name = @index
		) THEN 1 ELSE 0 END AS bit)`

	var exists bool
	err := e.dbtx.QueryRowContext(ctx, query,
		sql.Named("index", params.Index),
		sql.Named("schema", params.Schema),
	).Scan(&exists)
	return exists, interpretError(err)
}

func (e *Executor) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	// Reorganizing is online on every edition of SQL Server, unlike an online
	// rebuild, which makes it the closest equivalent to Postgres' `REINDEX
	// CONCURRENTLY`.
	const query = indexTableLookupSQL + `
		IF @table_name IS NULL
		BEGIN
			DECLARE @message nvarchar(2048) = N'index ' + QUOTENAME(@index) + N' does not exist in schema ' + QUOTENAME(@schema_name);
			THROW 50000, @message, 1;
		END;

		DECLARE @statement nvarchar(max) = N'ALTER INDEX ' + QUOTENAME(@index) + N' ON ' + QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name) + N' REORGANIZE';
		EXEC sp_executesql @statement;`

	_, err := e.dbtx.ExecContext(ctx, query,
		sql.Named("index", params.Index),
		sql.Named("schema", params.Schema),
	)
	return interpretError(err)
}

// IndexReindexArtifacts returns nothing because reorganizing an index doesn't
// leave artifacts behind like Postgres' `REINDEX CONCURRENTLY` can.
func (e *Executor) IndexReindexArtifacts(ctx context.Context, params *riverdriver.IndexReindexArtifactsParams) ([]string, error) {
	return nil, nil
}

func (e *Executor) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	exists := make(map[string]bool, len(params.IndexNames))
	for _, index := range params.IndexNames {
		indexExists, err := e.IndexExists(ctx, &riverdriver.IndexExistsParams{
			Index:  index,
			Schema: params.Schema,
		})
		if err != nil {
			return nil, err
		}
		exists[index] = indexExists
	}

	return exists, nil
}

func (e *Executor) InitDriver(context.Context) error { return nil }

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	cancelAttemptedAt, err := params.CancelAttemptedAt.UTC().MarshalJSON()
	if err != nil {
		return nil, err
	}
	cancelAttemptedAt = cancelAttemptedAt[1 : len(cancelAttemptedAt)-1] // remove quotes because the time is set as a JSON string

	// If the update comes back empty because the job doesn't exist or is
	// already finalized, the job is fetched to return it as it is or the most
	// appropriate error.
	return dbutil.WithTxV(ctx, e.executor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		const query = `
			UPDATE /* TEMPLATE: schema */river_job
			SET
				-- If the job is actively running, we want to let its current
				-- client and producer handle the cancellation. Otherwise,
				-- immediately cancel it.
				state = CASE WHEN state = N'running' THEN state ELSE N'cancelled' END,
				finalized_at = CASE WHEN state = N'running' THEN finalized_at ELSE ` + nowSQL + ` END,
				-- Mark the job as cancelled by query so that the rescuer knows
				-- not to rescue it, even if it gets stuck in the running state.
				metadata = JSON_MODIFY(metadata, '$.cancel_attempted_at', @cancel_attempted_at)
			OUTPUT ` + jobColumnsInserted + `
			WHERE id = @id
				AND state NOT IN (N'cancelled', N'completed', N'discarded')
				AND finalized_at IS NULL`

		job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.txDBTX(execTx), query,
			sql.Named("cancel_attempted_at", string(cancelAttemptedAt)),
			sql.Named("id", params.ID),
			sql.Named("now", nullableTime(params.Now)),
		)
		if errors.Is(err, rivertype.ErrNotFound) {
			return execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
				ID:     params.ID,
				Schema: params.Schema,
			})
		}
		if err != nil {
			return nil, err
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobCountByAllStates(ctx context.Context, params *riverdriver.JobCountByAllStatesParams) (map[rivertype.JobState]int, error) {
	const query = `
		SELECT state, count(*)
		FROM /* TEMPLATE: schema */river_job
		GROUP BY state`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	countsMap := make(map[rivertype.JobState]int)
	for _, state := range rivertype.JobStates() {
		countsMap[state] = 0
	}
	for rows.Next() {
		var (
			count int
			state string
		)
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		countsMap[rivertype.JobState(state)] = count
	}
	return countsMap, interpretError(rows.Err())
}

func (e *Executor) JobCountByQueueAndState(ctx context.Context, params *riverdriver.JobCountByQueueAndStateParams) ([]*riverdriver.JobCountByQueueAndStateResult, error) {
	queueNames, err := jsonArrayOrNull(params.QueueNames)
	if err != nil {
		return nil, err
	}

	// Returns a row for every requested queue, including those without jobs.
	const query = `
		SELECT
			all_queues.queue,
			count(CASE WHEN river_job.state = N'available' THEN 1 END),
			count(CASE WHEN river_job.state = N'running' THEN 1 END)
		FROM (
			SELECT DISTINCT value COLLATE Latin1_General_100_BIN2 AS queue
			FROM OPENJSON(@queue_names)
		) AS all_queues
		LEFT JOIN /* TEMPLATE: schema */river_job AS river_job
			ON river_job.queue = all_queues.queue
			AND river_job.state IN (N'available', N'running')
		GROUP BY all_queues.queue
		ORDER BY all_queues.queue`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query, sql.Named("queue_names", queueNames))
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	results := make([]*riverdriver.JobCountByQueueAndStateResult, 0, len(params.QueueNames))
	for rows.Next() {
		var result riverdriver.JobCountByQueueAndStateResult
		if err := rows.Scan(&result.Queue, &result.CountAvailable, &result.CountRunning); err != nil {
			return nil, err
		}
		results = append(results, &result)
	}
	return results, interpretError(rows.Err())
}

func (e *Executor) JobCountByState(ctx context.Context, params *riverdriver.JobCountByStateParams) (int, error) {
	const query = `
		SELECT count(*)
		FROM /* TEMPLATE: schema */river_job
		WHERE state = @state`

	var numJobs int
	err := e.dbtx.QueryRowContext(schemaTemplateParam(ctx, params.Schema), query, sql.Named("state", string(params.State))).Scan(&numJobs)
	return numJobs, interpretError(err)
}

func (e *Executor) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	// If the delete comes back empty because the job doesn't exist or is
	// running, the job is fetched to return the most appropriate error.
	return dbutil.WithTxV(ctx, e.executor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		const query = `
			DELETE FROM /* TEMPLATE: schema */river_job
			OUTPUT ` + jobColumnsDeleted + `
			WHERE id = @id
				-- Do not touch running jobs:
				AND state <> N'running'`

		job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.txDBTX(execTx), query, sql.Named("id", params.ID))
		if errors.Is(err, rivertype.ErrNotFound) {
			job, err := execTx.JobGetByID(ctx, (*riverdriver.JobGetByIDParams)(params))
			if err != nil {
				return nil, err
			}
			if job.State == rivertype.JobStateRunning {
				return nil, rivertype.ErrJobRunning
			}
			return nil, fmt.Errorf("bug; expected only to fetch a job with state %q, but was: %q", rivertype.JobStateRunning, job.State)
		}
		if err != nil {
			return nil, err
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	queuesExcluded, err := jsonArrayOrNull(params.QueuesExcluded)
	if err != nil {
		return 0, err
	}
	queuesIncluded, err := jsonArrayOrNull(params.QueuesIncluded)
	if err != nil {
		return 0, err
	}

	const query = `
		DELETE FROM /* TEMPLATE: schema */river_job
		WHERE id IN (
			SELECT TOP (@max) id
			FROM /* TEMPLATE: schema */river_job
			WHERE (
					(state = N'cancelled' AND @cancelled_do_delete = 1 AND finalized_at < CAST(@cancelled_finalized_at_horizon AS datetime2(6))) OR
					(state = N'completed' AND @completed_do_delete = 1 AND finalized_at < CAST(@completed_finalized_at_horizon AS datetime2(6))) OR
					(state = N'discarded' AND @discarded_do_delete = 1 AND finalized_at < CAST(@discarded_finalized_at_horizon AS datetime2(6)))
				)
				AND (
					@queues_excluded IS NULL
					OR queue NOT IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@queues_excluded))
				)
				AND (
					@queues_included IS NULL
					OR queue IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@queues_included))
				)
			ORDER BY id
		)`

	return e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("cancelled_do_delete", params.CancelledDoDelete),
		sql.Named("cancelled_finalized_at_horizon", params.CancelledFinalizedAtHorizon.UTC()),
		sql.Named("completed_do_delete", params.CompletedDoDelete),
		sql.Named("completed_finalized_at_horizon", params.CompletedFinalizedAtHorizon.UTC()),
		sql.Named("discarded_do_delete", params.DiscardedDoDelete),
		sql.Named("discarded_finalized_at_horizon", params.DiscardedFinalizedAtHorizon.UTC()),
		sql.Named("max", int64(params.Max)),
		sql.Named("queues_excluded", queuesExcluded),
		sql.Named("queues_included", queuesIncluded),
	)
}

func (e *Executor) JobDeleteMany(ctx context.Context, params *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error) {
	// Positional arguments only because the named arguments of the where clause
	// are rendered as positional arguments after them.
	const query = `
		DELETE FROM /* TEMPLATE: schema */river_job
		OUTPUT ` + jobColumnsDeleted + `
		WHERE id IN (
			SELECT TOP (@p1) id
			FROM /* TEMPLATE: schema */river_job
			WHERE (/* TEMPLATE_BEGIN: where_clause */ 1 = 1 /* TEMPLATE_END */)
				AND state <> N'running'
			ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
		)`

	jobs, err := queryJobs(schemaTemplateParam(jobListContext(ctx, (*riverdriver.JobListParams)(params)), params.Schema), e.dbtx, query, int64(params.Max))
	if err != nil {
		return nil, err
	}
	// The order of rows output by a delete isn't guaranteed, so order them by
	// ID like the SQLite driver, which has the same problem.
	slices.SortFunc(jobs, func(j1, j2 *jobRow) int { return cmp.Compare(j1.ID, j2.ID) })
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) (*riverdriver.JobGetAvailableResult, error) {
	kinds, err := jsonArrayOrNull(params.Kind)
	if err != nil {
		return nil, err
	}

	// `READPAST` skips rows locked by other clients like Postgres' `SKIP
	// LOCKED`. `attempted_by` is trimmed to its last `max_attempted_by - 1`
	// elements before the new client ID is appended.
	const query = `
		WITH locked_jobs AS (
			SELECT TOP (@max_to_lock) *
			FROM /* TEMPLATE: schema */river_job WITH (UPDLOCK, READPAST, ROWLOCK)
			WHERE state = N'available'
				AND queue = @queue
				AND (@kind IS NULL OR kind IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kind)))
				AND scheduled_at <= ` + nowSQL + `
			ORDER BY
				priority ASC,
				scheduled_at ASC,
				id ASC
		)
		UPDATE locked_jobs
		SET
			attempt = attempt + 1,
			attempted_at = ` + nowSQL + `,
			attempted_by = (
				SELECT N'[' + STRING_AGG(attempted_by_element.element, N',') WITHIN GROUP (ORDER BY attempted_by_element.ordinal) + N']'
				FROM (
					SELECT kept.ordinal, kept.element
					FROM (
						SELECT TOP (@max_attempted_by_kept)
							CAST([key] AS int) AS ordinal,
							CAST(N'"' + STRING_ESCAPE(value, 'json') + N'"' AS nvarchar(max)) AS element
						FROM OPENJSON(locked_jobs.attempted_by)
						ORDER BY CAST([key] AS int) DESC
					) AS kept
					UNION ALL
					SELECT 2147483647, CAST(N'"' + STRING_ESCAPE(@attempted_by, 'json') + N'"' AS nvarchar(max))
				) AS attempted_by_element
			),
			state = N'running'
		OUTPUT ` + jobColumnsInserted

	jobs, err := queryJobs(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("attempted_by", params.ClientID),
		sql.Named("kind", kinds),
		sql.Named("max_attempted_by_kept", int64(max(params.MaxAttemptedBy-1, 0))),
		sql.Named("max_to_lock", int64(params.MaxToLock)),
		sql.Named("now", nullableTime(params.Now)),
		sql.Named("queue", params.Queue),
	)
	if err != nil {
		return nil, err
	}

	// The order of rows output by an update isn't guaranteed, so return them in
	// the order they were selected in.
	slices.SortFunc(jobs, func(j1, j2 *jobRow) int {
		return cmp.Or(
			cmp.Compare(j1.Priority, j2.Priority),
			j1.ScheduledAt.Compare(j2.ScheduledAt),
			cmp.Compare(j1.ID, j2.ID),
		)
	})
	return jobGetAvailableResultFromInternal(jobs), nil
}

func (e *Executor) JobGetByID(ctx context.Context, params *riverdriver.JobGetByIDParams) (*rivertype.JobRow, error) {
	const query = `
		SELECT ` + jobColumns + `
		FROM /* TEMPLATE: schema */river_job
		WHERE id = @id`

	job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, sql.Named("id", params.ID))
	if err != nil {
		return nil, err
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByIDMany(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := e.jobGetByIDMany(ctx, params.Schema, params.ID)
	if err != nil {
		return nil, err
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByKindMany(ctx context.Context, params *riverdriver.JobGetByKindManyParams) ([]*rivertype.JobRow, error) {
	kinds, err := jsonArrayOrNull(params.Kind)
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT ` + jobColumns + `
		FROM /* TEMPLATE: schema */river_job
		WHERE kind IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kind))
		ORDER BY id`

	jobs, err := queryJobs(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, sql.Named("kind", kinds))
	if err != nil {
		return nil, err
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetCancelRequested(ctx context.Context, params *riverdriver.JobGetCancelRequestedParams) ([]int64, error) {
	ids, err := jsonArrayOrNull(params.ID)
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT id
		FROM /* TEMPLATE: schema */river_job
		WHERE id IN (SELECT CAST(value AS bigint) FROM OPENJSON(@id))
			AND JSON_PATH_EXISTS(metadata, '$.cancel_attempted_at') = 1
			AND state = N'running'
		ORDER BY id`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query, sql.Named("id", ids))
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var cancelRequestedIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		cancelRequestedIDs = append(cancelRequestedIDs, id)
	}
	return cancelRequestedIDs, interpretError(rows.Err())
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	const query = `
		SELECT TOP (@max) ` + jobColumns + `
		FROM /* TEMPLATE: schema */river_job
		WHERE state = N'running'
			AND id > @after_id
			AND attempted_at < CAST(@stuck_horizon AS datetime2(6))
		ORDER BY id`

	jobs, err := queryJobs(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("after_id", params.AfterID),
		sql.Named("max", int64(params.Max)),
		sql.Named("stuck_horizon", params.StuckHorizon.UTC()),
	)
	if err != nil {
		return nil, err
	}
	return jobRowsFromInternalPartial(jobs), nil
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	if len(params.Jobs) < 1 {
		return []*riverdriver.JobInsertFastResult{}, nil
	}

	// Postgres rejects a statement that would affect the same unique row twice.
	// The merge below would instead insert both jobs and violate the unique
	// index, so reject repeated keys up front like the SQLite driver.
	seenUniqueKeys := make(map[string]bool, len(params.Jobs))
	for _, job := range params.Jobs {
		if !uniqueKeyEnforced(job) {
			continue
		}
		if seenUniqueKeys[string(job.UniqueKey)] {
			return nil, errors.New("unique key appears more than once in batch")
		}
		seenUniqueKeys[string(job.UniqueKey)] = true
	}

	jobsParam, numIDsToAllocate, err := jobInsertFastManyParam(params.Jobs)
	if err != nil {
		return nil, err
	}

	// A job conflicting with an existing one on its unique key is matched by
	// the merge instead of inserted, and the existing job is output with
	// `$action` set to UPDATE. `HOLDLOCK` keeps a concurrent insert of the
	// same key from slipping in between the match and the insert.
	const query = jobIDRangeSQL + `
		MERGE INTO /* TEMPLATE: schema */river_job WITH (HOLDLOCK) AS river_job
		USING (` + jobInsertFastSourceSQL + `) AS job_input
		ON river_job.unique_key_enforced = job_input.unique_key_enforced
		-- Something needs to be updated for a row to be output on a conflict.
		-- Keep the existing kind, which may differ under ExcludeKind.
		WHEN MATCHED THEN
			UPDATE SET kind = river_job.kind
		WHEN NOT MATCHED THEN
			INSERT (` + jobInsertFastColumns + `)
			VALUES (` + jobInsertFastValues + `)
		OUTPUT
			job_input.ordinal,
			CAST(CASE WHEN $action = N'UPDATE' THEN 1 ELSE 0 END AS bit),
			` + jobColumnsInserted + `;`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("id_range_size", numIDsToAllocate),
		sql.Named("jobs", jobsParam),
		sql.Named("sequence_name", jobIDSequenceName(params.Schema)),
	)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	results := make([]*riverdriver.JobInsertFastResult, len(params.Jobs))
	for rows.Next() {
		var (
			job                      jobRow
			ordinal                  int
			uniqueSkippedAsDuplicate bool
		)
		if err := rows.Scan(append([]any{&ordinal, &uniqueSkippedAsDuplicate}, job.scanDest()...)...); err != nil {
			return nil, err
		}

		jobRow, err := jobRowFromInternal(&job)
		if err != nil {
			return nil, err
		}
		results[ordinal] = &riverdriver.JobInsertFastResult{
			Job:                      jobRow,
			UniqueSkippedAsDuplicate: uniqueSkippedAsDuplicate,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, interpretError(err)
	}

	if slices.Contains(results, nil) {
		return nil, errors.New("bug; merge didn't output a row for every job")
	}
	return results, nil
}

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	// Like with Postgres' `ON CONFLICT DO NOTHING`, a job whose unique key
	// repeats one earlier in the batch is skipped rather than conflicting with
	// it.
	jobs := make([]*riverdriver.JobInsertFastParams, 0, len(params.Jobs))
	seenUniqueKeys := make(map[string]bool, len(params.Jobs))
	for _, job := range params.Jobs {
		if uniqueKeyEnforced(job) {
			if seenUniqueKeys[string(job.UniqueKey)] {
				continue
			}
			seenUniqueKeys[string(job.UniqueKey)] = true
		}
		jobs = append(jobs, job)
	}
	if len(jobs) < 1 {
		return 0, nil
	}

	jobsParam, numIDsToAllocate, err := jobInsertFastManyParam(jobs)
	if err != nil {
		return 0, err
	}

	const query = jobIDRangeSQL + `
		MERGE INTO /* TEMPLATE: schema */river_job WITH (HOLDLOCK) AS river_job
		USING (` + jobInsertFastSourceSQL + `) AS job_input
		ON river_job.unique_key_enforced = job_input.unique_key_enforced
		WHEN NOT MATCHED THEN
			INSERT (` + jobInsertFastColumns + `)
			VALUES (` + jobInsertFastValues + `);

		SELECT @@ROWCOUNT;`

	var numInserted int
	err = e.dbtx.QueryRowContext(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("id_range_size", numIDsToAllocate),
		sql.Named("jobs", jobsParam),
		sql.Named("sequence_name", jobIDSequenceName(params.Schema)),
	).Scan(&numInserted)
	return numInserted, interpretError(err)
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	jobs, err := e.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
		Jobs:   []*riverdriver.JobInsertFullParams{params},
		Schema: params.Schema,
	})
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (e *Executor) JobInsertFullMany(ctx context.Context, params *riverdriver.JobInsertFullManyParams) ([]*rivertype.JobRow, error) {
	if len(params.Jobs) < 1 {
		return []*rivertype.JobRow{}, nil
	}

	type jobInput struct {
		Args         json.RawMessage `json:"args"`
		Attempt      int             `json:"attempt"`
		AttemptedAt  *string         `json:"attempted_at"`
		AttemptedBy  []string        `json:"attempted_by"`
		CreatedAt    *string         `json:"created_at"`
		Errors       json.RawMessage `json:"errors"`
		FinalizedAt  *string         `json:"finalized_at"`
		IDOffset     int             `json:"id_offset"`
		Kind         string          `json:"kind"`
		MaxAttempts  int             `json:"max_attempts"`
		Metadata     json.RawMessage `json:"metadata"`
		Priority     int             `json:"priority"`
		Queue        string          `json:"queue"`
		ScheduledAt  *string         `json:"scheduled_at"`
		State        string          `json:"state"`
		Tags         []string        `json:"tags"`
		UniqueKey    *string         `json:"unique_key"`
		UniqueStates *int            `json:"unique_states"`
	}

	jobInputs := make([]jobInput, len(params.Jobs))
	for i, job := range params.Jobs {
		attemptErrors, err := attemptErrorsJSON(job.Errors)
		if err != nil {
			return nil, err
		}

		jobInputs[i] = jobInput{
			Args:         job.EncodedArgs,
			Attempt:      job.Attempt,
			AttemptedAt:  nullableTimeString(job.AttemptedAt),
			AttemptedBy:  job.AttemptedBy,
			CreatedAt:    nullableTimeString(job.CreatedAt),
			Errors:       attemptErrors,
			FinalizedAt:  nullableTimeString(job.FinalizedAt),
			IDOffset:     i,
			Kind:         job.Kind,
			MaxAttempts:  job.MaxAttempts,
			Metadata:     sliceutil.FirstNonEmpty(job.Metadata, []byte("{}")),
			Priority:     job.Priority,
			Queue:        job.Queue,
			ScheduledAt:  nullableTimeString(job.ScheduledAt),
			State:        string(job.State),
			Tags:         emptyIfNil(job.Tags),
			UniqueKey:    nullableHexString(job.UniqueKey),
			UniqueStates: nullableUniqueStates(job.UniqueStates),
		}
	}

	jobsParam, err := json.Marshal(jobInputs)
	if err != nil {
		return nil, err
	}

	const query = jobIDRangeSQL + `
		INSERT INTO /* TEMPLATE: schema */river_job (
			id,
			args,
			attempt,
			attempted_at,
			attempted_by,
			created_at,
			errors,
			finalized_at,
			kind,
			max_attempts,
			metadata,
			priority,
			queue,
			scheduled_at,
			state,
			tags,
			unique_key,
			unique_states
		)
		OUTPUT ` + jobColumnsInserted + `
		SELECT
			CAST(@id_range_first AS bigint) + job_input.id_offset,
			job_input.args,
			job_input.attempt,
			job_input.attempted_at,
			job_input.attempted_by,
			coalesce(job_input.created_at, sysutcdatetime()),
			job_input.errors,
			job_input.finalized_at,
			job_input.kind,
			job_input.max_attempts,
			job_input.metadata,
			job_input.priority,
			job_input.queue,
			coalesce(job_input.scheduled_at, sysutcdatetime()),
			job_input.state,
			job_input.tags,
			CONVERT(varbinary(255), job_input.unique_key, 2),
			job_input.unique_states
		FROM OPENJSON(@jobs) WITH (
			args nvarchar(max) '$.args' AS JSON,
			attempt smallint '$.attempt',
			attempted_at datetime2(6) '$.attempted_at',
			attempted_by nvarchar(max) '$.attempted_by' AS JSON,
			created_at datetime2(6) '$.created_at',
			errors nvarchar(max) '$.errors' AS JSON,
			finalized_at datetime2(6) '$.finalized_at',
			id_offset bigint '$.id_offset',
			kind nvarchar(128) '$.kind',
			max_attempts smallint '$.max_attempts',
			metadata nvarchar(max) '$.metadata' AS JSON,
			priority smallint '$.priority',
			queue nvarchar(128) '$.queue',
			scheduled_at datetime2(6) '$.scheduled_at',
			state nvarchar(16) '$.state',
			tags nvarchar(max) '$.tags' AS JSON,
			unique_key varchar(510) '$.unique_key',
			unique_states tinyint '$.unique_states'
		) AS job_input;`

	jobs, err := queryJobs(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("id_range_size", int64(len(params.Jobs))),
		sql.Named("jobs", string(jobsParam)),
		sql.Named("sequence_name", jobIDSequenceName(params.Schema)),
	)
	if err != nil {
		return nil, err
	}
	// IDs were allocated in input order, but the order of rows output by an
	// insert isn't guaranteed.
	slices.SortFunc(jobs, func(j1, j2 *jobRow) int { return cmp.Compare(j1.ID, j2.ID) })
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobKindList(ctx context.Context, params *riverdriver.JobKindListParams) ([]string, error) {
	exclude, err := json.Marshal(emptyIfNil(params.Exclude))
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT DISTINCT TOP (@max) kind
		FROM /* TEMPLATE: schema */river_job
		WHERE (@match = N'' OR LOWER(kind) LIKE N'%' + LOWER(@match) + N'%')
			AND (@after = N'' OR kind > @after)
			AND kind NOT IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@exclude))
		ORDER BY kind ASC`

	return e.queryStrings(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("after", params.After),
		sql.Named("exclude", string(exclude)),
		sql.Named("match", params.Match),
		sql.Named("max", int64(min(params.Max, math.MaxInt32))),
	)
}

func (e *Executor) JobList(ctx context.Context, params *riverdriver.JobListParams) ([]*rivertype.JobRow, error) {
	// Positional arguments only because the named arguments of the where clause
	// are rendered as positional arguments after them.
	const query = `
		SELECT TOP (@p1) ` + jobColumns + `
		FROM /* TEMPLATE: schema */river_job
		WHERE /* TEMPLATE_BEGIN: where_clause */ 1 = 1 /* TEMPLATE_END */
		ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */`

	jobs, err := queryJobs(schemaTemplateParam(jobListContext(ctx, params), params.Schema), e.dbtx, query, int64(params.Max))
	if err != nil {
		return nil, err
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	type jobInput struct {
		ID          int64           `json:"id"`
		Error       json.RawMessage `json:"error"`
		FinalizedAt *string         `json:"finalized_at"`
		ScheduledAt string          `json:"scheduled_at"`
		State       string          `json:"state"`
	}

	jobInputs := make([]jobInput, len(params.ID))
	for i := range params.ID {
		jobInputs[i] = jobInput{
			ID:          params.ID[i],
			Error:       params.Error[i],
			FinalizedAt: nullableTimeString(params.FinalizedAt[i]),
			ScheduledAt: timeString(params.ScheduledAt[i]),
			State:       params.State[i],
		}
	}

	jobsParam, err := json.Marshal(jobInputs)
	if err != nil {
		return nil, err
	}

	const query = `
		UPDATE river_job
		SET
			errors = JSON_MODIFY(coalesce(river_job.errors, N'[]'), 'append $', JSON_QUERY(job_input.error_data)),
			finalized_at = job_input.finalized_at,
			scheduled_at = job_input.scheduled_at,
			metadata = JSON_MODIFY(
				river_job.metadata,
				'$."river:rescue_count"',
				coalesce((
					SELECT TOP (1) TRY_CAST(value AS int)
					FROM OPENJSON(river_job.metadata)
					WHERE [key] = N'river:rescue_count'
						AND type = 2 -- number
				), 0) + 1
			),
			state = job_input.state
		FROM /* TEMPLATE: schema */river_job AS river_job
		INNER JOIN OPENJSON(@jobs) WITH (
			id bigint '$.id',
			error_data nvarchar(max) '$.error' AS JSON,
			finalized_at datetime2(6) '$.finalized_at',
			scheduled_at datetime2(6) '$.scheduled_at',
			state nvarchar(16) '$.state'
		) AS job_input ON river_job.id = job_input.id
		WHERE river_job.state = N'running'
			AND river_job.attempted_at < CAST(@stuck_horizon AS datetime2(6))`

	if _, err := e.dbtx.ExecContext(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("jobs", string(jobsParam)),
		sql.Named("stuck_horizon", params.StuckHorizon.UTC()),
	); err != nil {
		return nil, interpretError(err)
	}

	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	// If the update comes back empty because the job doesn't exist, is
	// running, or is already available, the job is fetched to return it as it
	// is or the most appropriate error.
	return dbutil.WithTxV(ctx, e.executor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		const query = `
			UPDATE /* TEMPLATE: schema */river_job
			SET
				state = N'available',
				max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
				finalized_at = NULL,
				scheduled_at = ` + nowSQL + `
			OUTPUT ` + jobColumnsInserted + `
			WHERE id = @id
				-- Do not touch running jobs:
				AND state <> N'running'
				-- If the job is already available with a prior scheduled_at, leave it alone.
				AND NOT (
					state = N'available'
					AND scheduled_at < ` + nowSQL + `
				)`

		job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.txDBTX(execTx), query,
			sql.Named("id", params.ID),
			sql.Named("now", nullableTime(params.Now)),
		)
		if errors.Is(err, rivertype.ErrNotFound) {
			return execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
				ID:     params.ID,
				Schema: params.Schema,
			})
		}
		if err != nil {
			return nil, err
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	// Jobs are made available unless that would conflict with the unique key
	// of another job, either an existing one or another one being scheduled.
	// In that case, the job is discarded instead (or all but the first one in
	// the batch to be scheduled with the same key are).
	const query = `
		WITH jobs_to_schedule AS (
			SELECT TOP (@max)
				id,
				unique_key,
				unique_states,
				priority,
				scheduled_at
			FROM /* TEMPLATE: schema */river_job WITH (UPDLOCK, ROWLOCK)
			WHERE state IN (N'retryable', N'scheduled')
				AND priority >= 0
				AND scheduled_at <= ` + nowSQL + `
			ORDER BY
				priority,
				scheduled_at,
				id
		),
		jobs_with_unique_row_number AS (
			SELECT
				*,
				CASE
					WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL
					THEN ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY priority, scheduled_at, id)
				END AS unique_row_number
			FROM jobs_to_schedule
		),
		unique_conflicts AS (
			SELECT DISTINCT river_job.unique_key
			FROM /* TEMPLATE: schema */river_job AS river_job
			INNER JOIN jobs_with_unique_row_number AS job
				ON river_job.unique_key = job.unique_key
				AND river_job.id <> job.id
			WHERE river_job.unique_key IS NOT NULL
				AND river_job.unique_states IS NOT NULL
				AND (river_job.unique_states & CASE river_job.state
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
		),
		job_updates AS (
			SELECT
				job.id,
				CASE
					WHEN job.unique_row_number IS NULL THEN N'available'
					WHEN unique_conflicts.unique_key IS NOT NULL THEN N'discarded'
					WHEN job.unique_row_number = 1 THEN N'available'
					ELSE N'discarded'
				END AS new_state
			FROM jobs_with_unique_row_number AS job
			LEFT JOIN unique_conflicts ON job.unique_key = unique_conflicts.unique_key
		)
		UPDATE river_job
		SET
			state = job_updates.new_state,
			finalized_at = CASE WHEN job_updates.new_state = N'discarded' THEN ` + nowSQL + ` ELSE river_job.finalized_at END,
			metadata = CASE WHEN job_updates.new_state = N'discarded'
				THEN JSON_MODIFY(river_job.metadata, '$.unique_key_conflict', N'scheduler_discarded')
				ELSE river_job.metadata END
		OUTPUT
			CAST(CASE WHEN inserted.state = N'discarded' THEN 1 ELSE 0 END AS bit),
			` + jobColumnsInserted + `
		FROM /* TEMPLATE: schema */river_job AS river_job
		INNER JOIN job_updates ON river_job.id = job_updates.id`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("max", int64(params.Max)),
		sql.Named("now", nullableTime(params.Now)),
	)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var results []*riverdriver.JobScheduleResult
	for rows.Next() {
		var (
			conflictDiscarded bool
			job               jobRow
		)
		if err := rows.Scan(append([]any{&conflictDiscarded}, job.scanDest()...)...); err != nil {
			return nil, err
		}

		// A job whose row can't be fully decoded is still returned (with the
		// undecodable fields left empty) so that it doesn't fail scheduling
		// for every other job.
		jobRow, _ := jobRowFromInternalPartial(&job)
		results = append(results, &riverdriver.JobScheduleResult{
			ConflictDiscarded: conflictDiscarded,
			Job:               *jobRow,
		})
	}
	return results, interpretError(rows.Err())
}

func (e *Executor) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	type jobInput struct {
		ID              int64           `json:"id"`
		Attempt         *int            `json:"attempt"`
		ErrData         json.RawMessage `json:"err_data"`
		FinalizedAt     *string         `json:"finalized_at"`
		MetadataDoMerge bool            `json:"metadata_do_merge"`
		MetadataUpdates json.RawMessage `json:"metadata_updates"`
		ScheduledAt     *string         `json:"scheduled_at"`
		State           string          `json:"state"`
	}

	jobInputs := make([]jobInput, len(params.ID))
	for i := range params.ID {
		jobInputs[i] = jobInput{
			ID:              params.ID[i],
			Attempt:         params.Attempt[i],
			FinalizedAt:     nullableTimeString(params.FinalizedAt[i]),
			MetadataDoMerge: params.MetadataDoMerge[i],
			ScheduledAt:     nullableTimeString(params.ScheduledAt[i]),
			State:           string(params.State[i]),
		}
		if len(params.ErrData[i]) > 0 {
			jobInputs[i].ErrData = params.ErrData[i]
		}
		if params.MetadataDoMerge[i] {
			jobInputs[i].MetadataUpdates = sliceutil.FirstNonEmpty(params.MetadataUpdates[i], []byte("{}"))
		}
	}

	jobsParam, err := json.Marshal(jobInputs)
	if err != nil {
		return nil, err
	}

	// A running job that's had a cancellation attempted is cancelled instead of
	// being made available or scheduled for another attempt. A job that isn't
	// running only has its metadata merged.
	const shouldCancelSQL = `(
		job_input.state IN (N'available', N'retryable', N'scheduled')
		AND JSON_PATH_EXISTS(river_job.metadata, '$.cancel_attempted_at') = 1
	)`
	const query = `
		UPDATE river_job
		SET
			attempt = CASE
				WHEN river_job.state = N'running' AND NOT ` + shouldCancelSQL + ` AND job_input.attempt IS NOT NULL
				THEN job_input.attempt
				ELSE river_job.attempt
			END,
			errors = CASE
				WHEN river_job.state = N'running' AND job_input.err_data IS NOT NULL
				THEN JSON_MODIFY(coalesce(river_job.errors, N'[]'), 'append $', JSON_QUERY(job_input.err_data))
				ELSE river_job.errors
			END,
			finalized_at = CASE
				WHEN river_job.state = N'running' AND ` + shouldCancelSQL + `
				THEN ` + nowSQL + `
				WHEN river_job.state = N'running' AND job_input.finalized_at IS NOT NULL
				THEN job_input.finalized_at
				ELSE river_job.finalized_at
			END,
			metadata = CASE
				WHEN job_input.metadata_do_merge = 1
				THEN ` + metadataMergeSQL + `
				ELSE river_job.metadata
			END,
			scheduled_at = CASE
				WHEN river_job.state = N'running' AND NOT ` + shouldCancelSQL + ` AND job_input.scheduled_at IS NOT NULL
				THEN job_input.scheduled_at
				ELSE river_job.scheduled_at
			END,
			state = CASE
				WHEN river_job.state = N'running' AND ` + shouldCancelSQL + `
				THEN N'cancelled'
				WHEN river_job.state = N'running'
				THEN job_input.state
				ELSE river_job.state
			END
		OUTPUT ` + jobColumnsInserted + `
		FROM /* TEMPLATE: schema */river_job AS river_job
		INNER JOIN OPENJSON(@jobs) WITH (
			id bigint '$.id',
			attempt smallint '$.attempt',
			err_data nvarchar(max) '$.err_data' AS JSON,
			finalized_at datetime2(6) '$.finalized_at',
			metadata_do_merge bit '$.metadata_do_merge',
			metadata_updates nvarchar(max) '$.metadata_updates' AS JSON,
			scheduled_at datetime2(6) '$.scheduled_at',
			state nvarchar(16) '$.state'
		) AS job_input ON river_job.id = job_input.id
		WHERE river_job.state = N'running'
			OR job_input.metadata_do_merge = 1`

	jobs, err := queryJobs(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("jobs", string(jobsParam)),
		sql.Named("now", nullableTime(params.Now)),
	)
	if err != nil {
		return nil, err
	}

	// Jobs that weren't updated because they're no longer running are returned
	// as they are, like in Postgres.
	updatedIDs := make(map[int64]bool, len(jobs))
	for _, job := range jobs {
		updatedIDs[job.ID] = true
	}
	notUpdatedIDs := slices.DeleteFunc(slices.Clone(params.ID), func(id int64) bool { return updatedIDs[id] })
	if len(notUpdatedIDs) > 0 {
		notUpdatedJobs, err := e.jobGetByIDMany(ctx, params.Schema, notUpdatedIDs)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, notUpdatedJobs...)
	}

	slices.SortFunc(jobs, func(j1, j2 *jobRow) int { return cmp.Compare(j1.ID, j2.ID) })

	// A job whose row can't be fully decoded is still returned (with the
	// undecodable fields left empty) so that it doesn't fail the state change
	// for every other job in the batch.
	return jobRowsFromInternalPartial(jobs), nil
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	const query = `
		UPDATE river_job
		SET
			metadata = CASE WHEN @metadata_do_merge = 1 THEN ` + metadataMergeSQL + ` ELSE river_job.metadata END
		OUTPUT ` + jobColumnsInserted + `
		FROM /* TEMPLATE: schema */river_job AS river_job
		CROSS JOIN (SELECT CAST(@metadata AS nvarchar(max)) AS metadata_updates) AS job_input
		WHERE river_job.id = @id`

	job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("id", params.ID),
		sql.Named("metadata", string(sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")))),
		sql.Named("metadata_do_merge", params.MetadataDoMerge),
	)
	if err != nil {
		return nil, err
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobUpdateFull(ctx context.Context, params *riverdriver.JobUpdateFullParams) (*rivertype.JobRow, error) {
	attemptedBy, err := jsonArrayOrNull(params.AttemptedBy)
	if err != nil {
		return nil, err
	}

	attemptErrors, err := attemptErrorsJSON(params.Errors)
	if err != nil {
		return nil, err
	}
	var errorsParam any
	if attemptErrors != nil {
		errorsParam = string(attemptErrors)
	}

	const query = `
		UPDATE /* TEMPLATE: schema */river_job
		SET
			attempt = CASE WHEN @attempt_do_update = 1 THEN @attempt ELSE attempt END,
			attempted_at = CASE WHEN @attempted_at_do_update = 1 THEN CAST(@attempted_at AS datetime2(6)) ELSE attempted_at END,
			attempted_by = CASE WHEN @attempted_by_do_update = 1 THEN @attempted_by ELSE attempted_by END,
			errors = CASE WHEN @errors_do_update = 1 THEN @errors ELSE errors END,
			finalized_at = CASE WHEN @finalized_at_do_update = 1 THEN CAST(@finalized_at AS datetime2(6)) ELSE finalized_at END,
			max_attempts = CASE WHEN @max_attempts_do_update = 1 THEN @max_attempts ELSE max_attempts END,
			metadata = CASE WHEN @metadata_do_update = 1 THEN @metadata ELSE metadata END,
			state = CASE WHEN @state_do_update = 1 THEN @state ELSE state END
		OUTPUT ` + jobColumnsInserted + `
		WHERE id = @id`

	job, err := queryJob(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("attempt", int64(params.Attempt)),
		sql.Named("attempt_do_update", params.AttemptDoUpdate),
		sql.Named("attempted_at", nullableTime(params.AttemptedAt)),
		sql.Named("attempted_at_do_update", params.AttemptedAtDoUpdate),
		sql.Named("attempted_by", attemptedBy),
		sql.Named("attempted_by_do_update", params.AttemptedByDoUpdate),
		sql.Named("errors", errorsParam),
		sql.Named("errors_do_update", params.ErrorsDoUpdate),
		sql.Named("finalized_at", nullableTime(params.FinalizedAt)),
		sql.Named("finalized_at_do_update", params.FinalizedAtDoUpdate),
		sql.Named("id", params.ID),
		sql.Named("max_attempts", int64(min(params.MaxAttempts, math.MaxInt16))),
		sql.Named("max_attempts_do_update", params.MaxAttemptsDoUpdate),
		sql.Named("metadata", string(sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")))),
		sql.Named("metadata_do_update", params.MetadataDoUpdate),
		sql.Named("state", string(params.State)),
		sql.Named("state_do_update", params.StateDoUpdate),
	)
	if err != nil {
		return nil, err
	}
	return jobRowFromInternal(job)
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
	// `UPDLOCK, HOLDLOCK` makes concurrent elections wait for each other
	// rather than one failing on the primary key, so a lost election returns
	// no row like Postgres' `ON CONFLICT DO NOTHING`.
	const query = `
		INSERT INTO /* TEMPLATE: schema */river_leader (
			leader_id,
			elected_at,
			expires_at
		)
		OUTPUT ` + leaderColumnsInserted + `
		SELECT
			@leader_id,
			` + nowSQL + `,
			` + ttlFromNowSQL + `
		WHERE NOT EXISTS (
			SELECT 1
			FROM /* TEMPLATE: schema */river_leader WITH (UPDLOCK, HOLDLOCK)
			WHERE name = N'default'
		)`

	return queryLeader(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, append(ttlArgs(params.TTL),
		sql.Named("leader_id", params.LeaderID),
		sql.Named("now", nullableTime(params.Now)),
	)...)
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
	const query = `
		UPDATE /* TEMPLATE: schema */river_leader
		SET expires_at = ` + ttlFromNowSQL + `
		OUTPUT ` + leaderColumnsInserted + `
		WHERE elected_at = CAST(@elected_at AS datetime2(6))
			AND expires_at >= ` + nowSQL + `
			AND leader_id = @leader_id`

	return queryLeader(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, append(ttlArgs(params.TTL),
		sql.Named("elected_at", params.ElectedAt.UTC()),
		sql.Named("leader_id", params.LeaderID),
		sql.Named("now", nullableTime(params.Now)),
	)...)
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	const query = `
		DELETE FROM /* TEMPLATE: schema */river_leader
		WHERE expires_at < ` + nowSQL

	return e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query, sql.Named("now", nullableTime(params.Now)))
}

func (e *Executor) LeaderGetElectedLeader(ctx context.Context, params *riverdriver.LeaderGetElectedLeaderParams) (*riverdriver.Leader, error) {
	const query = `
		SELECT ` + leaderColumns + `
		FROM /* TEMPLATE: schema */river_leader`

	return queryLeader(schemaTemplateParam(ctx, params.Schema), e.dbtx, query)
}

func (e *Executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	const query = `
		INSERT INTO /* TEMPLATE: schema */river_leader (
			elected_at,
			expires_at,
			leader_id
		)
		OUTPUT ` + leaderColumnsInserted + `
		VALUES (
			coalesce(CAST(@elected_at AS datetime2(6)), ` + nowSQL + `),
			coalesce(CAST(@expires_at AS datetime2(6)), ` + ttlFromNowSQL + `),
			@leader_id
		)`

	return queryLeader(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, append(ttlArgs(params.TTL),
		sql.Named("elected_at", nullableTime(params.ElectedAt)),
		sql.Named("expires_at", nullableTime(params.ExpiresAt)),
		sql.Named("leader_id", params.LeaderID),
		sql.Named("now", nullableTime(params.Now)),
	)...)
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	const query = `
		DELETE FROM /* TEMPLATE: schema */river_leader
		WHERE elected_at = CAST(@elected_at AS datetime2(6))
			AND leader_id = @leader_id`

	numResigned, err := e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("elected_at", params.ElectedAt.UTC()),
		sql.Named("leader_id", params.LeaderID),
	)
	return numResigned > 0, err
}

func (e *Executor) MigrationDeleteAssumingMainMany(ctx context.Context, params *riverdriver.MigrationDeleteAssumingMainManyParams) ([]*riverdriver.Migration, error) {
	versions, err := json.Marshal(params.Versions)
	if err != nil {
		return nil, err
	}

	const query = `
		DELETE FROM /* TEMPLATE: schema */river_migration
		OUTPUT deleted.created_at, N'` + riverdriver.MigrationLineMain + `', deleted.version
		WHERE version IN (SELECT CAST(value AS bigint) FROM OPENJSON(@versions))`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query, sql.Named("versions", string(versions)))
}

func (e *Executor) MigrationDeleteByLineAndVersionMany(ctx context.Context, params *riverdriver.MigrationDeleteByLineAndVersionManyParams) ([]*riverdriver.Migration, error) {
	versions, err := json.Marshal(params.Versions)
	if err != nil {
		return nil, err
	}

	const query = `
		DELETE FROM /* TEMPLATE: schema */river_migration
		OUTPUT deleted.created_at, deleted.line, deleted.version
		WHERE line = @line
			AND version IN (SELECT CAST(value AS bigint) FROM OPENJSON(@versions))`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("line", params.Line),
		sql.Named("versions", string(versions)),
	)
}

func (e *Executor) MigrationGetAllAssumingMain(ctx context.Context, params *riverdriver.MigrationGetAllAssumingMainParams) ([]*riverdriver.Migration, error) {
	// Only selects columns that exist before the `line` column was added to the
	// table in version 005.
	const query = `
		SELECT created_at, N'` + riverdriver.MigrationLineMain + `', version
		FROM /* TEMPLATE: schema */river_migration`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query)
}

func (e *Executor) MigrationGetByLine(ctx context.Context, params *riverdriver.MigrationGetByLineParams) ([]*riverdriver.Migration, error) {
	const query = `
		SELECT created_at, line, version
		FROM /* TEMPLATE: schema */river_migration
		WHERE line = @line`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query, sql.Named("line", params.Line))
}

func (e *Executor) MigrationInsertMany(ctx context.Context, params *riverdriver.MigrationInsertManyParams) ([]*riverdriver.Migration, error) {
	versions, err := json.Marshal(params.Versions)
	if err != nil {
		return nil, err
	}

	const query = `
		INSERT INTO /* TEMPLATE: schema */river_migration (
			line,
			version
		)
		OUTPUT inserted.created_at, inserted.line, inserted.version
		SELECT @line, CAST(value AS bigint)
		FROM OPENJSON(@versions)`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("line", params.Line),
		sql.Named("versions", string(versions)),
	)
}

func (e *Executor) MigrationInsertManyAssumingMain(ctx context.Context, params *riverdriver.MigrationInsertManyAssumingMainParams) ([]*riverdriver.Migration, error) {
	versions, err := json.Marshal(params.Versions)
	if err != nil {
		return nil, err
	}

	const query = `
		INSERT INTO /* TEMPLATE: schema */river_migration (
			version
		)
		OUTPUT inserted.created_at, N'` + riverdriver.MigrationLineMain + `', inserted.version
		SELECT CAST(value AS bigint)
		FROM OPENJSON(@versions)`

	return e.queryMigrations(schemaTemplateParam(ctx, params.Schema), query, sql.Named("versions", string(versions)))
}

func (e *Executor) NotificationDeleteBefore(ctx context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
	const query = `
		DELETE FROM /* TEMPLATE: schema */river_notification
		WHERE id IN (
			SELECT TOP (@max) id
			FROM /* TEMPLATE: schema */river_notification
			WHERE created_at < CAST(@created_at_horizon AS datetime2(6))
			ORDER BY created_at, id
		)`

	return e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("created_at_horizon", params.CreatedAtHorizon.UTC()),
		sql.Named("max", int64(params.Max)),
	)
}

// NotifyMany does nothing because SQL Server has no LISTEN/NOTIFY equivalent,
// and SQL Server clients run in poll only mode.
func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	return nil
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) Ping(ctx context.Context) error {
	return e.Exec(ctx, "SELECT 1")
}

func (e *Executor) QueryRow(ctx context.Context, query string, args ...any) riverdriver.Row {
	return e.dbtx.QueryRowContext(ctx, query, args...)
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	const query = `
		MERGE INTO /* TEMPLATE: schema */river_queue WITH (HOLDLOCK) AS river_queue
		USING (SELECT @name AS name) AS queue_input
		ON river_queue.name = queue_input.name
		WHEN MATCHED THEN
			UPDATE SET updated_at = coalesce(CAST(@updated_at AS datetime2(6)), ` + nowSQL + `)
		WHEN NOT MATCHED THEN
			INSERT (
				created_at,
				metadata,
				name,
				paused_at,
				updated_at
			) VALUES (
				` + nowSQL + `,
				@metadata,
				@name,
				CAST(@paused_at AS datetime2(6)),
				coalesce(CAST(@updated_at AS datetime2(6)), ` + nowSQL + `)
			)
		OUTPUT ` + queueColumnsInserted + `;`

	return queryQueue(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("metadata", string(sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")))),
		sql.Named("name", params.Name),
		sql.Named("now", nullableTime(params.Now)),
		sql.Named("paused_at", nullableTime(params.PausedAt)),
		sql.Named("updated_at", nullableTime(params.UpdatedAt)),
	)
}

func (e *Executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	const query = `
		DELETE FROM /* TEMPLATE: schema */river_queue
		OUTPUT deleted.name
		WHERE name IN (
			SELECT TOP (@max) name
			FROM /* TEMPLATE: schema */river_queue
			WHERE updated_at < CAST(@updated_at_horizon AS datetime2(6))
			ORDER BY name ASC
		)`

	queueNames, err := e.queryStrings(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("max", int64(params.Max)),
		sql.Named("updated_at_horizon", params.UpdatedAtHorizon.UTC()),
	)
	if err != nil {
		return nil, err
	}
	// The order of rows output by a delete isn't guaranteed.
	slices.Sort(queueNames)
	return queueNames, nil
}

func (e *Executor) QueueGet(ctx context.Context, params *riverdriver.QueueGetParams) (*rivertype.Queue, error) {
	const query = `
		SELECT ` + queueColumns + `
		FROM /* TEMPLATE: schema */river_queue
		WHERE name = @name`

	return queryQueue(schemaTemplateParam(ctx, params.Schema), e.dbtx, query, sql.Named("name", params.Name))
}

func (e *Executor) QueueList(ctx context.Context, params *riverdriver.QueueListParams) ([]*rivertype.Queue, error) {
	const query = `
		SELECT TOP (@max) ` + queueColumns + `
		FROM /* TEMPLATE: schema */river_queue
		ORDER BY name ASC`

	rows, err := e.dbtx.QueryContext(schemaTemplateParam(ctx, params.Schema), query, sql.Named("max", int64(params.Max)))
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var queues []*rivertype.Queue
	for rows.Next() {
		queue, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, interpretError(rows.Err())
}

func (e *Executor) QueueNameList(ctx context.Context, params *riverdriver.QueueNameListParams) ([]string, error) {
	exclude, err := json.Marshal(emptyIfNil(params.Exclude))
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT TOP (@max) name
		FROM /* TEMPLATE: schema */river_queue
		WHERE name > @after
			AND (@match = N'' OR LOWER(name) LIKE N'%' + LOWER(@match) + N'%')
			AND name NOT IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@exclude))
		ORDER BY name ASC`

	return e.queryStrings(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("after", params.After),
		sql.Named("exclude", string(exclude)),
		sql.Named("match", params.Match),
		sql.Named("max", int64(min(params.Max, math.MaxInt32))),
	)
}

func (e *Executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	const query = `
		UPDATE /* TEMPLATE: schema */river_queue
		SET
			paused_at = CASE WHEN paused_at IS NULL THEN ` + nowSQL + ` ELSE paused_at END,
			updated_at = CASE WHEN paused_at IS NULL THEN ` + nowSQL + ` ELSE updated_at END
		WHERE @name = N'` + riverdriver.AllQueuesString + `' OR name = @name`

	rowsAffected, err := e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("name", params.Name),
		sql.Named("now", nullableTime(params.Now)),
	)
	if err != nil {
		return err
	}
	if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueResume(ctx context.Context, params *riverdriver.QueueResumeParams) error {
	const query = `
		UPDATE /* TEMPLATE: schema */river_queue
		SET
			paused_at = NULL,
			updated_at = CASE WHEN paused_at IS NOT NULL THEN ` + nowSQL + ` ELSE updated_at END
		WHERE @name = N'` + riverdriver.AllQueuesString + `' OR name = @name`

	rowsAffected, err := e.execRowsAffected(schemaTemplateParam(ctx, params.Schema), query,
		sql.Named("name", params.Name),
		sql.Named("now", nullableTime(params.Now)),
	)
	if err != nil {
		return err
	}
	if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueUpdate(ctx context.Context, params *riverdriver.QueueUpdateParams) (*rivertype.Queue, error) {
	const query = `
		UPDATE /* TEMPLATE: schema */river_queue
		SET
			metadata = CASE WHEN @metadata_do_update = 1 THEN @metadata ELSE metadata END,
			updated_at = sysutcdatetime()
		OUTPUT ` + queueColumnsInserted + `
		WHERE name = @name`

	return queryQueue(schemaTemplateParam(ctx, params.Schema), e.dbtx, query,
		sql.Named("metadata", string(sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")))),
		sql.Named("metadata_do_update", params.MetadataDoUpdate),
		sql.Named("name", params.Name),
	)
}

func (e *Executor) SchemaCreate(ctx context.Context, params *riverdriver.SchemaCreateParams) error {
	// `CREATE SCHEMA` must be alone in its batch, so it's run as dynamic SQL.
	const query = `
		DECLARE @statement nvarchar(max) = N'CREATE SCHEMA ' + QUOTENAME(@schema);
		EXEC sp_executesql @statement;`

	_, err := e.dbtx.ExecContext(ctx, query, sql.Named("schema", params.Schema))
	return interpretError(err)
}

func (e *Executor) SchemaDrop(ctx context.Context, params *riverdriver.SchemaDropParams) error {
	// SQL Server has no equivalent of `DROP SCHEMA ... CASCADE`, so the
	// objects in the schema (River's tables, which may reference each other,
	// and its job ID sequence) are dropped before the schema itself.
	const query = `
		DECLARE @statement nvarchar(max);

		SELECT @statement = STRING_AGG(CAST(N'ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + N' DROP CONSTRAINT ' + QUOTENAME(name) AS nvarchar(max)), N'; ')
		FROM sys.foreign_keys
		WHERE schema_id = SCHEMA_ID(@schema);
		IF @statement IS NOT NULL EXEC sp_executesql @statement;

		SELECT @statement = STRING_AGG(CAST(N'DROP TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(name) AS nvarchar(max)), N'; ')
		FROM sys.tables
		WHERE schema_id = SCHEMA_ID(@schema);
		IF @statement IS NOT NULL EXEC sp_executesql @statement;

		SELECT @statement = STRING_AGG(CAST(N'DROP SEQUENCE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(name) AS nvarchar(max)), N'; ')
		FROM sys.sequences
		WHERE schema_id = SCHEMA_ID(@schema);
		IF @statement IS NOT NULL EXEC sp_executesql @statement;

		IF SCHEMA_ID(@schema) IS NOT NULL
		BEGIN
			SET @statement = N'DROP SCHEMA ' + QUOTENAME(@schema);
			EXEC sp_executesql @statement;
		END;`

	_, err := e.dbtx.ExecContext(ctx, query, sql.Named("schema", params.Schema))
	return interpretError(err)
}

func (e *Executor) SchemaGetExpired(ctx context.Context, params *riverdriver.SchemaGetExpiredParams) ([]string, error) {
	// Compares prefixes with LEFT rather than LIKE because test schema
	// prefixes contain underscores, which are LIKE wildcards.
	const query = `
		SELECT name
		FROM sys.schemas
		WHERE LEFT(name, LEN(@prefix)) = @prefix
			AND name < @before_name
		ORDER BY name`

	return e.queryStrings(ctx, query,
		sql.Named("before_name", params.BeforeName),
		sql.Named("prefix", params.Prefix),
	)
}

func (e *Executor) TableExists(ctx context.Context, params *riverdriver.TableExistsParams) (bool, error) {
	const query = `
		SELECT CAST(CASE WHEN EXISTS (
			SELECT 1
			FROM sys.tables
			WHERE schema_id = SCHEMA_ID(` + schemaNameSQL + `)
				AND name = @table
		) THEN 1 ELSE 0 END AS bit)`

	var exists bool
	err := e.dbtx.QueryRowContext(ctx, query,
		sql.Named("schema", params.Schema),
		sql.Named("table", params.Table),
	).Scan(&exists)
	return exists, interpretError(err)
}

func (e *Executor) TableTruncate(ctx context.Context, params *riverdriver.TableTruncateParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = quoteIdentifier(params.Schema) + "."
	}

	// `DELETE` rather than `TRUNCATE` because a table referenced by a foreign
	// key (like `river_client` from `river_client_queue`) can't be truncated.
	for _, table := range params.Table {
		if _, err := e.dbtx.ExecContext(ctx, "DELETE FROM "+maybeSchema+quoteIdentifier(table)); err != nil {
			return interpretError(err)
		}
	}

	return nil
}

// execRowsAffected runs a statement and returns the number of rows it affected.
func (e *Executor) execRowsAffected(ctx context.Context, query string, args ...any) (int, error) {
	res, err := e.dbtx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, interpretError(err)
	}
	rowsAffected, err := res.RowsAffected()
	return int(rowsAffected), interpretError(err)
}

// executor preserves transaction identity when a method is invoked through an
// embedded Executor, so transaction helpers can reuse it without a savepoint.
func (e *Executor) executor() riverdriver.Executor {
	if e.execTx != nil {
		return e.execTx
	}
	return e
}

// jobGetByIDMany gets jobs by ID, ordered by ID.
func (e *Executor) jobGetByIDMany(ctx context.Context, schema string, ids []int64) ([]*jobRow, error) {
	idsParam, err := jsonArrayOrNull(ids)
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT ` + jobColumns + `
		FROM /* TEMPLATE: schema */river_job
		WHERE id IN (SELECT CAST(value AS bigint) FROM OPENJSON(@id))
		ORDER BY id`

	return queryJobs(schemaTemplateParam(ctx, schema), e.dbtx, query, sql.Named("id", idsParam))
}

// queryMigrations runs a query returning rows of created_at, line, and
// version, ordering the migrations by version.
func (e *Executor) queryMigrations(ctx context.Context, query string, args ...any) ([]*riverdriver.Migration, error) {
	rows, err := e.dbtx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var migrations []*riverdriver.Migration
	for rows.Next() {
		var migration riverdriver.Migration
		if err := rows.Scan(&migration.CreatedAt, &migration.Line, &migration.Version); err != nil {
			return nil, err
		}
		migration.CreatedAt = migration.CreatedAt.UTC()
		migrations = append(migrations, &migration)
	}
	if err := rows.Err(); err != nil {
		return nil, interpretError(err)
	}

	slices.SortFunc(migrations, func(m1, m2 *riverdriver.Migration) int { return cmp.Compare(m1.Version, m2.Version) })
	return migrations, nil
}

// queryStrings runs a query returning rows of a single string column.
func (e *Executor) queryStrings(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := e.dbtx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, interpretError(rows.Err())
}

// txDBTX returns a query runner for a transaction started by dbutil.WithTxV.
func (e *Executor) txDBTX(execTx riverdriver.ExecutorTx) templateReplaceWrapper {
	return templateReplaceWrapper{e.driver.UnwrapTx(execTx), &e.driver.replacer}
}

// ExecutorTx is an Executor running in a transaction.
type ExecutorTx struct {
	Executor

	tx *sql.Tx
}

func (t *ExecutorTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	executorSubTx := &ExecutorSubTx{
		beginOnce:    &savepointutil.BeginOnlyOnce{},
		savepointNum: 0,
		tx:           t.tx,
	}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, t.dbtx.replacer}, t.driver, executorSubTx}
	return executorSubTx.Begin(ctx)
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Commit()
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Rollback()
}

// ExecutorSubTx is an Executor running in a savepoint of a transaction.
type ExecutorSubTx struct {
	Executor

	beginOnce    *savepointutil.BeginOnlyOnce
	savepointNum int
	tx           *sql.Tx
}

const savepointPrefix = "river_savepoint_"

func (t *ExecutorSubTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if err := t.beginOnce.Begin(); err != nil {
		return nil, err
	}

	nextSavepointNum := t.savepointNum + 1
	if err := t.Exec(ctx, fmt.Sprintf("SAVE TRANSACTION %s%02d", savepointPrefix, nextSavepointNum)); err != nil {
		return nil, err
	}

	executorSubTx := &ExecutorSubTx{
		beginOnce:    savepointutil.NewBeginOnlyOnce(t.beginOnce),
		savepointNum: nextSavepointNum,
		tx:           t.tx,
	}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, t.dbtx.replacer}, t.driver, executorSubTx}

	return executorSubTx, nil
}

// Commit keeps the savepoint's changes in its transaction. SQL Server has no
// equivalent of `RELEASE SAVEPOINT`, so there's nothing to run.
func (t *ExecutorSubTx) Commit(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	return nil
}

func (t *ExecutorSubTx) Rollback(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	return t.Exec(ctx, fmt.Sprintf("ROLLBACK TRANSACTION %s%02d", savepointPrefix, t.savepointNum))
}

// jobRow is a row of jobColumns, before its JSON columns are decoded.
type jobRow struct {
	ID           int64
	Args         []byte
	Attempt      int64
	AttemptedAt  *time.Time
	AttemptedBy  []byte
	CreatedAt    time.Time
	Errors       []byte
	FinalizedAt  *time.Time
	Kind         string
	MaxAttempts  int64
	Metadata     []byte
	Priority     int64
	Queue        string
	ScheduledAt  time.Time
	State        string
	Tags         []byte
	UniqueKey    []byte
	UniqueStates *int64
}

// scanDest returns the destinations to scan jobColumns into.
func (j *jobRow) scanDest() []any {
	return []any{
		&j.ID,
		&j.Args,
		&j.Attempt,
		&j.AttemptedAt,
		&j.AttemptedBy,
		&j.CreatedAt,
		&j.Errors,
		&j.FinalizedAt,
		&j.Kind,
		&j.MaxAttempts,
		&j.Metadata,
		&j.Priority,
		&j.Queue,
		&j.ScheduledAt,
		&j.State,
		&j.Tags,
		&j.UniqueKey,
		&j.UniqueStates,
	}
}

// queryer is the part of *sql.DB and *sql.Tx used to run queries.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type templateReplaceWrapper struct {
	dbtx     queryer
	replacer *sqlctemplate.Replacer
}

func (w templateReplaceWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	query, args = w.replacer.Run(ctx, argPlaceholder, query, args)
	return w.dbtx.ExecContext(ctx, query, args...)
}

func (w templateReplaceWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query, args = w.replacer.Run(ctx, argPlaceholder, query, args)
	return w.dbtx.QueryContext(ctx, query, args...)
}

func (w templateReplaceWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	query, args = w.replacer.Run(ctx, argPlaceholder, query, args)
	return w.dbtx.QueryRowContext(ctx, query, args...)
}

// Column lists in the order of jobRow.scanDest. `unique_key_enforced` is left
// out because it's derived from the other columns for the unique index.
const (
	jobColumns         = "id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states"
	jobColumnsDeleted  = "deleted.id, deleted.args, deleted.attempt, deleted.attempted_at, deleted.attempted_by, deleted.created_at, deleted.errors, deleted.finalized_at, deleted.kind, deleted.max_attempts, deleted.metadata, deleted.priority, deleted.queue, deleted.scheduled_at, deleted.state, deleted.tags, deleted.unique_key, deleted.unique_states"
	jobColumnsInserted = "inserted.id, inserted.args, inserted.attempt, inserted.attempted_at, inserted.attempted_by, inserted.created_at, inserted.errors, inserted.finalized_at, inserted.kind, inserted.max_attempts, inserted.metadata, inserted.priority, inserted.queue, inserted.scheduled_at, inserted.state, inserted.tags, inserted.unique_key, inserted.unique_states"

	leaderColumns         = "elected_at, expires_at, leader_id"
	leaderColumnsInserted = "inserted.elected_at, inserted.expires_at, inserted.leader_id"

	queueColumns         = "name, created_at, metadata, paused_at, updated_at"
	queueColumnsInserted = "inserted.name, inserted.created_at, inserted.metadata, inserted.paused_at, inserted.updated_at"
)

// Shared SQL fragments.
const (
	// indexTableLookupSQL looks up the table of the index named by `@index` in
	// the schema named by `@schema` (or the default schema), declaring
	// `@schema_name` and `@table_name`. `@table_name` is null if there's no
	// such index.
	indexTableLookupSQL = `
		DECLARE @schema_name sysname = ` + schemaNameSQL + `;
		DECLARE @table_name sysname = (
			SELECT TOP (1) tables.name
			FROM sys.indexes AS indexes
			INNER JOIN sys.tables AS tables ON tables.object_id = indexes.object_id
			WHERE tables.schema_id = SCHEMA_ID(@schema_name)
				AND indexes.name = @index
		);`

	// jobIDRangeSQL allocates `@id_range_size` job IDs from the sequence named
	// by `@sequence_name`, putting the first of them in `@id_range_first`. IDs
	// are allocated ahead of an insert (rather than through the `id` column's
	// default) so that every inserted row can be given an ID in input order,
	// and so that jobs with and without explicit IDs can be inserted together.
	jobIDRangeSQL = `
		DECLARE @id_range_first sql_variant;
		IF @id_range_size > 0
			EXEC sys.sp_sequence_get_range
				@sequence_name = @sequence_name,
				@range_size = @id_range_size,
				@range_first_value = @id_range_first OUTPUT;`

	jobInsertFastColumns = "id, args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states"
	jobInsertFastValues  = "job_input.id, job_input.args, job_input.created_at, job_input.kind, job_input.max_attempts, job_input.metadata, job_input.priority, job_input.queue, job_input.scheduled_at, job_input.state, job_input.tags, job_input.unique_key, job_input.unique_states"

	// jobInsertFastSourceSQL selects the jobs encoded by jobInsertFastManyParam
	// in `@jobs`, along with the value each would have in
	// `river_job.unique_key_enforced` if its unique key is enforced in its
	// state (and null otherwise).
	jobInsertFastSourceSQL = `
		SELECT
			job_input.ordinal,
			coalesce(job_input.id, CAST(@id_range_first AS bigint) + job_input.id_offset) AS id,
			job_input.args,
			coalesce(job_input.created_at, sysutcdatetime()) AS created_at,
			job_input.kind,
			job_input.max_attempts,
			job_input.metadata,
			job_input.priority,
			job_input.queue,
			coalesce(job_input.scheduled_at, sysutcdatetime()) AS scheduled_at,
			job_input.state,
			job_input.tags,
			job_input.unique_key,
			job_input.unique_states,
			CASE
				WHEN job_input.unique_key IS NOT NULL
					AND job_input.unique_states IS NOT NULL
					AND (job_input.unique_states & CASE job_input.state
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
				THEN 0x01 + job_input.unique_key
			END AS unique_key_enforced
		FROM (
			SELECT
				ordinal,
				id,
				id_offset,
				args,
				created_at,
				kind,
				max_attempts,
				metadata,
				priority,
				queue,
				scheduled_at,
				state,
				tags,
				CONVERT(varbinary(255), unique_key, 2) AS unique_key,
				unique_states
			FROM OPENJSON(@jobs) WITH (
				ordinal int '$.ordinal',
				id bigint '$.id',
				id_offset bigint '$.id_offset',
				args nvarchar(max) '$.args' AS JSON,
				created_at datetime2(6) '$.created_at',
				kind nvarchar(128) '$.kind',
				max_attempts smallint '$.max_attempts',
				metadata nvarchar(max) '$.metadata' AS JSON,
				priority smallint '$.priority',
				queue nvarchar(128) '$.queue',
				scheduled_at datetime2(6) '$.scheduled_at',
				state nvarchar(16) '$.state',
				tags nvarchar(max) '$.tags' AS JSON,
				unique_key varchar(510) '$.unique_key',
				unique_states tinyint '$.unique_states'
			)
		) AS job_input`

	// metadataMergeSQL merges the JSON object `job_input.metadata_updates` into
	// the JSON object `river_job.metadata`, like Postgres' `||` operator for
	// jsonb objects: keys in the updates replace those of the same name.
	// OPENJSON unescapes string values (type 1), so they're escaped again.
	metadataMergeSQL = `coalesce((
		SELECT N'{' + STRING_AGG(CAST(N'"' + STRING_ESCAPE(merged.[key], 'json') + N'":' + merged.value AS nvarchar(max)), N',') + N'}'
		FROM (
			SELECT
				[key],
				CASE type WHEN 0 THEN N'null' WHEN 1 THEN N'"' + STRING_ESCAPE(value, 'json') + N'"' ELSE value END AS value
			FROM OPENJSON(river_job.metadata)
			WHERE [key] NOT IN (SELECT [key] FROM OPENJSON(job_input.metadata_updates))
			UNION ALL
			SELECT
				[key],
				CASE type WHEN 0 THEN N'null' WHEN 1 THEN N'"' + STRING_ESCAPE(value, 'json') + N'"' ELSE value END AS value
			FROM OPENJSON(job_input.metadata_updates)
		) AS merged
	), N'{}')`

	// nowSQL is the current time, which is the time in `@now` if it's set and
	// the database's time otherwise.
	nowSQL = "coalesce(CAST(@now AS datetime2(6)), sysutcdatetime())"

	// schemaNameSQL is the name of the schema in `@schema`, or the default
	// schema if it's empty.
	schemaNameSQL = "coalesce(nullif(@schema, N''), SCHEMA_NAME())"

	// ttlFromNowSQL is nowSQL plus the duration of ttlArgs.
	ttlFromNowSQL = "DATEADD(microsecond, @ttl_microseconds, DATEADD(second, @ttl_seconds, " + nowSQL + "))"
)

// attemptErrorsJSON encodes attempt errors as a JSON array, or returns nil if
// there are none.
func attemptErrorsJSON(attemptErrors [][]byte) (json.RawMessage, error) {
	if len(attemptErrors) < 1 {
		return nil, nil
	}
	return json.Marshal(sliceutil.Map(attemptErrors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
}

// emptyIfNil returns an empty slice for a nil one so that it encodes to a JSON
// array rather than null.
func emptyIfNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func interpretError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

// jobGetAvailableResultFromInternal decodes the job rows locked by
// JobGetAvailable, retaining every row and recording decode errors by job ID
// because they've all been moved to `running`.
func jobGetAvailableResultFromInternal(jobs []*jobRow) *riverdriver.JobGetAvailableResult {
	res := &riverdriver.JobGetAvailableResult{Jobs: make([]*rivertype.JobRow, len(jobs))}
	for i, internal := range jobs {
		job, err := jobRowFromInternalPartial(internal)
		res.Jobs[i] = job
		if err != nil {
			if res.DecodeErrors == nil {
				res.DecodeErrors = make(map[int64]error)
			}
			res.DecodeErrors[job.ID] = err
		}
	}
	return res
}

// jobIDSequenceName returns the name of the job ID sequence in schema (or the
// default schema), quoted for sp_sequence_get_range.
func jobIDSequenceName(schema string) string {
	if schema == "" {
		return quoteIdentifier("river_job_id_seq")
	}
	return quoteIdentifier(schema) + "." + quoteIdentifier("river_job_id_seq")
}

// jobInsertFastManyParam encodes jobs as JSON for jobInsertFastSourceSQL,
// returning it along with the number of job IDs that need to be allocated for
// jobs without one.
func jobInsertFastManyParam(jobs []*riverdriver.JobInsertFastParams) (string, int64, error) {
	type jobInput struct {
		Args         json.RawMessage `json:"args"`
		CreatedAt    *string         `json:"created_at"`
		ID           *int64          `json:"id"`
		IDOffset     int64           `json:"id_offset"`
		Kind         string          `json:"kind"`
		MaxAttempts  int             `json:"max_attempts"`
		Metadata     json.RawMessage `json:"metadata"`
		Ordinal      int             `json:"ordinal"`
		Priority     int             `json:"priority"`
		Queue        string          `json:"queue"`
		ScheduledAt  *string         `json:"scheduled_at"`
		State        string          `json:"state"`
		Tags         []string        `json:"tags"`
		UniqueKey    *string         `json:"unique_key"`
		UniqueStates *int            `json:"unique_states"`
	}

	var numIDsToAllocate int64
	jobInputs := make([]jobInput, len(jobs))
	for i, job := range jobs {
		jobInputs[i] = jobInput{
			Args:         job.EncodedArgs,
			CreatedAt:    nullableTimeString(job.CreatedAt),
			ID:           job.ID,
			Kind:         job.Kind,
			MaxAttempts:  job.MaxAttempts,
			Metadata:     sliceutil.FirstNonEmpty(job.Metadata, []byte("{}")),
			Ordinal:      i,
			Priority:     job.Priority,
			Queue:        job.Queue,
			ScheduledAt:  nullableTimeString(job.ScheduledAt),
			State:        string(job.State),
			Tags:         emptyIfNil(job.Tags),
			UniqueKey:    nullableHexString(job.UniqueKey),
			UniqueStates: nullableUniqueStates(job.UniqueStates),
		}
		if job.ID == nil {
			jobInputs[i].IDOffset = numIDsToAllocate
			numIDsToAllocate++
		}
	}

	data, err := json.Marshal(jobInputs)
	if err != nil {
		return "", 0, err
	}
	return string(data), numIDsToAllocate, nil
}

// jobListContext adds the clauses of a job list (or delete) to ctx for the
// job list query templates, adapting them to T-SQL.
func jobListContext(ctx context.Context, params *riverdriver.JobListParams) context.Context {
	// Times are bound in UTC like all others (see nullableTime). Keep the
	// caller's arguments intact for repeated or concurrent use.
	namedArgs := maps.Clone(params.NamedArgs)
	for name, arg := range namedArgs {
		switch arg := arg.(type) {
		case time.Time:
			namedArgs[name] = arg.UTC()
		case *time.Time:
			namedArgs[name] = nullableTime(arg)
		}
	}

	// "true" is the predicate written for a list without any others, but T-SQL
	// has no boolean literals.
	whereClause := params.WhereClause
	if whereClause == "true" {
		whereClause = "1 = 1"
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"order_by_clause": {Value: orderByClause(params.OrderByClause)},
		"where_clause":    {Value: whereClause},
	}, namedArgs)
}

// jobRowFromInternal decodes a job row, returning an error if any of its
// fields can't be decoded.
func jobRowFromInternal(internal *jobRow) (*rivertype.JobRow, error) {
	job, err := jobRowFromInternalPartial(internal)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// jobRowFromInternalPartial decodes a job row. A row is always returned, even
// along with an error, in which case the fields that couldn't be decoded are
// left empty. JSON columns can't hold invalid JSON, but can be changed out of
// band to JSON of the wrong shape.
func jobRowFromInternalPartial(internal *jobRow) (*rivertype.JobRow, error) {
	var decodeErrs []error

	var attemptedAt *time.Time
	if internal.AttemptedAt != nil {
		attemptedAt = new(internal.AttemptedAt.UTC())
	}

	var attemptedBy []string
	if internal.AttemptedBy != nil {
		if err := json.Unmarshal(internal.AttemptedBy, &attemptedBy); err != nil {
			decodeErrs = append(decodeErrs, fmt.Errorf("error unmarshaling `attempted_by`: %w", err))
			attemptedBy = nil
		}
	}

	attemptErrors := make([]rivertype.AttemptError, 0)
	if internal.Errors != nil {
		if err := riverdriver.UnmarshalAttemptErrors(internal.Errors, &attemptErrors); err != nil {
			decodeErrs = append(decodeErrs, fmt.Errorf("error unmarshaling `errors`: %w", err))
			attemptErrors = nil
		}
	}

	var finalizedAt *time.Time
	if internal.FinalizedAt != nil {
		finalizedAt = new(internal.FinalizedAt.UTC())
	}

	var tags []string
	if err := json.Unmarshal(internal.Tags, &tags); err != nil {
		decodeErrs = append(decodeErrs, fmt.Errorf("error unmarshaling `tags`: %w", err))
		tags = nil
	}

	var uniqueStatesByte byte
	if internal.UniqueStates != nil {
		if *internal.UniqueStates < 0 || *internal.UniqueStates > math.MaxUint8 {
			decodeErrs = append(decodeErrs, fmt.Errorf("value out of range for byte: %d", *internal.UniqueStates))
		} else {
			uniqueStatesByte = byte(*internal.UniqueStates)
		}
	}

	return &rivertype.JobRow{
		ID:           internal.ID,
		Attempt:      max(int(internal.Attempt), 0),
		AttemptedAt:  attemptedAt,
		AttemptedBy:  attemptedBy,
		CreatedAt:    internal.CreatedAt.UTC(),
		EncodedArgs:  internal.Args,
		Errors:       attemptErrors,
		FinalizedAt:  finalizedAt,
		Kind:         internal.Kind,
		MaxAttempts:  max(int(internal.MaxAttempts), 0),
		Metadata:     internal.Metadata,
		Priority:     max(int(internal.Priority), 0),
		Queue:        internal.Queue,
		ScheduledAt:  internal.ScheduledAt.UTC(),
		State:        rivertype.JobState(internal.State),
		Tags:         tags,
		UniqueKey:    internal.UniqueKey,
		UniqueStates: uniquestates.UniqueBitmaskToStates(uniqueStatesByte),
	}, errors.Join(decodeErrs...)
}

// jobRowsFromInternalPartial decodes job rows with jobRowFromInternalPartial,
// ignoring decode errors so that one bad row doesn't prevent returning the
// others.
func jobRowsFromInternalPartial(jobs []*jobRow) []*rivertype.JobRow {
	return sliceutil.Map(jobs, func(internal *jobRow) *rivertype.JobRow {
		job, _ := jobRowFromInternalPartial(internal)
		return job
	})
}

// jsonArrayOrNull encodes values as a JSON array, or returns nil (SQL NULL) for
// a nil slice so that a query can tell an absent filter apart from an empty one.
func jsonArrayOrNull[T any](values []T) (any, error) {
	if values == nil {
		return nil, nil //nolint:nilnil
	}

	data, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

// nullableHexString hex encodes data for `CONVERT(varbinary, ..., 2)`, or
// returns nil for empty data.
func nullableHexString(data []byte) *string {
	if len(data) < 1 {
		return nil
	}
	return new(hex.EncodeToString(data))
}

// nullableTime returns a time in UTC, or nil (SQL NULL) for a nil time.
//
// Times are bound in UTC because go-mssqldb sends them as `datetimeoffset`, and
// casting a `datetimeoffset` to `datetime2` (the type of River's columns, which
// hold UTC) drops its offset rather than converting to UTC.
func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// nullableTimeString formats a time with timeString, or returns nil for a nil
// time.
func nullableTimeString(t *time.Time) *string {
	if t == nil {
		return nil
	}
	return new(timeString(*t))
}

// nullableUniqueStates returns nil for empty unique states, which are stored
// as null.
func nullableUniqueStates(uniqueStates byte) *int {
	if uniqueStates == 0 {
		return nil
	}
	return new(int(uniqueStates))
}

var orderByNullsRE = regexp.MustCompile(`(\S+) (ASC|DESC) NULLS (FIRST|LAST)`)

// orderByClause rewrites `NULLS FIRST` and `NULLS LAST` in an ORDER BY clause,
// which T-SQL doesn't support, as a sort on whether the expression is null
// ahead of the sort on the expression itself.
func orderByClause(clause string) string {
	return orderByNullsRE.ReplaceAllStringFunc(clause, func(match string) string {
		submatches := orderByNullsRE.FindStringSubmatch(match)
		expr, direction, nulls := submatches[1], submatches[2], submatches[3]

		nullRank, notNullRank := "0", "1"
		if nulls == "LAST" {
			nullRank, notNullRank = "1", "0"
		}
		return "CASE WHEN " + expr + " IS NULL THEN " + nullRank + " ELSE " + notNullRank + " END, " + expr + " " + direction
	})
}

func queryJob(ctx context.Context, dbtx templateReplaceWrapper, query string, args ...any) (*jobRow, error) {
	var job jobRow
	if err := dbtx.QueryRowContext(ctx, query, args...).Scan(job.scanDest()...); err != nil {
		return nil, interpretError(err)
	}
	return &job, nil
}

func queryJobs(ctx context.Context, dbtx templateReplaceWrapper, query string, args ...any) ([]*jobRow, error) {
	rows, err := dbtx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, interpretError(err)
	}
	defer rows.Close()

	var jobs []*jobRow
	for rows.Next() {
		var job jobRow
		if err := rows.Scan(job.scanDest()...); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, interpretError(rows.Err())
}

func queryLeader(ctx context.Context, dbtx templateReplaceWrapper, query string, args ...any) (*riverdriver.Leader, error) {
	var leader riverdriver.Leader
	if err := dbtx.QueryRowContext(ctx, query, args...).Scan(&leader.ElectedAt, &leader.ExpiresAt, &leader.LeaderID); err != nil {
		return nil, interpretError(err)
	}
	leader.ElectedAt = leader.ElectedAt.UTC()
	leader.ExpiresAt = leader.ExpiresAt.UTC()
	return &leader, nil
}

func queryQueue(ctx context.Context, dbtx templateReplaceWrapper, query string, args ...any) (*rivertype.Queue, error) {
	queue, err := scanQueue(dbtx.QueryRowContext(ctx, query, args...))
	return queue, interpretError(err)
}

// quoteIdentifier quotes a T-SQL identifier like QUOTENAME.
func quoteIdentifier(ident string) string {
	return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
}

// scanQueue scans a row of queueColumns.
func scanQueue(row riverdriver.Row) (*rivertype.Queue, error) {
	var queue rivertype.Queue
	if err := row.Scan(&queue.Name, &queue.CreatedAt, &queue.Metadata, &queue.PausedAt, &queue.UpdatedAt); err != nil {
		return nil, err
	}

	queue.CreatedAt = queue.CreatedAt.UTC()
	if queue.PausedAt != nil {
		queue.PausedAt = new(queue.PausedAt.UTC())
	}
	queue.UpdatedAt = queue.UpdatedAt.UTC()
	return &queue, nil
}

func schemaTemplateParam(ctx context.Context, schema string) context.Context {
	if schema != "" {
		schema = quoteIdentifier(schema) + "."
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema, Stable: true},
	}, nil)
}

// timeString formats a time for a JSON parameter that OPENJSON decodes as a
// `datetime2(6)`, which takes ISO 8601 without a time zone.
func timeString(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000")
}

// ttlArgs returns the arguments for ttlFromNowSQL. The TTL is split into whole
// seconds and microseconds because DATEADD only takes an int.
func ttlArgs(ttl time.Duration) []any {
	return []any{
		sql.Named("ttl_microseconds", int64(ttl%time.Second/time.Microsecond)),
		sql.Named("ttl_seconds", int64(ttl/time.Second)),
	}
}

// uniqueKeyEnforced returns true if a job's unique key is enforced in the state
// it's being inserted in, in which case it conflicts with any other job with
// the same key in one of its unique states.
func uniqueKeyEnforced(job *riverdriver.JobInsertFastParams) bool {
	return len(job.UniqueKey) > 0 && job.UniqueStates&uniquestates.UniqueStatesToBitmask([]rivertype.JobState{job.State}) != 0
}
