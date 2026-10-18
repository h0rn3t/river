package riversqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/rivershared/sqlctemplate"
	"github.com/h0rn3t/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestDriverSQLFragmentColumnIn(t *testing.T) {
	t.Parallel()

	driver := New(nil)

	t.Run("IntegerValues", func(t *testing.T) {
		t.Parallel()

		sqlFragment, arg, err := driver.SQLFragmentColumnIn("id", []int64{1, 2})
		require.NoError(t, err)
		require.Equal(t, "id IN (SELECT CAST(value AS bigint) FROM OPENJSON(@id))", sqlFragment)
		require.Equal(t, "[1,2]", arg)
	})

	t.Run("StringValues", func(t *testing.T) {
		t.Parallel()

		sqlFragment, arg, err := driver.SQLFragmentColumnIn("kind", []string{"kind1", "kind2"})
		require.NoError(t, err)
		require.Equal(t, "kind IN (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kind))", sqlFragment)
		require.Equal(t, `["kind1","kind2"]`, arg)
	})
}

func TestExecutorJobInsertFastMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("DuplicateUniqueKeyInBatch", func(t *testing.T) {
		t.Parallel()

		job := &riverdriver.JobInsertFastParams{
			Kind:         "kind",
			State:        rivertype.JobStateAvailable,
			UniqueKey:    []byte("unique-key"),
			UniqueStates: 0xff,
		}

		// Rejected before reaching the database, so no pool is needed.
		_, err := New(nil).GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{job, job},
		})
		require.EqualError(t, err, "unique key appears more than once in batch")
	})

	t.Run("NoJobs", func(t *testing.T) {
		t.Parallel()

		results, err := New(nil).GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{})
		require.NoError(t, err)
		require.Empty(t, results)
	})
}

func TestExecutorJobInsertFastManyNoReturning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("NoJobs", func(t *testing.T) {
		t.Parallel()

		numInserted, err := New(nil).GetExecutor().JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{})
		require.NoError(t, err)
		require.Zero(t, numInserted)
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

func TestJobIDSequenceName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "[river_job_id_seq]", jobIDSequenceName(""))
	require.Equal(t, "[custom_schema].[river_job_id_seq]", jobIDSequenceName("custom_schema"))
	require.Equal(t, "[schema]]name].[river_job_id_seq]", jobIDSequenceName("schema]name"))
}

func TestJobInsertFastManyParam(t *testing.T) {
	t.Parallel()

	scheduledAt := time.Date(2026, 10, 15, 12, 34, 56, 123456789, time.FixedZone("UTC+2", 2*60*60))

	jobsParam, numIDsToAllocate, err := jobInsertFastManyParam([]*riverdriver.JobInsertFastParams{
		{EncodedArgs: []byte(`{"a":1}`), Kind: "kind", State: rivertype.JobStateAvailable},
		{EncodedArgs: []byte(`{}`), ID: new(int64(123)), Kind: "kind", ScheduledAt: &scheduledAt, State: rivertype.JobStateScheduled, Tags: []string{"tag"}},
		{EncodedArgs: []byte(`{}`), Kind: "kind", State: rivertype.JobStateAvailable, UniqueKey: []byte{0x00, 0xff}, UniqueStates: 0x01},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), numIDsToAllocate)

	var jobs []map[string]any
	require.NoError(t, json.Unmarshal([]byte(jobsParam), &jobs))
	require.Len(t, jobs, 3)

	// ID offsets are only assigned to jobs without an ID, in input order.
	require.Nil(t, jobs[0]["id"])
	require.InDelta(t, 0, jobs[0]["id_offset"], 0)
	require.InDelta(t, 123, jobs[1]["id"], 0)
	require.Nil(t, jobs[2]["id"])
	require.InDelta(t, 1, jobs[2]["id_offset"], 0)

	for i, job := range jobs {
		require.InDelta(t, i, job["ordinal"], 0)
	}

	// Raw JSON is embedded rather than encoded as a string, and missing
	// metadata and tags default to an empty object and array.
	require.Equal(t, map[string]any{"a": float64(1)}, jobs[0]["args"])
	require.Equal(t, map[string]any{}, jobs[0]["metadata"])
	require.Equal(t, []any{}, jobs[0]["tags"])
	require.Equal(t, []any{"tag"}, jobs[1]["tags"])

	// Times are formatted in UTC without a time zone.
	require.Nil(t, jobs[0]["scheduled_at"])
	require.Equal(t, "2026-10-15T10:34:56.123456", jobs[1]["scheduled_at"])

	// Empty unique keys and states are null, and keys are hex encoded.
	require.Nil(t, jobs[0]["unique_key"])
	require.Nil(t, jobs[0]["unique_states"])
	require.Equal(t, "00ff", jobs[2]["unique_key"])
	require.InDelta(t, 1, jobs[2]["unique_states"], 0)
}

func TestJobInsertMergeHint(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		jobs []*riverdriver.JobInsertFastParams
		want string
	}{
		{"NoJobs", nil, ""},
		{"NoUniqueKeys", []*riverdriver.JobInsertFastParams{
			{Kind: "kind", State: rivertype.JobStateAvailable},
		}, ""},
		{"UniqueKeyNotEnforcedInState", []*riverdriver.JobInsertFastParams{
			{Kind: "kind", State: rivertype.JobStateScheduled, UniqueKey: []byte("unique-key"), UniqueStates: 0x01},
		}, ""},
		{"UniqueKeyEnforced", []*riverdriver.JobInsertFastParams{
			{Kind: "kind", State: rivertype.JobStateAvailable},
			{Kind: "kind", State: rivertype.JobStateAvailable, UniqueKey: []byte("unique-key"), UniqueStates: 0x01},
		}, " WITH (HOLDLOCK)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, jobInsertMergeHint(tt.jobs), "jobInsertMergeHint(%+v)", tt.jobs)
		})
	}
}

func TestJobListContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const query = "SELECT TOP (@p1) id FROM river_job WHERE /* TEMPLATE_BEGIN: where_clause */ 1 = 1 /* TEMPLATE_END */ ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */"

	t.Run("AlwaysTruePredicate", func(t *testing.T) {
		t.Parallel()

		sql, args := (&sqlctemplate.Replacer{}).Run(jobListContext(ctx, &riverdriver.JobListParams{
			OrderByClause: "id ASC",
			WhereClause:   "true",
		}), argPlaceholder, query, []any{int64(10)})
		require.Equal(t, "SELECT TOP (@p1) id FROM river_job WHERE 1 = 1 ORDER BY id ASC", sql)
		require.Equal(t, []any{int64(10)}, args)
	})

	t.Run("NamedArgsAndNullOrdering", func(t *testing.T) {
		t.Parallel()

		var (
			cursorTime = time.Date(2026, 10, 15, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*60*60))
			namedArgs  = map[string]any{"cursor_time": cursorTime, "kind": `["kind"]`}
		)

		sql, args := (&sqlctemplate.Replacer{}).Run(jobListContext(ctx, &riverdriver.JobListParams{
			NamedArgs:     namedArgs,
			OrderByClause: "finalized_at DESC NULLS FIRST, id DESC",
			WhereClause:   `kind IN (SELECT value FROM OPENJSON(@kind)) AND "finalized_at" < @cursor_time`,
		}), argPlaceholder, query, []any{int64(10)})
		require.Equal(t, `SELECT TOP (@p1) id FROM river_job WHERE kind IN (SELECT value FROM OPENJSON(@p3)) AND "finalized_at" < @p2 ORDER BY CASE WHEN finalized_at IS NULL THEN 0 ELSE 1 END, finalized_at DESC, id DESC`, sql)
		require.Equal(t, []any{int64(10), cursorTime.UTC(), `["kind"]`}, args)
		require.Equal(t, time.UTC, args[1].(time.Time).Location()) //nolint:forcetypeassert

		// The caller's arguments are left as they were.
		require.Equal(t, cursorTime, namedArgs["cursor_time"])
	})
}

func TestOrderByClause(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		clause string
		want   string
	}{
		{"IDOnly", "id ASC", "id ASC"},
		{"AscNullsFirst", "attempted_at ASC NULLS FIRST, id ASC", "CASE WHEN attempted_at IS NULL THEN 0 ELSE 1 END, attempted_at ASC, id ASC"},
		{"AscNullsLast", "finalized_at ASC NULLS LAST, id ASC", "CASE WHEN finalized_at IS NULL THEN 1 ELSE 0 END, finalized_at ASC, id ASC"},
		{"DescNullsFirst", "finalized_at DESC NULLS FIRST, id DESC", "CASE WHEN finalized_at IS NULL THEN 0 ELSE 1 END, finalized_at DESC, id DESC"},
		{"DescNullsLast", "attempted_at DESC NULLS LAST, id DESC", "CASE WHEN attempted_at IS NULL THEN 1 ELSE 0 END, attempted_at DESC, id DESC"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, orderByClause(tt.clause), "orderByClause(%q)", tt.clause)
		})
	}
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const query = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		sql, _ := (&sqlctemplate.Replacer{}).Run(schemaTemplateParam(ctx, ""), argPlaceholder, query, nil)
		require.Equal(t, "SELECT 1 FROM river_job", sql)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		sql, _ := (&sqlctemplate.Replacer{}).Run(schemaTemplateParam(ctx, "custom]schema"), argPlaceholder, query, nil)
		require.Equal(t, "SELECT 1 FROM [custom]]schema].river_job", sql)
	})
}

func TestTTLArgs(t *testing.T) {
	t.Parallel()

	require.Equal(t, []any{
		sql.Named("ttl_microseconds", int64(500_001)),
		sql.Named("ttl_seconds", int64(15)),
	}, ttlArgs(15*time.Second+500*time.Millisecond+time.Microsecond+999*time.Nanosecond))
}

func TestTimeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "2026-10-15T10:34:56.123456", timeString(time.Date(2026, 10, 15, 12, 34, 56, 123456789, time.FixedZone("UTC+2", 2*60*60))))
	require.Equal(t, "2026-10-15T12:34:56.000000", timeString(time.Date(2026, 10, 15, 12, 34, 56, 0, time.UTC)))
}

func TestTxDBTX(t *testing.T) {
	t.Parallel()

	t.Run("ExecutorSubTx", func(t *testing.T) {
		t.Parallel()

		subTx := &ExecutorSubTx{dbtx: templateReplaceWrapper{replacer: &sqlctemplate.Replacer{}}}
		require.Equal(t, subTx.dbtx, txDBTX(subTx))
	})

	t.Run("ExecutorTxFromNilDriver", func(t *testing.T) {
		t.Parallel()

		// rivertest unwraps transactions with a nil driver, which leaves no
		// driver to take a query runner from.
		var driver *Driver
		execTx, ok := driver.UnwrapExecutor(nil).(*ExecutorTx)
		require.True(t, ok)
		require.Equal(t, execTx.dbtx, txDBTX(execTx))
	})
}
