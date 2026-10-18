package riverdrivertest_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
	_ "turso.tech/database/tursogo"

	"github.com/h0rn3t/river"
	"github.com/h0rn3t/river/riverdbtest"
	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/riverdriver/riverdatabasesql"
	"github.com/h0rn3t/river/riverdriver/riverdrivertest"
	"github.com/h0rn3t/river/riverdriver/riverpgxv5"
	"github.com/h0rn3t/river/riverdriver/riversqlite"
	"github.com/h0rn3t/river/rivermigrate"
	"github.com/h0rn3t/river/rivershared/riversharedtest"
	"github.com/h0rn3t/river/rivershared/testfactory"
	"github.com/h0rn3t/river/rivershared/util/urlutil"
	"github.com/h0rn3t/river/rivertype"
)

func TestDriverRiverDatabaseSQLLibPQ(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(t, err)

	stdPool := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			tx, schema := riverdbtest.TestTx(ctx, t, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)

			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverDatabaseSQLPgxNoListener(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			tx, schema := riverdbtest.TestTx(ctx, t, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)

			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverDatabaseSQLPgxWithPgxListener(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.NewWithPgxListener(stdPool, dbPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			tx, schema := riverdbtest.TestTx(ctx, t, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)

			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverPgxV5(t *testing.T) {
	t.Parallel()

	// Default/primary pgx path with prepared statement caching.
	t.Run("DefaultMode", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeCacheStatement)
	})

	// PgBouncer transaction-pooling compatibility path (simple protocol).
	t.Run("SimpleProtocol", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeSimpleProtocol)
	})

	// Text-parameter execution path without prepared statement caching.
	t.Run("ExecMode", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeExec)
	})
}

func exerciseDriverRiverPgxV5WithMode(t *testing.T, mode pgx.QueryExecMode) {
	t.Helper()

	var (
		ctx    = context.Background()
		dbPool = dbPoolWithExecMode(ctx, t, mode)
		driver = riverpgxv5.New(dbPool)
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[pgx.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[pgx.Tx]) {
			t.Helper()

			tx, _ := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)
			return driver.UnwrapExecutor(tx), driver
		})
}

func dbPoolWithExecMode(ctx context.Context, t *testing.T, mode pgx.QueryExecMode) *pgxpool.Pool {
	t.Helper()

	config := riversharedtest.DBPool(ctx, t).Config()
	config.ConnConfig.DefaultQueryExecMode = mode

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(dbPool.Close)

	return dbPool
}

func TestDriverRiverLiteLibSQL(t *testing.T) { //nolint:dupl
	t.Parallel()

	var (
		ctx         = context.Background()
		procurePool = func(ctx context.Context, schema string) (any, string) {
			return riversharedtest.DBPoolLibSQL(ctx, t, schema), "" // could also be `main` instead of empty string
		}
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			if opts == nil {
				opts = &riverdbtest.TestSchemaOpts{}
			}
			opts.ProcurePool = procurePool

			var (
				// Driver will have its pool set by TestSchema.
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, opts)
			)
			return driver, schema
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			// Driver will have its pool set by TestSchema.
			driver := riversqlite.New(nil)

			tx, _ := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
				// Unfortunately, the normal test transaction schema sharing has
				// to be disabled for SQLite. When enabled, there's too much
				// contention on the shared test databases and operations fail
				// with `database is locked (5) (SQLITE_BUSY)`, which is a
				// common concurrency error in SQLite whose recommended
				// remediation is a backoff and retry. I tried various
				// techniques like journal_mode=WAL, but it didn't seem to help
				// enough. SQLite databases are just local files anyway, and
				// test transactions can still reuse schemas freed by other
				// tests through TestSchema, so this should be okay performance
				// wise.
				DisableSchemaSharing: true,

				ProcurePool: procurePool,
			})
			return driver.UnwrapExecutor(tx), driver
		})
}

// With multiple connections in WAL mode, a transaction that reads before
// writing can't wait for another writer through busy_timeout because its
// snapshot would be stale by the time it got the write lock. JobSchedule must
// take the write lock first so that it waits instead of failing with
// SQLITE_BUSY.
func TestDriverRiverSQLiteJobScheduleConcurrentWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dbPool, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "river.sqlite3")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbPool.Close()) })
	dbPool.SetMaxOpenConns(2)

	driver := riversqlite.New(dbPool)

	migrator, err := rivermigrate.New(driver, nil)
	require.NoError(t, err)
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	require.NoError(t, err)

	exec := driver.GetExecutor()

	job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: new(time.Now().Add(-time.Hour)), State: new(rivertype.JobStateScheduled)})

	// Another connection holds the write lock until it commits.
	otherTx, err := dbPool.BeginTx(ctx, nil)
	require.NoError(t, err)
	_ = testfactory.Job(ctx, t, driver.UnwrapExecutor(otherTx), &testfactory.JobOpts{})

	type scheduleResult struct {
		err     error
		results []*riverdriver.JobScheduleResult
	}
	resultChan := make(chan scheduleResult, 1)
	go func() {
		results, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{Max: 10})
		resultChan <- scheduleResult{err: err, results: results}
	}()

	// Give JobSchedule time to start before the other writer commits.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, otherTx.Commit())

	result := <-resultChan
	require.NoError(t, result.err)
	require.Len(t, result.results, 1)
	require.Equal(t, job.ID, result.results[0].Job.ID)
	require.Equal(t, rivertype.JobStateAvailable, result.results[0].Job.State)
}

func TestDriverRiverSQLiteModernC(t *testing.T) { //nolint:dupl
	t.Parallel()

	var (
		ctx         = context.Background()
		procurePool = func(ctx context.Context, schema string) (any, string) {
			return riversharedtest.DBPoolSQLite(ctx, t, schema), "" // could also be `main` instead of empty string
		}
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			if opts == nil {
				opts = &riverdbtest.TestSchemaOpts{}
			}
			opts.ProcurePool = procurePool

			var (
				// Driver will have its pool set by TestSchema.
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, opts)
			)
			return driver, schema
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			// Driver will have its pool set by TestSchema.
			driver := riversqlite.New(nil)

			tx, _ := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
				// Unfortunately, the normal test transaction schema sharing has
				// to be disabled for SQLite. When enabled, there's too much
				// contention on the shared test databases and operations fail
				// with `database is locked (5) (SQLITE_BUSY)`, which is a
				// common concurrency error in SQLite whose recommended
				// remediation is a backoff and retry. I tried various
				// techniques like journal_mode=WAL, but it didn't seem to help
				// enough. SQLite databases are just local files anyway, and
				// test transactions can still reuse schemas freed by other
				// tests through TestSchema, so this should be okay performance
				// wise.
				DisableSchemaSharing: true,

				ProcurePool: procurePool,
			})
			return driver.UnwrapExecutor(tx), driver
		})
}

// rivertest unwraps transactions with a nil driver, so an executor from a nil
// SQLite driver must not dereference its driver, including in operations that
// start their own (sub)transactions.
func TestDriverRiverSQLiteNilDriver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.ExecutorTx
	}

	setup := func(ctx context.Context, t *testing.T) *testBundle {
		t.Helper()

		tx, _ := riverdbtest.TestTx(ctx, t, riversqlite.New(nil), &riverdbtest.TestTxOpts{
			DisableSchemaSharing: true, // see TestDriverRiverSQLiteModernC
			ProcurePool: func(ctx context.Context, schema string) (any, string) {
				return riversharedtest.DBPoolSQLite(ctx, t, schema), ""
			},
		})

		var driver *riversqlite.Driver
		return &testBundle{exec: driver.UnwrapExecutor(tx)}
	}

	t.Run("Begin", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		subTx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)

		subSubTx, err := subTx.Begin(ctx)
		require.NoError(t, err)

		// An operation that starts its own savepoint from within a savepoint.
		job := testfactory.Job(ctx, t, subSubTx, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})
		jobAfter, err := subSubTx.JobCancel(ctx, &riverdriver.JobCancelParams{ID: job.ID, CancelAttemptedAt: time.Now(), ControlTopic: "control"})
		require.NoError(t, err)
		require.Equal(t, job.ID, jobAfter.ID)

		require.NoError(t, subSubTx.Commit(ctx))
		require.NoError(t, subTx.Rollback(ctx))
	})

	t.Run("JobCancel", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateAvailable)})

		jobAfter, err := bundle.exec.JobCancel(ctx, &riverdriver.JobCancelParams{ID: job.ID, CancelAttemptedAt: time.Now(), ControlTopic: "control"})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
	})

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateCompleted)})

		jobAfter, err := bundle.exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID})
		require.NoError(t, err)
		require.Equal(t, job.ID, jobAfter.ID)
	})

	t.Run("JobRescueMany", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		now := time.Now().UTC()
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})

		_, err := bundle.exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			ID:           []int64{job.ID},
			Error:        [][]byte{[]byte(`{"error": "message"}`)},
			FinalizedAt:  []*time.Time{nil},
			ScheduledAt:  []time.Time{now},
			State:        []string{string(rivertype.JobStateAvailable)},
			StuckHorizon: now,
		})
		require.NoError(t, err)
	})

	t.Run("JobRetry", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateDiscarded)})

		jobAfter, err := bundle.exec.JobRetry(ctx, &riverdriver.JobRetryParams{ID: job.ID})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
	})

	t.Run("JobSchedule", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		now := time.Now().UTC()
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{ScheduledAt: new(now.Add(-time.Hour)), State: new(rivertype.JobStateScheduled)})

		results, err := bundle.exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{Max: 10, Now: &now})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, job.ID, results[0].Job.ID)
	})

	t.Run("JobSetStateIfRunningMany", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		now := time.Now().UTC()
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})

		jobsAfter, err := bundle.exec.JobSetStateIfRunningMany(ctx, &riverdriver.JobSetStateIfRunningManyParams{
			ID:              []int64{job.ID},
			Attempt:         []*int{nil},
			ErrData:         [][]byte{nil},
			FinalizedAt:     []*time.Time{&now},
			MetadataDoMerge: []bool{false},
			MetadataUpdates: [][]byte{nil},
			ScheduledAt:     []*time.Time{nil},
			State:           []rivertype.JobState{rivertype.JobStateCompleted},
		})
		require.NoError(t, err)
		require.Len(t, jobsAfter, 1)
		require.Equal(t, rivertype.JobStateCompleted, jobsAfter[0].State)
	})
}

func TestDriverRiverTurso(t *testing.T) {
	t.Parallel()

	var (
		ctx         = context.Background()
		procurePool = func(ctx context.Context, schema string) (any, string) {
			return riversharedtest.DBPoolTurso(ctx, t, schema), "" // could also be `main` instead of empty string
		}
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			if opts == nil {
				opts = &riverdbtest.TestSchemaOpts{}
			}
			opts.ProcurePool = procurePool

			var (
				// Driver will have its pool set by TestSchema.
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, opts)
			)
			return driver, schema
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			// Driver will have its pool set by TestSchema.
			driver := riversqlite.New(nil)

			tx, _ := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
				ProcurePool: procurePool,
			})
			return driver.UnwrapExecutor(tx), driver
		})
}

func BenchmarkDriverRiverDatabaseSQLLibPQ(b *testing.B) {
	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(b, err)

	stdPool := sql.OpenDB(connector)
	b.Cleanup(func() { require.NoError(b, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[*sql.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()

			tx, schema := riverdbtest.TestTx(ctx, b, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(b, err)

			return driver.UnwrapExecutor(tx)
		},
	)
}

func BenchmarkDriverRiverDatabaseSQLPgx(b *testing.B) {
	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, b)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
		schema  = riverdbtest.TestSchema(ctx, b, driver, nil)
	)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[*sql.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()

			tx, schema := riverdbtest.TestTx(ctx, b, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(b, err)

			return driver.UnwrapExecutor(tx)
		},
	)
}

func BenchmarkDriverRiverPgxV5(b *testing.B) {
	var (
		ctx    = context.Background()
		dbPool = riversharedtest.DBPool(ctx, b)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, b, driver, nil)
	)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[pgx.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()
			return riverpgxv5.New(nil).UnwrapExecutor(riverdbtest.TestTxPgx(ctx, b))
		},
	)
}

func BenchmarkDriverRiverPgxV5_Executor(b *testing.B) {
	const (
		clientID = "test-client-id"
		timeout  = 5 * time.Minute
	)

	ctx := context.Background()

	type testBundle struct {
		schema string
	}

	setupPool := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, b)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, b, driver, nil)
		)

		b.ResetTimer()

		return driver.GetExecutor(), &testBundle{
			schema: schema,
		}
	}

	setupTx := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		driver := riverpgxv5.New(nil)
		tx := riverdbtest.TestTxPgx(ctx, b)

		b.ResetTimer()

		return driver.UnwrapExecutor(tx), &testBundle{}
	}

	makeInsertParams := func(bundle *testBundle) *riverdriver.JobInsertFastManyParams {
		return &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`),
				Kind:        "fake_job",
				MaxAttempts: river.MaxAttemptsDefault,
				Metadata:    []byte(`{}`),
				Priority:    river.PriorityDefault,
				Queue:       river.QueueDefault,
				ScheduledAt: nil,
				State:       rivertype.JobStateAvailable,
			}},
			Schema: bundle.schema,
		}
	}

	b.Run("JobInsert_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupTx(b)

		for range b.N {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobInsert_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupPool(b)

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})

	b.Run("JobGetAvailable_100_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupTx(b)

		for range b.N * 100 {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for range b.N {
			if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:  clientID,
				MaxToLock: 100,
				Queue:     river.QueueDefault,
				Schema:    bundle.schema,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobGetAvailable_100_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupPool(b)

		for range b.N * 100 * runtime.NumCPU() {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
					ClientID:  clientID,
					MaxToLock: 100,
					Queue:     river.QueueDefault,
					Schema:    bundle.schema,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkDriverRiverPgxV5Insert(b *testing.B) {
	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
		tx   pgx.Tx
	}

	setup := func(b *testing.B) (*riverpgxv5.Driver, *testBundle) {
		b.Helper()

		var (
			driver = riverpgxv5.New(nil)
			tx     = riverdbtest.TestTxPgx(ctx, b)
		)

		bundle := &testBundle{
			exec: driver.UnwrapExecutor(tx),
			tx:   tx,
		}

		return driver, bundle
	}

	b.Run("InsertFastMany", func(b *testing.B) {
		_, bundle := setup(b)

		for range b.N {
			_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: river.MaxAttemptsDefault,
					Priority:    river.PriorityDefault,
					Queue:       river.QueueDefault,
					State:       rivertype.JobStateAvailable,
				}},
			})
			require.NoError(b, err)
		}
	})

	b.Run("InsertFastMany_WithUnique", func(b *testing.B) {
		_, bundle := setup(b)

		for i := range b.N {
			_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  river.MaxAttemptsDefault,
					Priority:     river.PriorityDefault,
					Queue:        river.QueueDefault,
					State:        rivertype.JobStateAvailable,
					UniqueKey:    []byte("test_unique_key_" + strconv.Itoa(i)),
					UniqueStates: 0xFB,
				}},
			})
			require.NoError(b, err)
		}
	})
}
