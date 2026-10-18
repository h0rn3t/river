package riverbench

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river"
	"github.com/h0rn3t/river/riverdbtest"
	"github.com/h0rn3t/river/riverdriver/riverpgxv5"
	"github.com/h0rn3t/river/rivershared/riversharedtest"
)

// Failed inserts must not be counted as inserted jobs. With `-n`, the
// benchmark waits until every counted job has been worked, so counting jobs
// that were never inserted makes it wait forever.
func TestBenchmarkerInsertError(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		benchmarker *Benchmarker[pgx.Tx]
		client      *river.Client[pgx.Tx]
	}

	// A driver without a pool makes every InsertMany fail without needing a
	// database.
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			driver = riverpgxv5.New(nil)
			logger = riversharedtest.Logger(t)
		)

		client, err := river.NewClient(driver, &river.Config{Logger: logger})
		require.NoError(t, err)

		return &testBundle{
			benchmarker: NewBenchmarker(driver, &Config{Logger: logger}),
			client:      client,
		}
	}

	t.Run("InsertJobs", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var (
			minJobsReady    = make(chan struct{})
			numJobsInserted atomic.Int64
			numJobsLeft     atomic.Int64
		)

		err := bundle.benchmarker.insertJobs(t.Context(), bundle.client, minJobsReady, &numJobsInserted, &numJobsLeft, 10, make(chan struct{}))
		require.ErrorContains(t, err, "error inserting jobs")
		require.Zero(t, numJobsInserted.Load())
		require.Zero(t, numJobsLeft.Load())
	})

	t.Run("InsertJobsContinuously", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			bundle := setup(t)

			ctx, cancel := context.WithCancel(t.Context())

			var (
				done            = make(chan struct{})
				minJobsReady    = make(chan struct{})
				numJobsInserted atomic.Int64
				numJobsLeft     atomic.Int64
			)

			go func() {
				defer close(done)
				bundle.benchmarker.insertJobsContinuously(ctx, bundle.client, minJobsReady, &numJobsInserted, &numJobsLeft, make(chan struct{}))
			}()

			// Long enough for several insert rounds on the bubble's fake clock.
			time.Sleep(time.Second)
			cancel()
			<-done

			require.Zero(t, numJobsInserted.Load())
			require.Zero(t, numJobsLeft.Load())

			select {
			case <-minJobsReady:
				require.FailNow(t, "minJobsReady closed even though no jobs were inserted")
			default:
			}
		})
	})
}

func TestBenchmarkerInsertJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		benchmarker *Benchmarker[pgx.Tx]
		client      *river.Client[pgx.Tx]
	}

	setup := func(ctx context.Context, t *testing.T) *testBundle {
		t.Helper()

		var (
			driver = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			logger = riversharedtest.Logger(t)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		// Insert-only client since jobs should stay in the database so that
		// their args can be inspected.
		client, err := river.NewClient(driver, &river.Config{
			Logger: logger,
			Schema: schema,
		})
		require.NoError(t, err)

		return &testBundle{
			benchmarker: NewBenchmarker(driver, &Config{Logger: logger, Schema: schema}),
			client:      client,
		}
	}

	t.Run("NumbersJobsSequentiallyAcrossBatches", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		// Read the persisted args across a batch boundary. This catches
		// changes to a copy of the args that never reach InsertMany.
		const numTotalJobs = insertBatchSize + 2

		var (
			minJobsReady    = make(chan struct{})
			numJobsInserted atomic.Int64
			numJobsLeft     atomic.Int64
		)

		require.NoError(t, bundle.benchmarker.insertJobs(ctx, bundle.client, minJobsReady, &numJobsInserted, &numJobsLeft, numTotalJobs, make(chan struct{})))
		require.Equal(t, int64(numTotalJobs), numJobsInserted.Load())

		listRes, err := bundle.client.JobList(ctx, river.NewJobListParams().Kinds((BenchmarkArgs{}).Kind()).First(numTotalJobs))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, numTotalJobs)

		jobNums := make([]int, len(listRes.Jobs))
		for i, job := range listRes.Jobs {
			var args BenchmarkArgs
			require.NoError(t, json.Unmarshal(job.EncodedArgs, &args))
			jobNums[i] = args.Num
		}
		slices.Sort(jobNums)

		for i, jobNum := range jobNums {
			require.Equal(t, i+1, jobNum, "job number at sorted index %d", i)
		}
	})
}
