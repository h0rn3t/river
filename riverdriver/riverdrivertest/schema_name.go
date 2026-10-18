package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river/riverdbtest"
	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/rivermigrate"
	"github.com/h0rn3t/river/rivershared/riversharedtest"
	"github.com/h0rn3t/river/rivershared/util/randutil"
	"github.com/h0rn3t/river/rivertype"
)

func exerciseSchemaName[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
) {
	t.Helper()

	t.Run("SchemaNameWithQuotes", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			t.Skip("Skipping; schema names with quotes only relevant for Postgres")
		}

		// Quotes of both kinds must be escaped wherever the schema ends up in
		// SQL: as an identifier, in a string literal, or in a LISTEN channel.
		// Kept short because notification channels are prefixed with it.
		schema := `o'b "q" ` + randutil.Hex(4)

		exec := driver.GetExecutor()

		require.NoError(t, exec.SchemaCreate(ctx, &riverdriver.SchemaCreateParams{Schema: schema}))
		t.Cleanup(func() {
			require.NoError(t, exec.SchemaDrop(ctx, &riverdriver.SchemaDropParams{Schema: schema}))
		})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)

		exists, err := exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{
			Column: "line",
			Schema: schema,
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.True(t, exists)

		// A zero ID gets one from the schema's sequence.
		insertedJobs, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`),
				Kind:        "test_kind",
				MaxAttempts: 25,
				Metadata:    []byte(`{}`),
				Priority:    1,
				Queue:       "default",
				State:       rivertype.JobStateAvailable,
			}},
			Schema: schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)
		require.NotZero(t, insertedJobs[0].Job.ID)

		if driver.SupportsListener() {
			listener := driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema})
			require.NoError(t, listener.Connect(ctx))
			t.Cleanup(func() { require.NoError(t, listener.Close(ctx)) })

			require.NoError(t, listener.Listen(ctx, "topic"))
			require.NoError(t, exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{"payload"}, Schema: schema, Topic: "topic"}))

			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			notification, err := listener.WaitForNotification(waitCtx)
			require.NoError(t, err)
			require.Equal(t, &riverdriver.Notification{Payload: "payload", Topic: "topic"}, notification)

			require.NoError(t, listener.Unlisten(ctx, "topic"))
		}

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
			TargetVersion: -1,
		})
		require.NoError(t, err)

		exists, err = exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema,
			Table:  "river_migration",
		})
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("SchemaNameWithSpace", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		// In SQLite schemas are files assigned to particular names, so this
		// check isn't relevant in the same way.
		if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			t.Skip("Skipping; schema names with spaces only relevant for Postgres")
		}

		// Schemas should get cleaned up, but still need some randomness in case
		// multiple tests are running in parallel.
		schema := "river test schema " + randutil.Hex(8)

		exec := driver.GetExecutor()

		require.NoError(t, exec.SchemaCreate(ctx, &riverdriver.SchemaCreateParams{Schema: schema}))
		t.Cleanup(func() {
			require.NoError(t, exec.SchemaDrop(ctx, &riverdriver.SchemaDropParams{Schema: schema}))
		})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)

		// Insert a job and verify it can be read back.
		insertedJobs, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`),
				Kind:        "test_kind",
				MaxAttempts: 25,
				Metadata:    []byte(`{}`),
				Priority:    1,
				Queue:       "default",
				State:       rivertype.JobStateAvailable,
			}},
			Schema: schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)

		fetchedJob, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
			ID:     insertedJobs[0].Job.ID,
			Schema: schema,
		})
		require.NoError(t, err)
		require.Equal(t, insertedJobs[0].Job.ID, fetchedJob.ID)
		require.Equal(t, "test_kind", fetchedJob.Kind)

		// Verify the table exists in the schema.
		exists, err := exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema,
			Table:  "river_job",
		})
		require.NoError(t, err)
		require.True(t, exists)

		// Migrate back down.
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
			TargetVersion: -1,
		})
		require.NoError(t, err)

		// Verify tables are gone.
		exists, err = exec.TableExists(ctx, &riverdriver.TableExistsParams{
			Schema: schema,
			Table:  "river_job",
		})
		require.NoError(t, err)
		require.False(t, exists)
	})
}
