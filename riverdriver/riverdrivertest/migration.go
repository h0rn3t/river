package riverdrivertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river/riverdbtest"
	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/rivermigrate"
	"github.com/h0rn3t/river/rivershared/riversharedtest"
	"github.com/h0rn3t/river/rivershared/testfactory"
	"github.com/h0rn3t/river/rivershared/testsignal"
	"github.com/h0rn3t/river/rivershared/uniquestates"
	"github.com/h0rn3t/river/rivershared/util/dbutil"
	"github.com/h0rn3t/river/rivershared/util/sliceutil"
	"github.com/h0rn3t/river/rivertype"
)

func exerciseMigration[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	t.Run("GetMigrationFS", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		for _, line := range driver.GetMigrationLines() {
			migrationFS := driver.GetMigrationFS(line)

			// Directory for the advertised migration line should exist.
			_, err := migrationFS.Open("migration/" + line)
			require.NoError(t, err)
		}
	})

	t.Run("GetMigrationTruncateTables", func(t *testing.T) {
		t.Parallel()

		t.Run("AllLinesNonEmpty", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)

			for _, line := range driver.GetMigrationLines() {
				truncateTables := driver.GetMigrationTruncateTables(line, 0)

				// Technically a migration line's truncate tables might be empty,
				// but this never happens in any of our migration lines, so check
				// non-empty until it becomes an actual problem.
				require.NotEmpty(t, truncateTables)
			}
		})

		t.Run("MainLine", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			expectedLatestTables := []string{"river_job", "river_leader", "river_queue", "river_notification"}

			require.Empty(t, driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 1))
			require.Equal(t, []string{"river_job", "river_leader"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 2))
			require.Equal(t, []string{"river_job", "river_leader"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 3))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 4))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 5))
			require.Equal(t, []string{"river_job", "river_leader", "river_queue", "river_client", "river_client_queue"},
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 6))
			require.Equal(t, expectedLatestTables,
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 7))
			require.Equal(t, expectedLatestTables,
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 8))
			require.Equal(t, expectedLatestTables,
				driver.GetMigrationTruncateTables(riverdriver.MigrationLineMain, 0))
		})
	})

	t.Run("GetMigrationLines", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		// Should contain at minimum a main migration line.
		require.Contains(t, driver.GetMigrationLines(), riverdriver.MigrationLineMain)
	})

	// This doesn't map to a particular function, but make sure the driver is
	// capable of migration all the way up, then all the way back down.
	t.Run("MigrateUpAndDown", func(t *testing.T) {
		t.Parallel()

		driver, _ := driverWithSchema(ctx, t, nil)

		for _, line := range driver.GetMigrationLines() {
			t.Run(strings.ToUpper(line[0:1])+line[1:], func(t *testing.T) {
				driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
					Lines: []string{},
				})

				migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
					Line:   line,
					Logger: riversharedtest.Logger(t),
					Schema: schema,
				})
				require.NoError(t, err)

				{
					t.Log("Migrating up (round 1)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
					require.NoError(t, err)

					t.Log("Migrating down (round 1)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
						TargetVersion: -1,
					})
					require.NoError(t, err)
				}

				// Do the process a second time to make sure all migrations
				// really were idempotent and didn't leave artifacts.
				{
					t.Log("Migrating up (round 2)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
					require.NoError(t, err)

					t.Log("Migrating down (round 2)")
					_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
						TargetVersion: -1,
					})
					require.NoError(t, err)
				}

				// Last check to make sure we really went down to zero.
				exists, err := driver.GetExecutor().TableExists(ctx, &riverdriver.TableExistsParams{
					Schema: schema,
					Table:  "river_migration",
				})
				require.NoError(t, err)
				require.False(t, exists)
			})
		}
	})

	t.Run("MigrateConcurrently", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			Lines: []string{},
		})

		newMigrator := func(t *testing.T) *rivermigrate.Migrator[TTx] {
			t.Helper()

			migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
				Line:   riverdriver.MigrationLineMain,
				Logger: riversharedtest.Logger(t),
				Schema: schema,
			})
			require.NoError(t, err)
			return migrator
		}

		// Runs two migrators at once, returning the versions that either of
		// them applied, sorted.
		migrateConcurrently := func(t *testing.T, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) []int {
			t.Helper()

			var (
				errs    = make([]error, 2)
				results = make([]*rivermigrate.MigrateResult, 2)
				wg      sync.WaitGroup
			)
			for i := range 2 {
				migrator := newMigrator(t)
				wg.Go(func() { results[i], errs[i] = migrator.Migrate(ctx, direction, opts) })
			}
			wg.Wait()
			require.NoError(t, errors.Join(errs...))

			var versions []int
			for _, res := range results {
				for _, version := range res.Versions {
					versions = append(versions, version.Version)
				}
			}
			slices.Sort(versions)
			return versions
		}

		allVersions := sliceutil.Map(newMigrator(t).AllVersions(), func(migration rivermigrate.Migration) int { return migration.Version })

		// Every version is applied, then removed, exactly once across both
		// migrators, and neither of them errors on a version that the other
		// one already handled.
		require.Equal(t, allVersions, migrateConcurrently(t, rivermigrate.DirectionUp, nil))
		require.Equal(t, allVersions, migrateConcurrently(t, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{TargetVersion: -1}))
	})

	t.Run("MigrateDownFromVersionEightWithJobData", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			DisableReuse: true,
		})
		exec := driver.GetExecutor()

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Line:   riverdriver.MigrationLineMain,
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
			TargetVersion: 6,
		})
		require.NoError(t, err)

		job, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)
		require.NotZero(t, job.ID)
	})

	t.Run("MigrateDownFromVersionSixWithDuplicateUniqueKeys", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			DisableReuse: true,
		})
		if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			t.Skip("Skipping; duplicate unique key cleanup is only implemented for Postgres")
		}
		exec := driver.GetExecutor()

		// Since 006, a completed job may share a unique key with a newer
		// available one as long as completed isn't one of its unique states.
		// These used to prevent the (kind, unique_key) index from being
		// recreated on the way down.
		uniqueStates := uniquestates.UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable})
		completedJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			FinalizedAt:  new(time.Now()),
			Kind:         new("duplicate_kind"),
			Schema:       schema,
			State:        new(rivertype.JobStateCompleted),
			UniqueKey:    []byte("duplicate_key"),
			UniqueStates: uniqueStates,
		})
		availableJob := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Kind:         new("duplicate_kind"),
			Schema:       schema,
			State:        new(rivertype.JobStateAvailable),
			UniqueKey:    []byte("duplicate_key"),
			UniqueStates: uniqueStates,
		})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Line:   riverdriver.MigrationLineMain,
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
			TargetVersion: 5,
		})
		require.NoError(t, err)

		// The key is kept on the job where it was still enforced.
		var keptJobID int64
		require.NoError(t, exec.QueryRow(ctx,
			fmt.Sprintf("SELECT id FROM %s.river_job WHERE unique_key IS NOT NULL", dbutil.SafeIdentifier(schema)),
		).Scan(&keptJobID))
		require.Equal(t, availableJob.ID, keptJobID)
		require.NotEqual(t, completedJob.ID, keptJobID)
	})

	t.Run("MigrateUpFromVersionSevenWithJobData", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			DisableReuse: true,
			LineTargetVersions: map[string]int{
				riverdriver.MigrationLineMain: 7,
			},
		})
		exec := driver.GetExecutor()

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Line:   riverdriver.MigrationLineMain,
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)

		job, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)

		_, err = exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)

		jobAfter := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})
		require.Greater(t, jobAfter.ID, job.ID)
	})

	t.Run("MigrateUpFromVersionSixWithQueueData", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
			DisableReuse: true,
			LineTargetVersions: map[string]int{
				riverdriver.MigrationLineMain: 6,
			},
		})
		exec := driver.GetExecutor()

		queueTable := "river_queue"
		if driver.DatabaseName() != riverdriver.DatabaseNameSQLite {
			queueTable = schema + ".river_queue"
		}

		_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
			Name:   new("default"),
			Schema: schema,
		})

		migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
			Line:   riverdriver.MigrationLineMain,
			Logger: riversharedtest.Logger(t),
			Schema: schema,
		})
		require.NoError(t, err)

		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
		require.NoError(t, err)

		queue, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{Schema: schema, Name: "default"})
		require.NoError(t, err)
		require.Equal(t, "default", queue.Name)
		require.NotZero(t, queue.UpdatedAt)

		require.NoError(t, exec.Exec(ctx, fmt.Sprintf("INSERT INTO %s (name) VALUES ('queue-with-defaults')", queueTable)))

		queue, err = exec.QueueGet(ctx, &riverdriver.QueueGetParams{Schema: schema, Name: "queue-with-defaults"})
		require.NoError(t, err)
		require.Equal(t, "queue-with-defaults", queue.Name)
		require.NotZero(t, queue.UpdatedAt)
	})

	t.Run("MigrateVersionEightRejectsProSchema", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			name string
			sql  string
		}{
			{"LegacyWorkflow", `CREATE INDEX river_job_workflow_scheduling ON river_job (state)`},
			{"Pro", `CREATE TABLE river_job_sequence (id integer PRIMARY KEY, key text)`},
			{"WorkflowV2", `CREATE TABLE river_workflow (id text PRIMARY KEY)`},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				for _, direction := range []rivermigrate.Direction{rivermigrate.DirectionDown, rivermigrate.DirectionUp} {
					t.Run(string(direction), func(t *testing.T) {
						t.Parallel()

						version := 7
						if direction == rivermigrate.DirectionDown {
							version = 8
						}
						driver, schema := driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
							DisableReuse: true,
							LineTargetVersions: map[string]int{
								riverdriver.MigrationLineMain: version,
							},
							Lines: []string{riverdriver.MigrationLineMain},
						})
						if driver.DatabaseName() != riverdriver.DatabaseNameSQLite {
							t.Skip("SQLite table rebuild")
						}
						exec := driver.GetExecutor()
						job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})

						// Deliberately omit Pro migration records, as when applying SQL
						// through an external migration tool.
						require.NoError(t, exec.Exec(ctx, testCase.sql))
						require.NoError(t, exec.Exec(ctx, `ALTER TABLE river_job ADD COLUMN partition_key text`))
						migrator, err := rivermigrate.New(driver, &rivermigrate.Config{Logger: riversharedtest.Logger(t), Schema: schema})
						require.NoError(t, err)

						_, err = migrator.Migrate(ctx, direction, &rivermigrate.MigrateOpts{MaxSteps: 1})
						require.ErrorContains(t, err, "River SQLite migration 008 cannot run while River Pro schema is installed")

						jobAfter, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
						require.NoError(t, err)
						require.Equal(t, job, jobAfter)
						exists, err := exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{Column: "partition_key", Schema: schema, Table: "river_job"})
						require.NoError(t, err)
						require.True(t, exists)
						exists, err = exec.IndexExists(ctx, &riverdriver.IndexExistsParams{Index: "river_job_kind", Schema: schema})
						require.NoError(t, err)
						require.True(t, exists)
						migrations, err := exec.MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{Line: riverdriver.MigrationLineMain, Schema: schema})
						require.NoError(t, err)
						require.Len(t, migrations, version)
					})
				}
			})
		}
	})

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		execTx, driver := executorWithTx(ctx, t)

		return execTx, &testBundle{
			driver: driver,
		}
	}

	t.Run("MigrationDeleteAssumingMainMany", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		// Doesn't use testfactory because we're using an old schema version.
		var (
			migration1 *riverdriver.Migration
			migration2 *riverdriver.Migration
		)
		{
			migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
				Schema:   schema,
				Versions: []int{1, 2},
			})
			require.NoError(t, err)
			migration1 = migrations[0]
			migration2 = migrations[1]
		}

		migrations, err := exec.MigrationDeleteAssumingMainMany(ctx, &riverdriver.MigrationDeleteAssumingMainManyParams{
			Schema: schema,
			Versions: []int{
				migration1.Version,
				migration2.Version,
			},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationDeleteByLineAndVersionMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		// not touched
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: new("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: new("alternate")})

		migrations, err := exec.MigrationDeleteByLineAndVersionMany(ctx, &riverdriver.MigrationDeleteByLineAndVersionManyParams{
			Line: "alternate",

			Versions: []int{
				migration1.Version,
				migration2.Version,
			},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		slices.SortFunc(migrations, func(a, b *riverdriver.Migration) int { return a.Version - b.Version })
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, migration2.Version, migrations[1].Version)
	})

	t.Run("MigrationGetAllAssumingMain", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		// Doesn't use testfactory because we're using an old schema version.
		var (
			migration1 *riverdriver.Migration
			migration2 *riverdriver.Migration
		)
		{
			migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
				Schema:   schema,
				Versions: []int{1, 2},
			})
			require.NoError(t, err)
			migration1 = migrations[0]
			migration2 = migrations[1]
		}

		migrations, err := exec.MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{
			Schema: schema,
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		require.WithinDuration(t, migration1.CreatedAt, migration1Fetched.CreatedAt, driver.TimePrecision())
		require.Equal(t, riverdriver.MigrationLineMain, migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationGetByLine", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		// not returned
		_ = testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{})

		migration1 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: new("alternate")})
		migration2 := testfactory.Migration(ctx, t, exec, &testfactory.MigrationOpts{Line: new("alternate")})

		migrations, err := exec.MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
			Line: "alternate",
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, migration1.Version, migrations[0].Version)
		require.Equal(t, migration2.Version, migrations[1].Version)

		// Check the full properties of one of the migrations.
		migration1Fetched := migrations[0]
		require.WithinDuration(t, migration1.CreatedAt, migration1Fetched.CreatedAt, bundle.driver.TimePrecision())
		require.Equal(t, "alternate", migration1Fetched.Line)
		require.Equal(t, migration1.Version, migration1Fetched.Version)
	})

	t.Run("MigrationInsertMany", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Table: []string{"river_migration"}}))

		migrations, err := exec.MigrationInsertMany(ctx, &riverdriver.MigrationInsertManyParams{
			Line:     "alternate",
			Versions: []int{1, 2},
		})
		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, "alternate", migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, "alternate", migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("MigrationInsertManyAssumingMain", func(t *testing.T) {
		t.Parallel()

		// Use dedicated schema instead of test transaction because SQLite
		// doesn't support transaction DDL.
		var (
			driver, schema = driverWithSchema(ctx, t, &riverdbtest.TestSchemaOpts{
				LineTargetVersions: map[string]int{
					riverdriver.MigrationLineMain: 4,
				},
			})
			exec = driver.GetExecutor()
		)

		// Truncates the migration table so we only have to work with test
		// migration data.
		require.NoError(t, exec.TableTruncate(ctx, &riverdriver.TableTruncateParams{Schema: schema, Table: []string{"river_migration"}}))

		migrations, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
			Schema:   schema,
			Versions: []int{1, 2},
		})

		require.NoError(t, err)
		require.Len(t, migrations, 2)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[0].Line)
		require.Equal(t, 1, migrations[0].Version)
		require.Equal(t, riverdriver.MigrationLineMain, migrations[1].Line)
		require.Equal(t, 2, migrations[1].Version)
	})

	t.Run("MigrationLock", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t, nil)
		exec := driver.GetExecutor()

		lockParams := &riverdriver.MigrationLockParams{Line: riverdriver.MigrationLineMain, Schema: schema}

		execTx, err := exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = execTx.Rollback(context.WithoutCancel(ctx)) })

		require.NoError(t, execTx.MigrationLock(ctx, lockParams))

		// SQLite's implementation is a no-op because its write transactions
		// are serialized. Test pools also only have one connection, so a
		// second transaction couldn't start anyway.
		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			return
		}

		// Acquires the lock in a separate transaction, holding it until
		// returning.
		lockInOtherTx := func(ctx context.Context, params *riverdriver.MigrationLockParams) error {
			otherExecTx, err := exec.Begin(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = otherExecTx.Rollback(context.WithoutCancel(ctx)) }()

			return otherExecTx.MigrationLock(ctx, params)
		}

		// A lock on another line doesn't conflict.
		{
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			require.NoError(t, lockInOtherTx(ctx, &riverdriver.MigrationLockParams{Line: "other_line", Schema: schema}))
		}

		// A lock on the same line blocks until the first transaction ends.
		var otherLocked testsignal.TestSignal[error]
		otherLocked.Init(t)
		go func() { otherLocked.Signal(lockInOtherTx(ctx, lockParams)) }()

		select {
		case err := <-otherLocked.WaitC():
			require.FailNow(t, "Lock acquired while held by another transaction", "err: %v", err)
		case <-time.After(500 * time.Millisecond):
		}

		require.NoError(t, execTx.Rollback(ctx))
		require.NoError(t, otherLocked.WaitOrTimeout())
	})
}
