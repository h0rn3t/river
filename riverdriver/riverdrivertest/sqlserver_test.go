package riverdrivertest

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river/riverdbtest"
	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/riverdriver/riversqlserver"
)

// sqlServerDatabaseURLEnv is the environment variable containing the URL of
// the SQL Server database used by SQL Server tests, which are skipped when it's
// unset. For example:
//
//	TEST_SQLSERVER_DATABASE_URL="sqlserver://sa:Password1!@localhost:1433?database=river_test"
const sqlServerDatabaseURLEnv = "TEST_SQLSERVER_DATABASE_URL"

func TestClientWithDriverRiverSQLServer(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		driver = riversqlserver.New(dbPoolSQLServer(t))
	)

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
	)
}

func TestDriverRiverSQLServer(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		dbPool = dbPoolSQLServer(t)
		driver = riversqlserver.New(dbPool)
	)

	// Drop the users made by testTxSQLServer for schemas that have since been
	// dropped, like riverdbtest drops the schemas of previous test runs.
	_, err := dbPool.ExecContext(ctx, `
		DECLARE @statement nvarchar(max);
		SELECT @statement = STRING_AGG(CAST(N'DROP USER ' + QUOTENAME(name) AS nvarchar(max)), N'; ')
		FROM sys.database_principals
		WHERE type = 'S'
			AND authentication_type = 0 -- no login
			AND name = default_schema_name
			AND SCHEMA_ID(default_schema_name) IS NULL;
		IF @statement IS NOT NULL EXEC sp_executesql @statement;`)
	require.NoError(t, err)

	Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			return driver.UnwrapExecutor(testTxSQLServer(ctx, t, dbPool, driver)), driver
		})
}

// dbPoolSQLServer opens a pool to the database in TEST_SQLSERVER_DATABASE_URL,
// skipping the test if it's not set.
func dbPoolSQLServer(t *testing.T) *sql.DB {
	t.Helper()

	databaseURL := os.Getenv(sqlServerDatabaseURLEnv)
	if databaseURL == "" {
		t.Skip(sqlServerDatabaseURLEnv + " isn't set; skipping SQL Server tests")
	}

	dbPool, err := sql.Open("sqlserver", databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbPool.Close()) })

	return dbPool
}

// testTxSQLServer starts a test transaction in which names that aren't schema
// qualified resolve to its test schema, like riverdbtest.TestTxPgx does in
// Postgres by setting `search_path`.
//
// SQL Server has no `search_path`. Instead, unqualified names resolve to the
// default schema of the current user, so the transaction impersonates a user
// (without a login) whose default schema is the test schema. Each transaction
// gets a test schema of its own because otherwise SQL Server's locking reads
// would have concurrent tests block on each other's uncommitted rows.
func testTxSQLServer(ctx context.Context, t *testing.T, dbPool *sql.DB, driver *riversqlserver.Driver) *sql.Tx {
	t.Helper()

	tx, schema := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
		DisableSchemaSharing: true,
	})

	// Test schemas are reused, so the user may already exist. It's made with
	// the pool rather than the transaction so it outlives the transaction's
	// rollback.
	_, err := dbPool.ExecContext(ctx, `
		IF DATABASE_PRINCIPAL_ID(@schema) IS NULL
		BEGIN
			DECLARE @statement nvarchar(max) =
				N'CREATE USER ' + QUOTENAME(@schema) + N' WITHOUT LOGIN WITH DEFAULT_SCHEMA = ' + QUOTENAME(@schema) + N'; ' +
				N'ALTER ROLE db_owner ADD MEMBER ' + QUOTENAME(@schema);
			EXEC sp_executesql @statement;
		END`, sql.Named("schema", schema))
	require.NoError(t, err)

	// Run without arguments so that go-mssqldb sends a plain batch. With
	// arguments, it's run through sp_executesql, and the impersonation would
	// end with it.
	_, err = tx.ExecContext(ctx, "EXECUTE AS USER = N'"+strings.ReplaceAll(schema, "'", "''")+"'")
	require.NoError(t, err)

	// Registered after TestTx's cleanup so that it runs before the transaction
	// is rolled back. The error is ignored because a test may have already
	// ended the transaction, in which case the pool's session reset reverts the
	// impersonation instead.
	t.Cleanup(func() {
		_, _ = tx.ExecContext(context.WithoutCancel(ctx), "REVERT")
	})

	return tx
}
