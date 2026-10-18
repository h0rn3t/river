package riversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/rivershared/sqlctemplate"
	"github.com/h0rn3t/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestDurationAsString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "3.000 seconds", durationAsString(3*time.Second))
	require.Equal(t, "3.255 seconds", durationAsString(3*time.Second+255*time.Millisecond))
}

func TestExecutorBegin(t *testing.T) {
	t.Parallel()

	t.Run("FailedBeginDiscardsConnection", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opened, closed int
		dbPool := sql.OpenDB(&beginTestConnector{
			connectFunc: func() (driver.Conn, error) {
				opened++
				return &beginTestConn{
					beginFunc: func() (driver.Tx, error) {
						// Simulate a driver that starts a transaction, but
						// returns a cancellation error without rolling back.
						cancel()
						return nil, context.Canceled
					},
					closeFunc: func() { closed++ },
				}, nil
			},
		})
		t.Cleanup(func() { require.NoError(t, dbPool.Close()) })
		dbPool.SetMaxOpenConns(1)
		exec := New(dbPool).GetExecutor()

		tx, err := exec.Begin(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, tx)
		require.Equal(t, 1, opened)
		require.Equal(t, 1, closed)

		// The next checkout must open a new connection instead of reusing
		// the one whose transaction state is unknown.
		conn, err := dbPool.Conn(context.Background())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		require.Equal(t, 2, opened)
	})
}

type beginTestConn struct {
	driver.Conn

	beginFunc func() (driver.Tx, error)
	closeFunc func()
}

func (c *beginTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.beginFunc()
}

func (c *beginTestConn) Close() error {
	c.closeFunc()
	return nil
}

type beginTestConnector struct {
	driver.Connector

	connectFunc func() (driver.Conn, error)
}

func (c *beginTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.connectFunc()
}

func TestExecutorClosedPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// The pool is closed before it's used, so it never connects.
	dbPool := sql.OpenDB(&beginTestConnector{})
	require.NoError(t, dbPool.Close())
	exec := New(dbPool).GetExecutor()

	t.Run("JobGetAvailable", func(t *testing.T) {
		t.Parallel()

		_, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{MaxToLock: 1, Queue: "default"})
		require.ErrorIs(t, err, riverdriver.ErrClosedPool)
	})

	t.Run("JobSetStateIfRunningMany", func(t *testing.T) {
		t.Parallel()

		_, err := exec.JobSetStateIfRunningMany(ctx, &riverdriver.JobSetStateIfRunningManyParams{ID: []int64{1}})
		require.ErrorIs(t, err, riverdriver.ErrClosedPool)
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))

	err := interpretError(sql.ErrConnDone)
	require.ErrorIs(t, err, riverdriver.ErrClosedPool)
	require.ErrorIs(t, err, sql.ErrConnDone)

	closedErr := fmt.Errorf("error beginning transaction: %w", errors.New("sql: database is closed"))
	err = interpretError(closedErr)
	require.ErrorIs(t, err, riverdriver.ErrClosedPool)
	require.ErrorIs(t, err, closedErr)
}

func TestTimeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "2025-04-30 13:26:39.100", timeString(time.Date(2025, 4, 30, 13, 26, 39, 100000000, time.UTC)))
	require.Equal(t, "2025-04-30 13:26:39.123", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC)))
	require.Equal(t, "2025-04-30 13:26:39.124", timeString(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC))) // test rounding
}

func TestTimeStringNullable(t *testing.T) {
	t.Parallel()

	require.Nil(t, timeStringNullable(nil))
	require.Equal(t, "2025-04-30 13:26:39.100", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 100000000, time.UTC))))
	require.Equal(t, "2025-04-30 13:26:39.123", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 123456789, time.UTC))))
	require.Equal(t, "2025-04-30 13:26:39.124", *timeStringNullable(new(time.Date(2025, 4, 30, 13, 26, 39, 123800000, time.UTC)))) // test rounding
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *Driver
	}

	setup := func(t *testing.T) (*sqlctemplate.Replacer, *testBundle) {
		t.Helper()

		return &sqlctemplate.Replacer{}, &testBundle{
			driver: New(nil),
		}
	}

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "custom_schema"),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1 FROM "custom_schema".river_job`, updatedSQL)
	})

	t.Run("SchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1 FROM "my_schema".river_job`, updatedSQL1)

		// Second call with same SQL + same schema produces identical result.
		// Because schema is marked Stable, the Replacer caches the output
		// after the first call and short-circuits regex on subsequent calls.
		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})

	t.Run("EmptySchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL1)

		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})
}
