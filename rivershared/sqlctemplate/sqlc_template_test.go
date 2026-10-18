package sqlctemplate

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplacer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Replacer, *testBundle) { //nolint:unparam
		t.Helper()

		return &Replacer{}, &testBundle{}
	}

	t.Run("NoContainerError", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		_, _, err := replacer.RunSafely(ctx, "$", `
			SELECT /* TEMPLATE: schema */river_job;
		`, nil)
		require.EqualError(t, err, "sqlctemplate found template(s) in SQL, but no context container; bug?")
	})

	t.Run("NoTemplateError", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{}, nil)

		_, _, err := replacer.RunSafely(ctx, "$", `
			SELECT 1;
		`, nil)
		require.EqualError(t, err, "sqlctemplate found context container but SQL contains no templates; bug?")
	})

	t.Run("NoContainerOrTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT 1;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, `
			SELECT 1;
		`, updatedSQL)
		require.Nil(t, args)
	})

	t.Run("BasicTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema": {Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE state = @state;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM test_schema.river_job
			WHERE state = @state;
		`, updatedSQL)
	})

	t.Run("BeginEndTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"order_by_clause": {Value: "kind, id"},
			"where_clause":    {Value: "kind = 'no_op'"},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			-- name: JobList :many
			SELECT *
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
			ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
			LIMIT @max::int;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			-- name: JobList :many
			SELECT *
			FROM river_job
			WHERE kind = 'no_op'
			ORDER BY kind, id
			LIMIT @max::int;
		`, updatedSQL)
	})

	t.Run("SyntaxMistakes", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		// Missing colon after "TEMPLATE".
		{
			ctx := WithReplacements(ctx, map[string]Replacement{}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /* TEMPLATE schema */river_job
			WHERE state = @state;
		`, nil)
			require.EqualError(t, err, "sqlctemplate found template-like tag after replacements; probably syntax error or missing end tag: /* TEMPLATE")
		}

		// Missing "TEMPLATE_END".
		{
			ctx := WithReplacements(ctx, map[string]Replacement{}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /* TEMPLATE_BEGIN: schema */river_job
			WHERE state = @state;
		`, nil)
			require.EqualError(t, err, "sqlctemplate found template-like tag after replacements; probably syntax error or missing end tag: /* TEMPLATE_BEGIN")
		}

		// Extra whitespace before "TEMPLATE".
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"state": {Value: "'available'"},
			}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /*   TEMPLATE schema */river_job
			WHERE state = /* TEMPLATE_BEGIN: state */ 'available' /* TEMPLATE_END */ -- need to have one valid template to get to the right error
		`, nil)
			require.EqualError(t, err, "sqlctemplate found template-like tag after replacements; probably syntax error or missing end tag: /*   TEMPLATE")
		}
	})

	t.Run("RepeatedTemplate", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema": {Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job r1
				INNER JOIN /* TEMPLATE: schema */river_job r2 ON r1.id = r2.id;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job r1
				INNER JOIN test_schema.river_job r2 ON r1.id = r2.id;
		`, updatedSQL)
	})

	t.Run("AllTemplatesStableCached", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema": {Stable: true, Value: "test_schema."},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job;
		`, updatedSQL)

		require.Len(t, replacer.cache, 1)

		// Invoke again to make sure we get back the same result.
		updatedSQL, args, err = replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job;
		`, updatedSQL)
	})

	t.Run("AnyTemplateNotStableNotCached", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema":       {Stable: true, Value: "test_schema."},
			"where_clause": {Value: "kind = 'no_op'"},
		}, nil)

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Nil(t, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job
			WHERE kind = 'no_op';
		`, updatedSQL)

		require.Empty(t, replacer.cache)
	})

	t.Run("CacheBasedOnInputValues", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		// SQL stays constant across all runs.
		const sql = `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE kind = @kind
				AND state = @state;
			`

		// Initially cached value
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "test_schema."},
			}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", sql, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 1)

		// Same SQL, but new value.
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "other_schema."},
			}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", sql, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 2)

		// Named arg added to the mix.
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "test_schema."},
			}, map[string]any{
				"kind": "kind_value",
			})

			_, _, err := replacer.RunSafely(ctx, "$", sql, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 3)

		// Different named arg _value_ (i.e. still same named arg) can still use
		// the previous cached SQL.
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "test_schema."},
			}, map[string]any{
				"kind": "other_kind_value",
			})

			_, _, err := replacer.RunSafely(ctx, "$", sql, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 3) // unchanged

		// New named arg adds a new cache value.
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "test_schema."},
			}, map[string]any{
				"kind":  "kind_value",
				"state": "state_value",
			})

			_, _, err := replacer.RunSafely(ctx, "$", sql, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 4)

		// Different input SQL.
		{
			ctx := WithReplacements(ctx, map[string]Replacement{
				"schema": {Stable: true, Value: "test_schema."},
			}, nil)

			_, _, err := replacer.RunSafely(ctx, "$", `
			SELECT /* TEMPLATE: schema */river_job;
		`, nil)
			require.NoError(t, err)
		}
		require.Len(t, replacer.cache, 5)
	})

	t.Run("NamedArgsMissing", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		// `@kind_name` is present, but that must not satisfy `@kind`.
		ctx := WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind_name = @kind_name"},
		}, map[string]any{
			"kind": "no_op",
		})

		_, _, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */;
		`, nil)
		require.EqualError(t, err, `sqltemplate expected to find named arg "@kind", but it wasn't present`)
	})

	t.Run("NamedArgsNoInitialArgs", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"no_op"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = $1;
		`, updatedSQL)
	})

	t.Run("NamedArgsOverlappingNames", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		// Each name is a prefix of another. Sorted, `after` and `kind` are
		// numbered before `after_id` and `kind_name`, which must not cause
		// them to clobber the longer names.
		ctx := WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind AND kind_name = @kind_name AND id > @after_id AND created_at > @after AND tags @> @tags AND kind = @kind"},
		}, map[string]any{
			"after":     "2025-01-01",
			"after_id":  123,
			"kind":      "no_op",
			"kind_name": "custom",
			"tags":      []string{"a"},
		})

		const sql = `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */;
		`

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", sql, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"2025-01-01", 123, "no_op", "custom", []string{"a"}}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = $3 AND kind_name = $4 AND id > $2 AND created_at > $1 AND tags @> $5 AND kind = $3;
		`, updatedSQL)

		require.NotPanics(t, func() { replacer.Run(ctx, "$", sql, nil) })
	})

	t.Run("NamedArgsSQLServerPlaceholder", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		// A placeholder like `@p1` must not be rescanned as a named arg, and
		// system variables like `@@ROWCOUNT` and unknown symbols like
		// `@other` are left untouched.
		ctx := WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind AND p1 = @p1 AND @@ROWCOUNT > 0 AND x = @other"},
		}, map[string]any{
			"kind": "no_op",
			"p1":   "val",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, "@p", `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ 1 = 1 /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"no_op", "val"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = @p1 AND p1 = @p2 AND @@ROWCOUNT > 0 AND x = @other;
		`, updatedSQL)
	})

	t.Run("NamedArgsWithInitialArgs", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */
				AND status = $1;
		`, []any{"succeeded"})
		require.NoError(t, err)
		require.Equal(t, []any{"succeeded", "no_op"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM river_job
			WHERE kind = $2
				AND status = $1;
		`, updatedSQL)
	})

	t.Run("MultipleWithReplacementsOverrides", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema":       {Stable: true, Value: "test_schema."},
			"where_clause": {Value: "kind = @kind"},
		}, map[string]any{
			"kind": "no_op",
		})

		ctx = WithReplacements(ctx, map[string]Replacement{
			"where_clause": {Value: "kind = @kind AND status = @status"},
		}, map[string]any{
			"status": "succeeded",
		})

		updatedSQL, args, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE /* TEMPLATE_BEGIN: where_clause */ true /* TEMPLATE_END */;
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []any{"no_op", "succeeded"}, args)
		require.Equal(t, `
			SELECT count(*)
			FROM test_schema.river_job
			WHERE kind = $1 AND status = $2;
		`, updatedSQL)
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		const (
			clearCacheIterations = 10
			numIterations        = 50
		)

		replacer, _ := setup(t)

		periodicallyClearCache := func(i int, replacer *Replacer) {
			if i+1%clearCacheIterations == 0 { // +1 so we don't clear cache on i == 0
				replacer.cacheMu.Lock()
				replacer.cache = nil
				replacer.cacheMu.Unlock()
			}
		}

		var wg sync.WaitGroup

		wg.Go(func() {
			for i := range numIterations {
				ctx := WithReplacements(ctx, map[string]Replacement{
					"schema": {Value: "test_schema."},
				}, nil)

				updatedSQL, _, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*) FROM /* TEMPLATE: schema */river_job;
		`, nil)
				require.NoError(t, err)
				require.Equal(t, `
			SELECT count(*) FROM test_schema.river_job;
		`, updatedSQL)

				periodicallyClearCache(i, replacer)
			}
		})

		wg.Go(func() {
			for i := range numIterations {
				ctx := WithReplacements(ctx, map[string]Replacement{
					"schema": {Stable: true, Value: "test_schema."},
				}, nil)

				updatedSQL, _, err := replacer.RunSafely(ctx, "$", `
			SELECT distinct(kind) FROM /* TEMPLATE: schema */river_job;
		`, nil)
				require.NoError(t, err)
				require.Equal(t, `
			SELECT distinct(kind) FROM test_schema.river_job;
		`, updatedSQL)

				periodicallyClearCache(i, replacer)
			}
		})

		wg.Go(func() {
			for i := range numIterations {
				ctx := WithReplacements(ctx, map[string]Replacement{
					"schema": {Stable: true, Value: "test_schema."},
				}, nil)

				updatedSQL, _, err := replacer.RunSafely(ctx, "$", `
			SELECT count(*) FROM /* TEMPLATE: schema */river_job WHERE status = 'succeeded';
		`, nil)
				require.NoError(t, err)
				require.Equal(t, `
			SELECT count(*) FROM test_schema.river_job WHERE status = 'succeeded';
		`, updatedSQL)

				periodicallyClearCache(i, replacer)
			}
		})

		wg.Wait()
	})
}

func BenchmarkReplacer(b *testing.B) {
	ctx := context.Background()

	runReplacer := func(b *testing.B, replacer *Replacer, stable bool) {
		b.Helper()

		ctx := WithReplacements(ctx, map[string]Replacement{
			"schema": {Stable: stable, Value: "test_schema."},
		}, nil)

		_, _, err := replacer.RunSafely(ctx, "$", `
			-- name: JobCountByState :one
			SELECT count(*)
			FROM /* TEMPLATE: schema */river_job
			WHERE state = @state;
		`, nil)
		require.NoError(b, err)
	}

	b.Run("WithCache", func(b *testing.B) {
		var replacer Replacer
		for range b.N {
			runReplacer(b, &replacer, true)
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		var replacer Replacer
		for range b.N {
			runReplacer(b, &replacer, false)
		}
	})
}
