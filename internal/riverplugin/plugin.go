package riverplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/h0rn3t/river/internal/jobexecutor"
	"github.com/h0rn3t/river/internal/rivercommon"
	"github.com/h0rn3t/river/rivertype"
)

// DefaultPlugins returns the default plugins that River applies to all jobs.
// This includes internal middleware like the resumable step middleware.
func DefaultPlugins() []rivertype.Plugin {
	return []rivertype.Plugin{&ResumableMiddleware{}}
}

// ResumableMiddleware is internal middleware that enables resumable step
// functionality. It reads the last completed step and cursor data from job
// metadata, injects them into the context, and persists updated step/cursor
// state back to metadata when a job errors after making progress.
type ResumableMiddleware struct{}

func (*ResumableMiddleware) IsMiddleware() bool { return true }
func (*ResumableMiddleware) IsPlugin() bool     { return true }

func (*ResumableMiddleware) Work(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
	metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
	if !hasMetadataUpdates {
		return errors.New("expected to find metadata updates in context, but didn't")
	}

	state := &ResumableState{
		AllStepNames:  make(map[string]struct{}),
		Cursors:       make(map[string]json.RawMessage),
		ResumeMatched: true,
		ResumeStep:    gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableStep).Str,
	}
	if state.ResumeStep != "" {
		state.ResumeMatched = false
	}

	hadCursors := false
	if cursorJSON := gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyResumableCursor); cursorJSON.Exists() && cursorJSON.Type == gjson.JSON {
		if err := json.Unmarshal([]byte(cursorJSON.Raw), &state.Cursors); err != nil {
			return fmt.Errorf("river: unmarshal resumable cursors: %w", err)
		}
		hadCursors = len(state.Cursors) > 0
	}

	ctx = context.WithValue(ctx, ResumableContextKey{}, state)

	var (
		err      error
		panicked = true
	)

	// Persist progress from a deferred function so that it's also recorded
	// when a step panics. The executor stores metadata updates for panicked
	// jobs along with their result.
	defer func() {
		if !panicked && err == nil {
			return
		}

		// The latest progress was persisted by ResumableSetStepTx or
		// ResumableSetStepCursorTx. It's durable if its transaction committed
		// and must not be resurrected if it rolled back, so leave the step and
		// cursor stored in the database as they are.
		if state.PersistedInTx {
			return
		}

		if len(state.Cursors) > 0 {
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = state.Cursors
		} else if hadCursors {
			// All cursors were consumed (their steps completed). Write null
			// to clear the stale cursor metadata, since the metadata merge
			// is additive and wouldn't remove the old key on its own.
			metadataUpdates[rivercommon.MetadataKeyResumableCursor] = nil
		}

		if state.CompletedStep != "" {
			metadataUpdates[rivercommon.MetadataKeyResumableStep] = state.CompletedStep
		}
	}()

	err = doInner(ctx)
	panicked = false

	if err == nil {
		switch {
		case state.Err != nil:
			err = state.Err
		case state.ResumeStep != "" && !state.ResumeMatched:
			err = fmt.Errorf("river: resumable step %q not found in Worker", state.ResumeStep)
		}
	}

	return err
}

// ResumableState holds the state for a resumable job execution. It is stored in
// the context and accessed by ResumableStep and ResumableStepCursor.
type ResumableState struct {
	AllStepNames  map[string]struct{}
	CompletedStep string
	Cursors       map[string]json.RawMessage
	Err           error

	// PersistedInTx is set when the most recent step progress was persisted
	// by ResumableSetStepTx or ResumableSetStepCursorTx rather than recorded
	// in memory. Because whether that transaction commits isn't known, step
	// progress isn't written again when the attempt ends. Recording progress
	// in memory afterwards unsets it.
	PersistedInTx bool

	ResumeMatched bool
	ResumeStep    string
	StepName      string
}

// ResumableContextKey is the context key for ResumableState.
type ResumableContextKey struct{}
