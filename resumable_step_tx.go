package river

import (
	"context"
	"encoding/json"
	"errors"
	"maps"

	"github.com/h0rn3t/river/internal/execution"
	"github.com/h0rn3t/river/internal/rivercommon"
	"github.com/h0rn3t/river/riverdriver"
	"github.com/h0rn3t/river/rivertype"
)

// ResumableSetStepTx immediately persists the current resumable step as
// part of transaction tx. If tx is rolled back, the step update will be as
// well.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination. This function is useful in cases where a
// resumable worker needs a guarantee of a checkpoint being recorded durably, at
// the cost of an extra database operation.
//
// Must be called from within a ResumableStep or ResumableStepCursor callback.
// The current step name to persist is read from context.
func ResumableSetStepTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs]) (*Job[TArgs], error) {
	return resumableSetStepTx(ctx, tx, job, nil)
}

// ResumableSetStepCursorTx immediately persists the current resumable step and
// cursor as part of transaction tx. If tx is rolled back, the step and cursor
// update will be as well.
//
// Normally, a resumable job's step progress is recorded after it runs along
// with its result status. This is normally sufficient, but because it happens
// out-of-transaction, there's a chance that it doesn't happen in case of panic
// or other abrupt termination. This function is useful in cases where a
// resumable worker needs a guarantee of a checkpoint being recorded durably, at
// the cost of an extra database operation.
//
// Must be called from within a ResumableStepCursor callback. The current step
// name to persist is read from context.
func ResumableSetStepCursorTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs JobArgs, TCursor any](ctx context.Context, tx TTx, job *Job[TArgs], cursor TCursor) (*Job[TArgs], error) {
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		return nil, err
	}

	return resumableSetStepTx(ctx, tx, job, json.RawMessage(cursorBytes))
}

func resumableSetStepTx[TTx any, TArgs JobArgs](ctx context.Context, tx TTx, job *Job[TArgs], cursor json.RawMessage) (*Job[TArgs], error) {
	if job.State != rivertype.JobStateRunning {
		return nil, errors.New("job must be running")
	}

	state, ok := resumableStateFromContext(ctx)
	if !ok {
		return nil, errors.New("not inside a resumable step; must be called from within ResumableStep or ResumableStepCursor")
	}
	if state.StepName == "" {
		return nil, errors.New("not inside a resumable step; must be called from within ResumableStep or ResumableStepCursor")
	}

	step := state.StepName

	client := ClientFromContext[TTx](ctx)
	if client == nil {
		return nil, errors.New("client not found in context, can only work within a River worker")
	}

	metadataUpdates := map[string]any{
		rivercommon.MetadataKeyResumableStep: step,
	}

	// In-memory resumable state and the work context's metadata updates are
	// left untouched because they'd be persisted when the attempt ends even if
	// tx is rolled back. Cursors are copied for the same reason.
	cursors := maps.Clone(state.Cursors)
	if cursor != nil {
		if cursors == nil {
			cursors = make(map[string]json.RawMessage)
		}
		cursors[step] = cursor
	}
	if len(cursors) > 0 {
		metadataUpdates[rivercommon.MetadataKeyResumableCursor] = cursors
	}

	metadataUpdatesBytes, err := json.Marshal(metadataUpdates)
	if err != nil {
		return nil, err
	}

	updatedJob, err := client.Driver().UnwrapExecutor(tx).JobUpdate(ctx, &riverdriver.JobUpdateParams{
		ID:              job.ID,
		MetadataDoMerge: true,
		Metadata:        metadataUpdatesBytes,
		Schema:          client.config.Schema,
	})
	if err != nil {
		if errors.Is(err, rivertype.ErrNotFound) {
			if _, isInsideTestWorker := ctx.Value(execution.ContextKeyInsideTestWorker{}).(bool); isInsideTestWorker {
				panic("to use ResumableSetStepTx or ResumableSetStepCursorTx in a rivertest.Worker, the job must be inserted into the database first")
			}
		}

		return nil, err
	}

	state.PersistedInTx = true

	result := &Job[TArgs]{JobRow: updatedJob}
	if err := json.Unmarshal(result.EncodedArgs, &result.Args); err != nil {
		return nil, err
	}

	return result, nil
}
