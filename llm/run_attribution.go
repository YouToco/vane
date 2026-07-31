package llm

import "context"

type runSnapshotAttributionKey struct{}

// WithRunSnapshotAttribution binds calls below this context to one immutable
// task-run snapshot. The value is local accounting metadata and is never sent
// to the model provider.
func WithRunSnapshotAttribution(ctx context.Context, snapshotID int64) context.Context {
	if snapshotID <= 0 {
		return ctx
	}
	return context.WithValue(ctx, runSnapshotAttributionKey{}, snapshotID)
}

func runSnapshotAttribution(ctx context.Context) *int64 {
	id, ok := ctx.Value(runSnapshotAttributionKey{}).(int64)
	if !ok || id <= 0 {
		return nil
	}
	return &id
}
