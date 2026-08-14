package runcontext

import "github.com/YouToco/vane/server/types"

// ToolCandidateV1 carries observation provenance through the in-memory and
// Temporal pipeline. It is a wire wrapper, not a persisted or user-managed
// ToolInvocation entity.
type ToolCandidateV1 struct {
	InvocationDigest string            `json:"invocation_digest"`
	Item             types.ContentItem `json:"item"`
}

type ToolScoredCandidateV1 struct {
	InvocationDigest string           `json:"invocation_digest"`
	Scored           types.ScoredItem `json:"scored"`
}
