package capabilityruntime

import "context"

// Adapter executes exactly one already-authorized, validated invocation.
// Implementations must advertise one closed kind and must return output bound
// to a ReceiptV1. Registry selection and authorization are intentionally out
// of scope for this data-only foundation.
type Adapter interface {
	Kind() CapabilityKind
	Invoke(context.Context, InvocationV1) (AdapterResultV1, error)
}
