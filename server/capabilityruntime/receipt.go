package capabilityruntime

import "bytes"

type ReceiptStatusV1 string

const (
	ReceiptStatusSucceeded      ReceiptStatusV1 = "succeeded"
	ReceiptStatusDefiniteFailed ReceiptStatusV1 = "definite_failed"
	ReceiptStatusRejected       ReceiptStatusV1 = "rejected"
	ReceiptStatusAmbiguous      ReceiptStatusV1 = "ambiguous"
)

type ResultEnvelopeV1 struct {
	Digest           string `json:"digest"`
	SizeBytes        int64  `json:"size_bytes"`
	MediaType        string `json:"media_type"`
	SanitizedPayload []byte `json:"sanitized_payload"`
}

// ReceiptV1 is a deterministic, replayable result envelope. Successful
// receipts persist bounded sanitized bytes; provider errors and credentials
// never enter it, and ErrorClass must be a controlled local label.
type ReceiptV1 struct {
	SchemaVersion     string           `json:"schema_version"`
	InvocationDigest  string           `json:"invocation_digest"`
	IdempotencyDigest string           `json:"idempotency_digest"`
	Attempt           int64            `json:"attempt"`
	Status            ReceiptStatusV1  `json:"status"`
	Result            ResultEnvelopeV1 `json:"result"`
	ErrorClass        string           `json:"error_class"`
	Retryable         bool             `json:"retryable"`
	ReceiptDigest     string           `json:"receipt_digest"`
}

// AdapterResultV1 proves the bytes returned by an adapter exactly match the
// durable sanitized response embedded in ReceiptV1.
type AdapterResultV1 struct {
	Receipt         ReceiptV1
	SanitizedOutput []byte
}

func NewReceiptV1(
	invocation InvocationV1,
	status ReceiptStatusV1,
	attempt int64,
	mediaType string,
	sanitizedOutput []byte,
	errorClass string,
	retryable bool,
) (ReceiptV1, error) {
	if err := invocation.Validate(); err != nil {
		return ReceiptV1{}, err
	}
	receipt := ReceiptV1{
		SchemaVersion:     ReceiptSchemaVersionV1,
		InvocationDigest:  invocation.InvocationDigest,
		IdempotencyDigest: invocation.IdempotencyDigest,
		Attempt:           attempt, Status: status, ErrorClass: errorClass, Retryable: retryable,
	}
	if status == ReceiptStatusSucceeded {
		receipt.Result = ResultEnvelopeV1{
			Digest: sha256Bytes(sanitizedOutput), SizeBytes: int64(len(sanitizedOutput)), MediaType: mediaType,
			SanitizedPayload: append([]byte{}, sanitizedOutput...),
		}
	}
	if err := receipt.validateFields(); err != nil {
		return ReceiptV1{}, err
	}
	var err error
	receipt.ReceiptDigest, err = receipt.expectedDigest()
	if err != nil {
		return ReceiptV1{}, invalid("receipt cannot be encoded")
	}
	if err := receipt.ValidateFor(invocation); err != nil {
		return ReceiptV1{}, err
	}
	return receipt, nil
}

func (r ReceiptV1) ValidateFor(invocation InvocationV1) error {
	if err := invocation.Validate(); err != nil {
		return err
	}
	if err := r.validateFields(); err != nil {
		return err
	}
	if r.InvocationDigest != invocation.InvocationDigest ||
		r.IdempotencyDigest != invocation.IdempotencyDigest {
		return invalid("receipt belongs to another invocation")
	}
	if r.Attempt > invocation.Policy.MaxAttempts {
		return invalid("receipt exceeds frozen attempt budget")
	}
	if r.Status == ReceiptStatusSucceeded &&
		r.Result.SizeBytes > invocation.Policy.MaxOutputBytes {
		return invalid("result exceeds frozen output budget")
	}
	expected, err := r.expectedDigest()
	if err != nil || r.ReceiptDigest != expected {
		return invalid("receipt digest differs")
	}
	return nil
}

func (r ReceiptV1) validateFields() error {
	if r.SchemaVersion != ReceiptSchemaVersionV1 || r.Attempt <= 0 ||
		!validSHA256(r.InvocationDigest) || !validSHA256(r.IdempotencyDigest) {
		return invalid("receipt fields are invalid")
	}
	switch r.Status {
	case ReceiptStatusSucceeded:
		if !validSHA256(r.Result.Digest) || r.Result.SizeBytes < 0 ||
			!validMediaType(r.Result.MediaType) ||
			r.Result.SanitizedPayload == nil ||
			r.Result.SizeBytes != int64(len(r.Result.SanitizedPayload)) ||
			r.Result.Digest != sha256Bytes(r.Result.SanitizedPayload) ||
			r.ErrorClass != "" || r.Retryable {
			return invalid("successful receipt is invalid")
		}
	// DefiniteFailed means the adapter proved that execution did not cross the
	// effect boundary; only that failure class may ever be marked retryable.
	case ReceiptStatusDefiniteFailed, ReceiptStatusRejected, ReceiptStatusAmbiguous:
		if !r.Result.empty() || !validErrorClass(r.ErrorClass) {
			return invalid("unsuccessful receipt is invalid")
		}
		// Ambiguous means the effect may already have happened. Automatic retry
		// would risk duplicating a remote side effect.
		if r.Status != ReceiptStatusDefiniteFailed && r.Retryable {
			return invalid("non-definite receipt cannot be retryable")
		}
	default:
		return invalid("receipt status is invalid")
	}
	return nil
}

func (r ReceiptV1) expectedDigest() (string, error) {
	return digestJSON(struct {
		SchemaVersion     string           `json:"schema_version"`
		InvocationDigest  string           `json:"invocation_digest"`
		IdempotencyDigest string           `json:"idempotency_digest"`
		Attempt           int64            `json:"attempt"`
		Status            ReceiptStatusV1  `json:"status"`
		Result            ResultEnvelopeV1 `json:"result"`
		ErrorClass        string           `json:"error_class"`
		Retryable         bool             `json:"retryable"`
	}{r.SchemaVersion, r.InvocationDigest, r.IdempotencyDigest, r.Attempt,
		r.Status, r.Result, r.ErrorClass, r.Retryable})
}

func (r AdapterResultV1) ValidateFor(invocation InvocationV1) error {
	if err := r.Receipt.ValidateFor(invocation); err != nil {
		return err
	}
	if r.Receipt.Status != ReceiptStatusSucceeded {
		if len(r.SanitizedOutput) != 0 {
			return invalid("unsuccessful result contains output")
		}
		return nil
	}
	if r.Receipt.Result.SizeBytes != int64(len(r.SanitizedOutput)) ||
		r.Receipt.Result.Digest != sha256Bytes(r.SanitizedOutput) ||
		!bytes.Equal(r.Receipt.Result.SanitizedPayload, r.SanitizedOutput) {
		return invalid("output differs from result receipt")
	}
	return nil
}

func (r ResultEnvelopeV1) empty() bool {
	return r.Digest == "" && r.SizeBytes == 0 && r.MediaType == "" &&
		r.SanitizedPayload == nil
}

func sha256Bytes(value []byte) string {
	return rawSHA256(value)
}
