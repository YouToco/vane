package capabilityruntime

type ReceiptStatusV1 string

const (
	ReceiptStatusSucceeded ReceiptStatusV1 = "succeeded"
	ReceiptStatusFailed    ReceiptStatusV1 = "failed"
	ReceiptStatusRejected  ReceiptStatusV1 = "rejected"
	ReceiptStatusAmbiguous ReceiptStatusV1 = "ambiguous"
)

type ResultRefV1 struct {
	Digest    string `json:"digest"`
	SizeBytes int64  `json:"size_bytes"`
	MediaType string `json:"media_type"`
}

// ReceiptV1 is a deterministic result receipt. It carries no provider error,
// credential, or output bytes; ErrorClass must be a controlled local label.
type ReceiptV1 struct {
	SchemaVersion     string          `json:"schema_version"`
	InvocationDigest  string          `json:"invocation_digest"`
	IdempotencyDigest string          `json:"idempotency_digest"`
	Attempt           int64           `json:"attempt"`
	Status            ReceiptStatusV1 `json:"status"`
	Result            ResultRefV1     `json:"result"`
	ErrorClass        string          `json:"error_class"`
	Retryable         bool            `json:"retryable"`
	ReceiptDigest     string          `json:"receipt_digest"`
}

// AdapterResultV1 keeps bytes transient while proving they match the durable
// receipt. Persistence and transport remain responsibilities of later layers.
type AdapterResultV1 struct {
	Receipt ReceiptV1
	Output  []byte
}

func NewReceiptV1(
	invocation InvocationV1,
	status ReceiptStatusV1,
	attempt int64,
	mediaType string,
	output []byte,
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
		receipt.Result = ResultRefV1{
			Digest: sha256Bytes(output), SizeBytes: int64(len(output)), MediaType: mediaType,
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
			!validMediaType(r.Result.MediaType) || r.ErrorClass != "" || r.Retryable {
			return invalid("successful receipt is invalid")
		}
	case ReceiptStatusFailed, ReceiptStatusRejected, ReceiptStatusAmbiguous:
		if r.Result != (ResultRefV1{}) || !validErrorClass(r.ErrorClass) {
			return invalid("unsuccessful receipt is invalid")
		}
		if r.Status == ReceiptStatusRejected && r.Retryable {
			return invalid("rejected receipt cannot be retryable")
		}
	default:
		return invalid("receipt status is invalid")
	}
	return nil
}

func (r ReceiptV1) expectedDigest() (string, error) {
	return digestJSON(struct {
		SchemaVersion     string          `json:"schema_version"`
		InvocationDigest  string          `json:"invocation_digest"`
		IdempotencyDigest string          `json:"idempotency_digest"`
		Attempt           int64           `json:"attempt"`
		Status            ReceiptStatusV1 `json:"status"`
		Result            ResultRefV1     `json:"result"`
		ErrorClass        string          `json:"error_class"`
		Retryable         bool            `json:"retryable"`
	}{r.SchemaVersion, r.InvocationDigest, r.IdempotencyDigest, r.Attempt,
		r.Status, r.Result, r.ErrorClass, r.Retryable})
}

func (r AdapterResultV1) ValidateFor(invocation InvocationV1) error {
	if err := r.Receipt.ValidateFor(invocation); err != nil {
		return err
	}
	if r.Receipt.Status != ReceiptStatusSucceeded {
		if len(r.Output) != 0 {
			return invalid("unsuccessful result contains output")
		}
		return nil
	}
	if r.Receipt.Result.SizeBytes != int64(len(r.Output)) ||
		r.Receipt.Result.Digest != sha256Bytes(r.Output) {
		return invalid("output differs from result receipt")
	}
	return nil
}

func sha256Bytes(value []byte) string {
	return rawSHA256(value)
}
