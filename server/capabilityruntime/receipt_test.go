package capabilityruntime

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestReceiptV1BindsInvocationAndRawResult(t *testing.T) {
	t.Parallel()

	invocation := testInvocationV1(t)
	result, err := (testAdapterV1{}).Invoke(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.ValidateFor(invocation); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeReceiptV1(result.Receipt, invocation)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReceiptV1(encoded, invocation)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeReceiptV1(decoded, invocation)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("receipt round trip: err=%v got=%s want=%s", err, reencoded, encoded)
	}

	tamperedOutput := result
	tamperedOutput.Output = []byte(`{"ok":false}`)
	if err := tamperedOutput.ValidateFor(invocation); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("tampered output error = %v, want ErrInvalidContract", err)
	}

	otherInput := testInvocationInputV1()
	otherInput.Principal.TenantID++
	other, err := NewInvocationV1(otherInput)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.ValidateFor(other); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("cross-tenant receipt error = %v, want ErrInvalidContract", err)
	}
}

func TestReceiptV1EveryFieldIsDigestBound(t *testing.T) {
	t.Parallel()

	invocation := testInvocationV1(t)
	receipt, err := NewReceiptV1(
		invocation, ReceiptStatusSucceeded, 1, "application/json", []byte(`{}`), "", false,
	)
	if err != nil {
		t.Fatal(err)
	}
	otherDigest := stringsOf("b", 64)
	mutations := map[string]func(*ReceiptV1){
		"invocation":     func(v *ReceiptV1) { v.InvocationDigest = otherDigest },
		"idempotency":    func(v *ReceiptV1) { v.IdempotencyDigest = otherDigest },
		"attempt":        func(v *ReceiptV1) { v.Attempt++ },
		"status":         func(v *ReceiptV1) { v.Status = ReceiptStatusFailed },
		"result digest":  func(v *ReceiptV1) { v.Result.Digest = otherDigest },
		"result size":    func(v *ReceiptV1) { v.Result.SizeBytes++ },
		"media type":     func(v *ReceiptV1) { v.Result.MediaType = "text/plain" },
		"error class":    func(v *ReceiptV1) { v.ErrorClass = "timeout" },
		"retryable":      func(v *ReceiptV1) { v.Retryable = true },
		"receipt digest": func(v *ReceiptV1) { v.ReceiptDigest = otherDigest },
	}
	for name, mutate := range mutations {
		candidate := receipt
		mutate(&candidate)
		if err := candidate.ValidateFor(invocation); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("%s mutation validated: %v", name, err)
		}
	}
}

func TestReceiptV1FailureIsControlledAndOutputFree(t *testing.T) {
	t.Parallel()

	invocation := testInvocationV1(t)
	receipt, err := NewReceiptV1(
		invocation, ReceiptStatusFailed, 2, "", nil, "upstream_timeout", true,
	)
	if err != nil {
		t.Fatal(err)
	}
	result := AdapterResultV1{Receipt: receipt}
	if err := result.ValidateFor(invocation); err != nil {
		t.Fatal(err)
	}
	result.Output = []byte("provider error containing a secret")
	if err := result.ValidateFor(invocation); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("failed output error = %v, want ErrInvalidContract", err)
	}
	if _, err := NewReceiptV1(
		invocation, ReceiptStatusRejected, 1, "", nil, "policy_denied", true,
	); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("retryable rejection error = %v, want ErrInvalidContract", err)
	}
}

func stringsOf(value string, count int) string {
	var buffer bytes.Buffer
	for range count {
		buffer.WriteString(value)
	}
	return buffer.String()
}
