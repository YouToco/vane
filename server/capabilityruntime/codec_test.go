package capabilityruntime

import (
	"bytes"
	"errors"
	"testing"
)

func TestPolicyV1StrictCanonicalCodec(t *testing.T) {
	t.Parallel()

	policy := testInvocationV1(t).Policy
	encoded, err := EncodePolicyV1(policy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePolicyV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodePolicyV1(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("policy round trip: err=%v got=%s want=%s", err, reencoded, encoded)
	}

	cases := map[string][]byte{
		"unknown": bytes.Replace(encoded, []byte(`"read_only":true`),
			[]byte(`"unknown":true,"read_only":true`), 1),
		"duplicate": bytes.Replace(encoded, []byte(`"read_only":true`),
			[]byte(`"read_only":false,"read_only":true`), 1),
		"missing":    bytes.Replace(encoded, []byte(`"read_only":true,`), nil, 1),
		"whitespace": append([]byte("\n"), encoded...),
		"trailing":   append(bytes.Clone(encoded), []byte("null")...),
	}
	for name, payload := range cases {
		if _, err := DecodePolicyV1(payload); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("%s: DecodePolicyV1() error = %v, want ErrInvalidContract", name, err)
		}
	}
}

func TestReceiptV1StrictCanonicalCodec(t *testing.T) {
	t.Parallel()

	invocation := testInvocationV1(t)
	receipt, err := NewReceiptV1(
		invocation, ReceiptStatusSucceeded, 1, "application/json", []byte(`{}`), "", false,
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeReceiptV1(receipt, invocation)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown": bytes.Replace(encoded, []byte(`"attempt":1`),
			[]byte(`"unknown":true,"attempt":1`), 1),
		"duplicate": bytes.Replace(encoded, []byte(`"attempt":1`),
			[]byte(`"attempt":2,"attempt":1`), 1),
		"missing":    bytes.Replace(encoded, []byte(`"attempt":1,`), nil, 1),
		"whitespace": append([]byte(" "), encoded...),
		"trailing":   append(bytes.Clone(encoded), []byte(`{}`)...),
	}
	for name, payload := range cases {
		if _, err := DecodeReceiptV1(payload, invocation); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("%s: DecodeReceiptV1() error = %v, want ErrInvalidContract", name, err)
		}
	}
}

func TestInvocationV1EnforcesFrozenByteBudgets(t *testing.T) {
	t.Parallel()

	input := testInvocationInputV1()
	input.Policy.MaxInputBytes = 1
	if _, err := NewInvocationV1(input); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("input budget error = %v, want ErrInvalidContract", err)
	}

	input = testInvocationInputV1()
	input.Policy.MaxOutputBytes = 1
	invocation, err := NewInvocationV1(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewReceiptV1(
		invocation, ReceiptStatusSucceeded, 1, "application/octet-stream", []byte("12"), "", false,
	); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("output budget error = %v, want ErrInvalidContract", err)
	}

	input = testInvocationInputV1()
	input.Policy.MaxAttempts = 1
	invocation, err = NewInvocationV1(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewReceiptV1(
		invocation, ReceiptStatusFailed, 2, "", nil, "upstream_timeout", false,
	); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("attempt budget error = %v, want ErrInvalidContract", err)
	}
}
