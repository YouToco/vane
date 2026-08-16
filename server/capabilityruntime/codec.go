package capabilityruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const maxEncodedContractBytes = 512 << 10

func EncodeInvocationV1(invocation InvocationV1) ([]byte, error) {
	if err := invocation.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(invocation)
}

func DecodeInvocationV1(payload []byte) (InvocationV1, error) {
	if len(payload) == 0 || len(payload) > maxEncodedContractBytes {
		return InvocationV1{}, invalid("invocation JSON size is invalid")
	}
	var invocation InvocationV1
	if err := strictjson.DecodeExact(payload, &invocation); err != nil {
		return InvocationV1{}, invalid("invocation JSON is invalid")
	}
	if err := invocation.Validate(); err != nil {
		return InvocationV1{}, err
	}
	canonical, err := json.Marshal(invocation)
	if err != nil || string(canonical) != string(payload) {
		return InvocationV1{}, invalid("invocation JSON is not canonical")
	}
	return invocation, nil
}

func EncodePolicyV1(policy PolicyV1) ([]byte, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(policy)
}

func DecodePolicyV1(payload []byte) (PolicyV1, error) {
	if len(payload) == 0 || len(payload) > maxEncodedContractBytes {
		return PolicyV1{}, invalid("policy JSON size is invalid")
	}
	var policy PolicyV1
	if err := strictjson.DecodeExact(payload, &policy); err != nil {
		return PolicyV1{}, invalid("policy JSON is invalid")
	}
	if err := policy.Validate(); err != nil {
		return PolicyV1{}, err
	}
	canonical, err := json.Marshal(policy)
	if err != nil || string(canonical) != string(payload) {
		return PolicyV1{}, invalid("policy JSON is not canonical")
	}
	return policy, nil
}

func EncodeReceiptV1(receipt ReceiptV1, invocation InvocationV1) ([]byte, error) {
	if err := receipt.ValidateFor(invocation); err != nil {
		return nil, err
	}
	return json.Marshal(receipt)
}

func DecodeReceiptV1(payload []byte, invocation InvocationV1) (ReceiptV1, error) {
	if len(payload) == 0 || len(payload) > maxEncodedContractBytes {
		return ReceiptV1{}, invalid("receipt JSON size is invalid")
	}
	var receipt ReceiptV1
	if err := strictjson.DecodeExact(payload, &receipt); err != nil {
		return ReceiptV1{}, invalid("receipt JSON is invalid")
	}
	if err := receipt.ValidateFor(invocation); err != nil {
		return ReceiptV1{}, err
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || string(canonical) != string(payload) {
		return ReceiptV1{}, invalid("receipt JSON is not canonical")
	}
	return receipt, nil
}

func rawSHA256(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
