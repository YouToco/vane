package runtimepolicy

import (
	"encoding/json"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const maxEncodedPolicyBytes = 256 << 10

type bundleV1Wire BundleV1
type capabilityCatalogV1Wire CapabilityCatalogV1
type toolPolicyV1Wire ToolPolicyV1
type promptPolicyV1Wire PromptPolicyV1
type modelPolicyV1Wire ModelPolicyV1
type quotaPolicyV1Wire QuotaPolicyV1

// MarshalJSON validates and canonically orders the V1 bundle before encoding.
func (b BundleV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeBundleV1(b)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(bundleV1Wire(normalized))
}

// UnmarshalJSON rejects duplicate and unknown fields and all schema versions
// other than V1.
func (b *BundleV1) UnmarshalJSON(payload []byte) error {
	if b == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("bundle json size is invalid")
	}
	var wire bundleV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("bundle json is invalid")
	}
	normalized, err := normalizeBundleV1(BundleV1(wire))
	if err != nil {
		return err
	}
	*b = normalized
	return nil
}

// MarshalJSON validates and canonically orders the V1 capability catalog.
func (p CapabilityCatalogV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeCapabilityCatalogV1(p)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(capabilityCatalogV1Wire(normalized))
}

// UnmarshalJSON strictly decodes a V1 capability catalog.
func (p *CapabilityCatalogV1) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("capability catalog json size is invalid")
	}
	var wire capabilityCatalogV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("capability catalog json is invalid")
	}
	normalized, err := normalizeCapabilityCatalogV1(CapabilityCatalogV1(wire))
	if err != nil {
		return err
	}
	*p = normalized
	return nil
}

// MarshalJSON validates the compiled V1 tool policy before encoding.
func (p ToolPolicyV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeToolPolicyV1(p)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(toolPolicyV1Wire(normalized))
}

// UnmarshalJSON strictly decodes a compiled V1 tool policy.
func (p *ToolPolicyV1) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("tool policy json size is invalid")
	}
	var wire toolPolicyV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("tool policy json is invalid")
	}
	normalized, err := normalizeToolPolicyV1(ToolPolicyV1(wire))
	if err != nil {
		return err
	}
	*p = normalized
	return nil
}

// MarshalJSON validates the V1 prompt policy before encoding.
func (p PromptPolicyV1) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return marshalPolicyJSON(promptPolicyV1Wire(p))
}

// UnmarshalJSON strictly decodes a V1 prompt policy.
func (p *PromptPolicyV1) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("prompt policy json size is invalid")
	}
	var wire promptPolicyV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("prompt policy json is invalid")
	}
	decoded := PromptPolicyV1(wire)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*p = decoded
	return nil
}

// MarshalJSON validates and canonically orders the V1 model policy.
func (p ModelPolicyV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeModelPolicyV1(p)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(modelPolicyV1Wire(normalized))
}

// UnmarshalJSON strictly decodes a V1 model policy.
func (p *ModelPolicyV1) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("model policy json size is invalid")
	}
	var wire modelPolicyV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("model policy json is invalid")
	}
	normalized, err := normalizeModelPolicyV1(ModelPolicyV1(wire))
	if err != nil {
		return err
	}
	*p = normalized
	return nil
}

// MarshalJSON validates and canonically orders the V1 quota policy.
func (p QuotaPolicyV1) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeQuotaPolicyV1(p)
	if err != nil {
		return nil, err
	}
	return marshalPolicyJSON(quotaPolicyV1Wire(normalized))
}

// UnmarshalJSON strictly decodes a V1 quota policy.
func (p *QuotaPolicyV1) UnmarshalJSON(payload []byte) error {
	if p == nil || len(payload) == 0 || len(payload) > maxEncodedPolicyBytes {
		return invalidPolicy("quota policy json size is invalid")
	}
	var wire quotaPolicyV1Wire
	if err := strictjson.DecodeExact(payload, &wire); err != nil {
		return invalidPolicy("quota policy json is invalid")
	}
	normalized, err := normalizeQuotaPolicyV1(QuotaPolicyV1(wire))
	if err != nil {
		return err
	}
	*p = normalized
	return nil
}

// EncodeBundleV1 returns validated canonical JSON for the full bundle.
func EncodeBundleV1(bundle BundleV1) ([]byte, error) { return json.Marshal(bundle) }

// DecodeBundleV1 strictly decodes a full V1 bundle.
func DecodeBundleV1(payload []byte) (BundleV1, error) {
	if !validEncodedPolicySize(payload) {
		return BundleV1{}, invalidPolicy("bundle json size is invalid")
	}
	var bundle BundleV1
	if err := json.Unmarshal(payload, &bundle); err != nil {
		return BundleV1{}, invalidPolicy("bundle json is invalid")
	}
	return bundle, nil
}

// EncodeCapabilityCatalogV1 returns validated canonical JSON.
func EncodeCapabilityCatalogV1(policy CapabilityCatalogV1) ([]byte, error) {
	return json.Marshal(policy)
}

// DecodeCapabilityCatalogV1 strictly decodes a V1 capability catalog.
func DecodeCapabilityCatalogV1(payload []byte) (CapabilityCatalogV1, error) {
	if !validEncodedPolicySize(payload) {
		return CapabilityCatalogV1{}, invalidPolicy("capability catalog json size is invalid")
	}
	var policy CapabilityCatalogV1
	if err := json.Unmarshal(payload, &policy); err != nil {
		return CapabilityCatalogV1{}, invalidPolicy("capability catalog json is invalid")
	}
	return policy, nil
}

// EncodeToolPolicyV1 returns validated canonical JSON.
func EncodeToolPolicyV1(policy ToolPolicyV1) ([]byte, error) { return json.Marshal(policy) }

// DecodeToolPolicyV1 strictly decodes a compiled V1 tool policy.
func DecodeToolPolicyV1(payload []byte) (ToolPolicyV1, error) {
	if !validEncodedPolicySize(payload) {
		return ToolPolicyV1{}, invalidPolicy("tool policy json size is invalid")
	}
	var policy ToolPolicyV1
	if err := json.Unmarshal(payload, &policy); err != nil {
		return ToolPolicyV1{}, invalidPolicy("tool policy json is invalid")
	}
	return policy, nil
}

// EncodePromptPolicyV1 returns validated canonical JSON.
func EncodePromptPolicyV1(policy PromptPolicyV1) ([]byte, error) { return json.Marshal(policy) }

// DecodePromptPolicyV1 strictly decodes a V1 prompt policy.
func DecodePromptPolicyV1(payload []byte) (PromptPolicyV1, error) {
	if !validEncodedPolicySize(payload) {
		return PromptPolicyV1{}, invalidPolicy("prompt policy json size is invalid")
	}
	var policy PromptPolicyV1
	if err := json.Unmarshal(payload, &policy); err != nil {
		return PromptPolicyV1{}, invalidPolicy("prompt policy json is invalid")
	}
	return policy, nil
}

// EncodeModelPolicyV1 returns validated canonical JSON.
func EncodeModelPolicyV1(policy ModelPolicyV1) ([]byte, error) { return json.Marshal(policy) }

// DecodeModelPolicyV1 strictly decodes a V1 model policy.
func DecodeModelPolicyV1(payload []byte) (ModelPolicyV1, error) {
	if !validEncodedPolicySize(payload) {
		return ModelPolicyV1{}, invalidPolicy("model policy json size is invalid")
	}
	var policy ModelPolicyV1
	if err := json.Unmarshal(payload, &policy); err != nil {
		return ModelPolicyV1{}, invalidPolicy("model policy json is invalid")
	}
	return policy, nil
}

// EncodeQuotaPolicyV1 returns validated canonical JSON.
func EncodeQuotaPolicyV1(policy QuotaPolicyV1) ([]byte, error) { return json.Marshal(policy) }

// DecodeQuotaPolicyV1 strictly decodes a V1 quota policy.
func DecodeQuotaPolicyV1(payload []byte) (QuotaPolicyV1, error) {
	if !validEncodedPolicySize(payload) {
		return QuotaPolicyV1{}, invalidPolicy("quota policy json size is invalid")
	}
	var policy QuotaPolicyV1
	if err := json.Unmarshal(payload, &policy); err != nil {
		return QuotaPolicyV1{}, invalidPolicy("quota policy json is invalid")
	}
	return policy, nil
}

func validEncodedPolicySize(payload []byte) bool {
	return len(payload) > 0 && len(payload) <= maxEncodedPolicyBytes
}

func marshalPolicyJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxEncodedPolicyBytes {
		return nil, invalidPolicy("encoded policy exceeds size limit")
	}
	return payload, nil
}
