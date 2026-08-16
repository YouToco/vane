package skillruntime

import (
	"bytes"
	"encoding/json"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const maxEncodedSkillContractBytesV1 = 256 << 10

func EncodeSkillRefV1(ref SkillRefV1) ([]byte, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(ref)
}

func DecodeSkillRefV1(payload []byte) (SkillRefV1, error) {
	var value SkillRefV1
	if err := decodeCanonical(payload, &value); err != nil {
		return SkillRefV1{}, err
	}
	if err := value.Validate(); err != nil {
		return SkillRefV1{}, err
	}
	return value, nil
}

func EncodeSkillResourceHandleV1(handle SkillResourceHandleV1) ([]byte, error) {
	if err := handle.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(handle)
}

func DecodeSkillResourceHandleV1(payload []byte) (SkillResourceHandleV1, error) {
	var value SkillResourceHandleV1
	if err := decodeCanonical(payload, &value); err != nil {
		return SkillResourceHandleV1{}, err
	}
	if err := value.Validate(); err != nil {
		return SkillResourceHandleV1{}, err
	}
	return value, nil
}

func EncodeSkillResourceChunkV1(chunk SkillResourceChunkV1, handle SkillResourceHandleV1) ([]byte, error) {
	if err := chunk.Validate(handle); err != nil {
		return nil, err
	}
	return json.Marshal(chunk)
}

func DecodeSkillResourceChunkV1(payload []byte, handle SkillResourceHandleV1) (SkillResourceChunkV1, error) {
	var value SkillResourceChunkV1
	if err := decodeCanonical(payload, &value); err != nil {
		return SkillResourceChunkV1{}, err
	}
	if err := value.Validate(handle); err != nil {
		return SkillResourceChunkV1{}, err
	}
	return value, nil
}

func decodeCanonical(payload []byte, target any) error {
	if len(payload) == 0 || len(payload) > maxEncodedSkillContractBytesV1 {
		return invalid("canonical JSON size")
	}
	if err := strictjson.DecodeExact(payload, target); err != nil {
		return invalid("canonical JSON")
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(payload, canonical) {
		return invalid("canonical JSON encoding")
	}
	return nil
}
