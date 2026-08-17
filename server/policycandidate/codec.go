package policycandidate

import (
	"encoding/json"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const maxEncodedContractBytes = 32 << 20

func EncodeCandidateV1(candidate CandidateV1) ([]byte, error) {
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	return encode(candidate)
}

func DecodeCandidateV1(payload []byte) (CandidateV1, error) {
	var candidate CandidateV1
	if err := decode(payload, &candidate); err != nil {
		return CandidateV1{}, err
	}
	if err := candidate.Validate(); err != nil {
		return CandidateV1{}, err
	}
	return candidate, nil
}

func EncodeEvaluationDatasetV1(dataset EvaluationDatasetV1) ([]byte, error) {
	if err := dataset.Validate(); err != nil {
		return nil, err
	}
	return encode(dataset)
}

func DecodeEvaluationDatasetV1(payload []byte) (EvaluationDatasetV1, error) {
	var dataset EvaluationDatasetV1
	if err := decode(payload, &dataset); err != nil {
		return EvaluationDatasetV1{}, err
	}
	if err := dataset.Validate(); err != nil {
		return EvaluationDatasetV1{}, err
	}
	return dataset, nil
}

func EncodeOfflineEvalInputV1(
	input OfflineEvalInputV1,
	candidate CandidateV1,
) ([]byte, error) {
	if err := input.ValidateFor(candidate); err != nil {
		return nil, err
	}
	return encode(input)
}

func DecodeOfflineEvalInputV1(
	payload []byte,
	candidate CandidateV1,
) (OfflineEvalInputV1, error) {
	var input OfflineEvalInputV1
	if err := decode(payload, &input); err != nil {
		return OfflineEvalInputV1{}, err
	}
	if err := input.ValidateFor(candidate); err != nil {
		return OfflineEvalInputV1{}, err
	}
	return input, nil
}

func EncodeOfflineEvalResultV1(
	result OfflineEvalResultV1,
	input OfflineEvalInputV1,
	candidate CandidateV1,
) ([]byte, error) {
	if err := result.ValidateFor(input, candidate); err != nil {
		return nil, err
	}
	return encode(result)
}

func DecodeOfflineEvalResultV1(
	payload []byte,
	input OfflineEvalInputV1,
	candidate CandidateV1,
) (OfflineEvalResultV1, error) {
	var result OfflineEvalResultV1
	if err := decode(payload, &result); err != nil {
		return OfflineEvalResultV1{}, err
	}
	if err := result.ValidateFor(input, candidate); err != nil {
		return OfflineEvalResultV1{}, err
	}
	return result, nil
}

func encode(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil || len(payload) == 0 || len(payload) > maxEncodedContractBytes {
		return nil, invalid("encoded contract size is invalid")
	}
	return payload, nil
}

func decode(payload []byte, destination any) error {
	if len(payload) == 0 || len(payload) > maxEncodedContractBytes {
		return invalid("contract JSON size is invalid")
	}
	if err := strictjson.DecodeExact(payload, destination); err != nil {
		return invalid("contract JSON is invalid")
	}
	canonical, err := json.Marshal(destination)
	if err != nil || string(canonical) != string(payload) {
		return invalid("contract JSON is not canonical")
	}
	return nil
}
