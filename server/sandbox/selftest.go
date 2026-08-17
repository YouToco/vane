package sandbox

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	GateCapabilityID = "vane.firecracker.release-gate"
	guestReceiptMark = "VANE_FIRECRACKER_RECEIPT="
)

type GuestReceipt struct {
	Schema          string `json:"schema"`
	InvocationID    string `json:"invocation_id"`
	RequestSHA256   string `json:"request_sha256"`
	InputSHA256     string `json:"input_sha256"`
	ResponseSHA256  string `json:"response_sha256"`
	GuestUID        int    `json:"guest_uid"`
	GuestGID        int    `json:"guest_gid"`
	OnlyLoopback    bool   `json:"only_loopback"`
	NoDefaultRoute  bool   `json:"no_default_route"`
	MMDSUnavailable bool   `json:"mmds_unavailable"`
}

func parseGuestReceipt(output []byte, request Request) ([]byte, error) {
	var payload []byte
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte(guestReceiptMark)) {
			if payload != nil {
				return nil, errors.New("guest returned duplicate Firecracker receipts")
			}
			payload = bytes.TrimSuffix(line[len(guestReceiptMark):], []byte{'\r'})
		}
	}
	if len(payload) == 0 || len(payload) > 4096 {
		return nil, errors.New("guest Firecracker receipt is missing or oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var receipt GuestReceipt
	if err := decoder.Decode(&receipt); err != nil || decoder.InputOffset() != int64(len(payload)) {
		return nil, errors.New("guest Firecracker receipt is not exact JSON")
	}
	inputDigest := sha256.Sum256(request.Input)
	wantResponse := sha256.Sum256(append([]byte("vane-firecracker-self-test/v1\x00"), request.Input...))
	if receipt.Schema != "vane.firecracker-guest-receipt/v1" ||
		receipt.InvocationID != request.InvocationID ||
		receipt.RequestSHA256 != request.RequestDigest ||
		receipt.InputSHA256 != hex.EncodeToString(inputDigest[:]) ||
		receipt.ResponseSHA256 != hex.EncodeToString(wantResponse[:]) ||
		receipt.GuestUID != request.Policy.GuestUID || receipt.GuestGID != request.Policy.GuestGID ||
		!receipt.OnlyLoopback || !receipt.NoDefaultRoute || !receipt.MMDSUnavailable {
		return nil, fmt.Errorf("guest Firecracker receipt failed the closed self-test contract")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonical, payload) {
		return nil, errors.New("guest Firecracker receipt is not canonical")
	}
	return canonical, nil
}
