package pusheffect

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func fixture() Prepared {
	return Prepared{
		ID: "effect-1", TenantID: 1, UserID: 2, TaskID: "task-1",
		RunSnapshotID: 3, RunID: "run-1", StepID: "push",
		ChunkIndex: 0, ChunkCount: 1, BatchID: 4,
		DeliveryIDs: []int64{5, 6}, Provider: "feishu",
		AppIdentity: "app-fingerprint", ProviderChatID: "oc_owner_p2p",
		Target:       "ou_target",
		Card:         []byte(`{"type":"card","body":"exact bytes"}`),
		ProviderUUID: uuid.MustParse("2f790ab2-0622-4df8-8f93-6079a3a0f94f").String(),
		IdempotencyExpiresAt: time.Date(
			2026, 7, 24, 12, 0, 0, 123000000, time.UTC),
	}
}

func TestCanonicalizeRoundTripAndDefensiveCopies(t *testing.T) {
	input := fixture()
	canonical, err := Canonicalize(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(canonical.Payload(), canonical.Digest())
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.Prepared()
	if !bytes.Equal(got.Card, input.Card) ||
		!bytes.Equal(canonical.Payload(), decoded.Payload()) ||
		canonical.Digest() != decoded.Digest() {
		t.Fatalf("round trip drifted: got=%+v", got)
	}
	input.Card[0] = 'x'
	input.DeliveryIDs[0] = 999
	if bytes.Equal(input.Card, got.Card) || got.DeliveryIDs[0] != 5 {
		t.Fatal("canonical payload aliases caller memory")
	}
}

func TestCanonicalizeRejectsAmbiguousIdentity(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Prepared)
	}{
		{name: "duplicate deliveries", change: func(p *Prepared) {
			p.DeliveryIDs = []int64{5, 5}
		}},
		{name: "unordered deliveries", change: func(p *Prepared) {
			p.DeliveryIDs = []int64{6, 5}
		}},
		{name: "noncanonical uuid", change: func(p *Prepared) {
			p.ProviderUUID = "2F790AB2-0622-4DF8-8F93-6079A3A0F94F"
		}},
		{name: "invalid chunk", change: func(p *Prepared) {
			p.ChunkIndex = p.ChunkCount
		}},
		{name: "missing provider chat", change: func(p *Prepared) {
			p.ProviderChatID = ""
		}},
		{name: "noncanonical expiry precision", change: func(p *Prepared) {
			p.IdempotencyExpiresAt = p.IdempotencyExpiresAt.Add(time.Nanosecond)
		}},
		{name: "newline identity", change: func(p *Prepared) {
			p.StepID = "push\nforged"
		}},
		{name: "invalid card utf8", change: func(p *Prepared) {
			p.Card = []byte{0xff}
		}},
		{name: "card array", change: func(p *Prepared) {
			p.Card = []byte(`[]`)
		}},
		{name: "duplicate card key", change: func(p *Prepared) {
			p.Card = []byte(`{"a":1,"a":2}`)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := fixture()
			tt.change(&input)
			if _, err := Canonicalize(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error=%v, want ErrInvalid", err)
			}
		})
	}
}

func TestDecodeRejectsDriftAndUnknownFields(t *testing.T) {
	canonical, err := Canonicalize(fixture())
	if err != nil {
		t.Fatal(err)
	}
	payload := canonical.Payload()
	for name, changed := range map[string][]byte{
		"digest": payload,
		"unknown": bytes.Replace(payload, []byte(`{"schema_version":`),
			[]byte(`{"unknown":1,"schema_version":`), 1),
		"bytes": bytes.Replace(payload, []byte(`"step_id":"push"`),
			[]byte(`"step_id":"other"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			digest := canonical.Digest()
			if name == "digest" {
				digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			if _, err := Decode(changed, digest); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error=%v, want ErrInvalid", err)
			}
		})
	}
}
