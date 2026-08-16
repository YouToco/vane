package types

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestChannelMessageEnvelopeV1RoundTrip(t *testing.T) {
	envelope := ChannelMessageEnvelopeV1{
		Schema:       ChannelMessageEnvelopeV1Schema,
		Caption:      "比较两张产品截图",
		MediaGroupID: "album-7",
		Items: []ChannelMessageMediaItemV1{
			{Kind: "image", ProviderFileID: "photo-file", ProviderUniqueID: "photo-unique",
				MIMEType: "image/jpeg", SizeBytes: 2048, Width: 1280, Height: 720},
			{Kind: "voice", ProviderFileID: "voice-file", MIMEType: "audio/ogg",
				DurationSeconds: 8},
		},
	}
	encoded, err := MarshalChannelMessageEnvelopeV1(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "bot-token") ||
		strings.Contains(string(encoded), "api.telegram.org/file") {
		t.Fatalf("envelope contains forbidden transport authority: %s", encoded)
	}
	decoded, err := DecodeChannelMessageEnvelopeV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Schema != envelope.Schema || decoded.Caption != envelope.Caption ||
		len(decoded.Items) != 2 || decoded.Items[1].Kind != "voice" {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestChannelMessageEnvelopeV1RejectsUnsafeOrUnknownShape(t *testing.T) {
	valid := ChannelMessageEnvelopeV1{Schema: ChannelMessageEnvelopeV1Schema,
		Items: []ChannelMessageMediaItemV1{{Kind: "image", ProviderFileID: "file"}}}
	for _, mutate := range []func(*ChannelMessageEnvelopeV1){
		func(value *ChannelMessageEnvelopeV1) { value.Schema = "vane.channel-message/v2" },
		func(value *ChannelMessageEnvelopeV1) { value.Items = nil },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].Kind = "executable" },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].ProviderFileID = " file" },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].FileName = "bad\nname.pdf" },
		func(value *ChannelMessageEnvelopeV1) { value.Caption = "bad\x00caption" },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].SizeBytes = -1 },
	} {
		candidate := valid
		candidate.Items = append([]ChannelMessageMediaItemV1(nil), valid.Items...)
		mutate(&candidate)
		if _, err := MarshalChannelMessageEnvelopeV1(candidate); !errors.Is(err, ErrValidation) {
			t.Fatalf("candidate=%+v err=%v", candidate, err)
		}
	}

	unknown, _ := json.Marshal(map[string]any{
		"schema":       ChannelMessageEnvelopeV1Schema,
		"items":        []map[string]any{{"kind": "image", "provider_file_id": "file"}},
		"download_url": "https://api.telegram.org/file/bot-secret/path",
	})
	if _, err := DecodeChannelMessageEnvelopeV1(unknown); !errors.Is(err, ErrValidation) {
		t.Fatalf("unknown field err=%v", err)
	}
}

func TestChannelMessageEnvelopeV1RejectsBoundaryAndDuplicateMetadata(t *testing.T) {
	valid := ChannelMessageEnvelopeV1{Schema: ChannelMessageEnvelopeV1Schema,
		Items: []ChannelMessageMediaItemV1{{Kind: "image", ProviderFileID: "file"}}}
	for _, mutate := range []func(*ChannelMessageEnvelopeV1){
		func(value *ChannelMessageEnvelopeV1) {
			value.Items = append(value.Items, value.Items[0])
		},
		func(value *ChannelMessageEnvelopeV1) { value.MediaGroupID = " bad" },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].ProviderUniqueID = "bad\nvalue" },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].Width = 1_000_001 },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].Height = -1 },
		func(value *ChannelMessageEnvelopeV1) { value.Items[0].DurationSeconds = 10*365*24*60*60 + 1 },
	} {
		candidate := valid
		candidate.Items = append([]ChannelMessageMediaItemV1(nil), valid.Items...)
		mutate(&candidate)
		if _, err := MarshalChannelMessageEnvelopeV1(candidate); !errors.Is(err, ErrValidation) {
			t.Fatalf("candidate=%+v err=%v", candidate, err)
		}
	}
	if _, err := DecodeChannelMessageEnvelopeV1(nil); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty decode err=%v", err)
	}
	encoded, err := MarshalChannelMessageEnvelopeV1(valid)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, []byte(` {}`)...)
	if _, err := DecodeChannelMessageEnvelopeV1(encoded); !errors.Is(err, ErrValidation) {
		t.Fatalf("trailing decode err=%v", err)
	}
	semanticInvalid := []byte(`{"schema":"vane.channel-message/v1","items":[]}`)
	if _, err := DecodeChannelMessageEnvelopeV1(semanticInvalid); !errors.Is(err, ErrValidation) {
		t.Fatalf("semantic decode err=%v", err)
	}
}
