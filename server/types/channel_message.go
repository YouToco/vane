package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	ChannelMessageEnvelopeV1Schema       = "vane.channel-message/v1"
	maxChannelMediaItems                 = 10
	maxChannelMediaEnvelopeBytes         = 64 << 10
	maxProviderFileIDBytes               = 2048
	maxProviderUniqueIDBytes             = 512
	maxChannelCaptionBytes               = 16 << 10
	maxSafeJSONInteger             int64 = 1<<53 - 1
)

// ChannelMessageEnvelopeV1 is the provider-neutral, durable description of
// media attached to one authenticated channel update. It deliberately stores
// only provider opaque file references and advisory metadata. Download URLs,
// credential material and local paths are never part of the envelope.
//
// A future media worker may resolve ProviderFileID with the then-active channel
// credential only after rechecking identity, route, quota and model capability.
// MIMEType and FileName are untrusted hints and must be verified from bytes.
type ChannelMessageEnvelopeV1 struct {
	Schema       string                      `json:"schema"`
	Caption      string                      `json:"caption,omitempty"`
	MediaGroupID string                      `json:"media_group_id,omitempty"`
	Items        []ChannelMessageMediaItemV1 `json:"items"`
}

type ChannelMessageMediaItemV1 struct {
	Kind             string `json:"kind"`
	ProviderFileID   string `json:"provider_file_id"`
	ProviderUniqueID string `json:"provider_unique_id,omitempty"`
	MIMEType         string `json:"mime_type,omitempty"`
	FileName         string `json:"file_name,omitempty"`
	SizeBytes        int64  `json:"size_bytes,omitempty"`
	Width            int64  `json:"width,omitempty"`
	Height           int64  `json:"height,omitempty"`
	DurationSeconds  int64  `json:"duration_seconds,omitempty"`
}

func validChannelMediaKind(kind string) bool {
	switch kind {
	case "image", "animation", "audio", "voice", "video", "video_note",
		"document", "sticker":
		return true
	default:
		return false
	}
}

func validTrimmedBounded(value string, maxBytes int, allowEmpty bool) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value || len(value) > maxBytes ||
		strings.IndexFunc(value, func(r rune) bool {
			return unicode.IsControl(r) && r != '\n' && r != '\t'
		}) >= 0 {
		return false
	}
	return allowEmpty || value != ""
}

func validOpaqueMetadata(value string, maxBytes int, allowEmpty bool) bool {
	return validTrimmedBounded(value, maxBytes, allowEmpty) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validateChannelMessageEnvelopeV1(envelope ChannelMessageEnvelopeV1) error {
	if envelope.Schema != ChannelMessageEnvelopeV1Schema ||
		len(envelope.Items) == 0 || len(envelope.Items) > maxChannelMediaItems ||
		!validTrimmedBounded(envelope.Caption, maxChannelCaptionBytes, true) ||
		!validOpaqueMetadata(envelope.MediaGroupID, 128, true) {
		return NewAppError(CodeValidation, "渠道媒体信封无效", ErrValidation)
	}
	seen := make(map[string]struct{}, len(envelope.Items))
	for _, item := range envelope.Items {
		if !validChannelMediaKind(item.Kind) ||
			!validOpaqueMetadata(item.ProviderFileID, maxProviderFileIDBytes, false) ||
			!validOpaqueMetadata(item.ProviderUniqueID, maxProviderUniqueIDBytes, true) ||
			!validOpaqueMetadata(item.MIMEType, 255, true) ||
			!validOpaqueMetadata(item.FileName, 1024, true) ||
			item.SizeBytes < 0 || item.SizeBytes > maxSafeJSONInteger ||
			item.Width < 0 || item.Width > 1_000_000 ||
			item.Height < 0 || item.Height > 1_000_000 ||
			item.DurationSeconds < 0 || item.DurationSeconds > 10*365*24*60*60 {
			return NewAppError(CodeValidation, "渠道媒体项目无效", ErrValidation)
		}
		key := item.Kind + "\x00" + item.ProviderFileID
		if _, ok := seen[key]; ok {
			return NewAppError(CodeValidation, "渠道媒体项目重复", ErrValidation)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// MarshalChannelMessageEnvelopeV1 validates and returns a stable JSON encoding
// suitable for hashing and durable ingress storage.
func MarshalChannelMessageEnvelopeV1(envelope ChannelMessageEnvelopeV1) ([]byte, error) {
	if err := validateChannelMessageEnvelopeV1(envelope); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil || len(encoded) > maxChannelMediaEnvelopeBytes {
		return nil, NewAppError(CodeValidation, "渠道媒体信封过大", ErrValidation)
	}
	return encoded, nil
}

// DecodeChannelMessageEnvelopeV1 rejects unknown fields and non-canonical
// values. Schema evolution must add a new decoder instead of silently changing
// the meaning of stored v1 bytes.
func DecodeChannelMessageEnvelopeV1(encoded []byte) (ChannelMessageEnvelopeV1, error) {
	if len(encoded) == 0 || len(encoded) > maxChannelMediaEnvelopeBytes {
		return ChannelMessageEnvelopeV1{}, NewAppError(
			CodeValidation, "渠道媒体信封大小无效", ErrValidation)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var envelope ChannelMessageEnvelopeV1
	if err := decoder.Decode(&envelope); err != nil {
		return ChannelMessageEnvelopeV1{}, NewAppError(
			CodeValidation, "渠道媒体信封格式无效", ErrValidation)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ChannelMessageEnvelopeV1{}, NewAppError(
			CodeValidation, "渠道媒体信封包含多余内容", ErrValidation)
	}
	if err := validateChannelMessageEnvelopeV1(envelope); err != nil {
		return ChannelMessageEnvelopeV1{}, err
	}
	return envelope, nil
}
