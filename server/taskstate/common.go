package taskstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
)

const (
	maxTaskIDBytes              = 255
	maxIntentBytes              = 16 << 10
	maxDescriptionBytes         = 16 << 10
	maxPlaybookBytes            = 256 << 10
	maxJSONObjectBytes          = 256 << 10
	maxDefinitionBytes          = 2 << 20
	maxAdaptiveBytes            = 256 << 10
	maxSourceCount              = 64
	maxSourceURLBytes           = 4096
	maxSourceTextBytes          = 4096
	maxQueryVariantCount        = 64
	maxQueryBytes               = 4096
	maxCapabilityCount          = 256
	maxToolNameBytes            = 255
	maxToolContractVersionBytes = 255
	maxToolCallCount            = 64
	maxCursorBytes              = 64 << 10
)

// ErrInvalidState identifies malformed, unsupported, or non-canonical task
// state input. Callers should not expose the wrapped implementation detail to
// end users.
var ErrInvalidState = errors.New("taskstate: invalid state")

func invalidState(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidState, message)
}

func canonicalJSONObject(raw json.RawMessage, field string) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxJSONObjectBytes {
		return nil, invalidState(field + " json size is invalid")
	}
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil || object == nil {
		return nil, invalidState(field + " must be a strict json object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, invalidState(field + " cannot be canonicalized")
	}
	return canonical, nil
}

func validIdentifier(value string, maxBytes int) bool {
	return validSingleLineText(value, maxBytes, false)
}

func validOptionalSingleLineText(value string, maxBytes int) bool {
	return value == "" || validSingleLineText(value, maxBytes, false)
}

func validSingleLineText(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || strings.TrimSpace(value) != value ||
		len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	return !containsUnsafeRune(value, false)
}

func validMultilineText(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && strings.TrimSpace(value) == "") || len(value) > maxBytes ||
		!utf8.ValidString(value) {
		return false
	}
	return !containsUnsafeRune(value, true)
}

func containsUnsafeRune(value string, allowLayoutControls bool) bool {
	for _, r := range value {
		if unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return true
		}
		if unicode.IsControl(r) &&
			!(allowLayoutControls && (r == '\n' || r == '\r' || r == '\t')) {
			return true
		}
	}
	return false
}

func validReadCapability(platform types.Platform, capability types.Capability) bool {
	// Use literal V1 wire values instead of current enum validators. Adding a
	// future application capability must not reinterpret retained V1 bytes.
	switch string(platform) + "/" + string(capability) {
	case "web/feed", "web/search", "web/contents", "x/user_posts",
		"xhs/search", "xhs/user_posts", "xhs/hot_list", "xhs/topic_feed",
		"xhs/faved_notes",
		// 2026-07-23 增补（与 capabilitycatalog/multi 路由同批）：缺席会让
		// weibo/wechat_mp 抓取要求通过物化校验，却在提交事务构建 Approved head
		// 时被拒。冻结 V1 reader 必须保留当时已批准的能力集合。
		"weibo/user_posts", "weibo/hot_list", "wechat_mp/user_posts":
		return true
	}
	return false
}

func validExecutionModeV1(mode types.ExecutionMode) bool {
	switch string(mode) {
	case "compiled", "discover_at_run":
		return true
	default:
		return false
	}
}

func validStrictnessV1(strictness types.PushStrictness) bool {
	switch string(strictness) {
	case "loose", "normal", "strict":
		return true
	default:
		return false
	}
}

func marshalBounded(value any, maxBytes int, field string) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, invalidState(field + " cannot be encoded")
	}
	if len(payload) == 0 || len(payload) > maxBytes {
		return nil, invalidState(field + " encoded size is invalid")
	}
	return payload, nil
}
