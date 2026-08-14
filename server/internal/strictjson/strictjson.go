// Package strictjson validates security-sensitive JSON before a JSONB write
// can erase representation-level evidence such as duplicate object keys.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// Validate rejects malformed input, duplicate keys at any nesting depth, and
// multiple top-level values. Standard encoding/json otherwise accepts a
// duplicate key by silently keeping the last value.
func Validate(raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("strictjson: input is not valid UTF-8")
	}
	if err := validateUnicodeEscapePairs(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := consumeValue(decoder); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

// encoding/json intentionally replaces an unpaired UTF-16 surrogate escape
// with U+FFFD. That is convenient for loose input, but unsafe for canonical,
// content-addressed wires: distinct invalid byte strings such as \ud800 and
// \ud801 would collapse to the same decoded value and digest. Reject them
// before the standard decoder can erase the distinction.
func validateUnicodeEscapePairs(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			if i+1 >= len(raw) {
				return errors.New("strictjson: truncated string escape")
			}
			if raw[i+1] != 'u' {
				i++ // Skip an escaped quote/backslash so it cannot toggle string state.
				continue
			}
			value, ok := decodeHexQuad(raw, i+2)
			if !ok {
				return errors.New("strictjson: invalid unicode escape")
			}
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				if i+11 >= len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
					return errors.New("strictjson: unpaired high surrogate escape")
				}
				low, valid := decodeHexQuad(raw, i+8)
				if !valid || low < 0xdc00 || low > 0xdfff {
					return errors.New("strictjson: unpaired high surrogate escape")
				}
				i += 11
			case value >= 0xdc00 && value <= 0xdfff:
				return errors.New("strictjson: unpaired low surrogate escape")
			default:
				i += 5
			}
		}
	}
	return nil
}

func decodeHexQuad(raw []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, char := range raw[start : start+4] {
		value <<= 4
		switch {
		case char >= '0' && char <= '9':
			value |= uint16(char - '0')
		case char >= 'a' && char <= 'f':
			value |= uint16(char-'a') + 10
		case char >= 'A' && char <= 'F':
			value |= uint16(char-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

// Decode applies Validate, then decodes one value with unknown struct fields
// rejected. UseNumber preserves integers wider than float64 when dst contains
// interface values.
func Decode(raw []byte, dst any) error {
	if err := Validate(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func consumeValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, isString := keyToken.(string)
			if !isString {
				return errors.New("strictjson: object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("strictjson: duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("strictjson: object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := consumeValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("strictjson: array is not closed")
		}
	default:
		return fmt.Errorf("strictjson: unexpected delimiter %q", delim)
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("strictjson: multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
