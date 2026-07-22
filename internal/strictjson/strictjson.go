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
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := consumeValue(decoder); err != nil {
		return err
	}
	return ensureEOF(decoder)
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
