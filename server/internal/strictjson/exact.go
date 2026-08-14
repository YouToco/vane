package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var rawMessageType = reflect.TypeFor[json.RawMessage]()

// DecodeExact adds an exact, representation-stable struct-key check to Decode.
// encoding/json otherwise accepts case-folded aliases (for example NAME for a
// `json:"name"` field). That behavior is unsafe for durable versioned wires:
// two distinct input keys can silently collapse into one field before the
// canonical bytes are hashed. It also requires every field not tagged
// `omitempty` to be present, so a missing security-sensitive boolean or number
// cannot silently become its Go zero value. RawMessage and map fields remain
// opaque schema islands, while Validate still rejects duplicate keys
// throughout their JSON.
func DecodeExact(raw []byte, dst any) error {
	if err := Validate(raw); err != nil {
		return err
	}
	destination := reflect.TypeOf(dst)
	if destination == nil || destination.Kind() != reflect.Pointer {
		return errors.New("strictjson: exact decode destination must be a pointer")
	}
	if err := validateExactShape(raw, destination.Elem()); err != nil {
		return err
	}
	return Decode(raw, dst)
}

func validateExactShape(payload []byte, destination reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := consumeExactValue(decoder, payload, destination); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("strictjson: multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func consumeExactValue(
	decoder *json.Decoder,
	payload []byte,
	destination reflect.Type,
) error {
	originalDestination := destination
	destination = indirectExactType(destination)
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil && !exactTypeAllowsNull(originalDestination) {
		return errors.New("strictjson: null does not match exact schema")
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter || destination == rawMessageType ||
		destination.Kind() == reflect.Map || destination.Kind() == reflect.Interface {
		if isDelimiter {
			return consumeOpaqueDelimited(decoder, delimiter)
		}
		return nil
	}
	switch delimiter {
	case '{':
		return consumeExactObject(decoder, payload, destination)
	case '[':
		return consumeExactArray(decoder, payload, destination)
	default:
		return fmt.Errorf("strictjson: unexpected delimiter %q", delimiter)
	}
}

func exactTypeAllowsNull(destination reflect.Type) bool {
	if destination == rawMessageType {
		return true
	}
	switch destination.Kind() {
	case reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func consumeExactObject(
	decoder *json.Decoder,
	payload []byte,
	destination reflect.Type,
) error {
	if destination.Kind() != reflect.Struct {
		return errors.New("strictjson: JSON object does not match exact schema")
	}
	fields := exactStructFields(destination)
	seen := make(map[string]struct{}, len(fields))
	for decoder.More() {
		start := decoder.InputOffset()
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		end := decoder.InputOffset()
		key, ok := token.(string)
		if !ok || !hasCanonicalRawKey(payload, start, end, key) {
			return errors.New("strictjson: object key is not canonical")
		}
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("strictjson: unknown exact field %q", key)
		}
		seen[key] = struct{}{}
		if err := consumeExactValue(decoder, payload, field.typ); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return errors.New("strictjson: object is not closed")
	}
	for name, field := range fields {
		if _, present := seen[name]; field.required && !present {
			return fmt.Errorf("strictjson: required exact field %q is missing", name)
		}
	}
	return nil
}

func consumeExactArray(
	decoder *json.Decoder,
	payload []byte,
	destination reflect.Type,
) error {
	if destination.Kind() != reflect.Array && destination.Kind() != reflect.Slice {
		return errors.New("strictjson: JSON array does not match exact schema")
	}
	for decoder.More() {
		if err := consumeExactValue(decoder, payload, destination.Elem()); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return errors.New("strictjson: array is not closed")
	}
	return nil
}

func consumeOpaqueDelimited(decoder *json.Decoder, opening json.Delim) error {
	var closing json.Delim
	switch opening {
	case '{':
		closing = '}'
	case '[':
		closing = ']'
	default:
		return fmt.Errorf("strictjson: unexpected opaque delimiter %q", opening)
	}
	for decoder.More() {
		if opening == '{' {
			if _, err := decoder.Token(); err != nil {
				return err
			}
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if nested, ok := token.(json.Delim); ok {
			if err := consumeOpaqueDelimited(decoder, nested); err != nil {
				return err
			}
		}
	}
	token, err := decoder.Token()
	if err != nil || token != closing {
		return errors.New("strictjson: opaque value is not closed")
	}
	return nil
}

type exactStructField struct {
	typ      reflect.Type
	required bool
}

func exactStructFields(destination reflect.Type) map[string]exactStructField {
	fields := make(map[string]exactStructField, destination.NumField())
	for i := range destination.NumField() {
		field := destination.Field(i)
		if !field.IsExported() {
			continue
		}
		name := field.Tag.Get("json")
		options := ""
		if comma := strings.IndexByte(name, ','); comma >= 0 {
			name, options = name[:comma], name[comma+1:]
		}
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = exactStructField{
			typ:      field.Type,
			required: !hasJSONTagOption(options, "omitempty"),
		}
	}
	return fields
}

func hasJSONTagOption(options, want string) bool {
	for options != "" {
		option := options
		if comma := strings.IndexByte(options, ','); comma >= 0 {
			option, options = options[:comma], options[comma+1:]
		} else {
			options = ""
		}
		if option == want {
			return true
		}
	}
	return false
}

func hasCanonicalRawKey(payload []byte, start, end int64, decoded string) bool {
	if start < 0 || end < start || end > int64(len(payload)) {
		return false
	}
	raw := bytes.TrimSpace(payload[start:end])
	if len(raw) > 0 && raw[0] == ',' {
		raw = bytes.TrimSpace(raw[1:])
	}
	expected, err := json.Marshal(decoded)
	return err == nil && bytes.Equal(raw, expected)
}

func indirectExactType(value reflect.Type) reflect.Type {
	for value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	return value
}
