package skillruntime

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSkillContractsCanonicalRoundTrip(t *testing.T) {
	ref := testSkillRefV1()
	refPayload, err := EncodeSkillRefV1(ref)
	if err != nil {
		t.Fatal(err)
	}
	decodedRef, err := DecodeSkillRefV1(refPayload)
	if err != nil || decodedRef != ref {
		t.Fatalf("decoded ref=%+v err=%v", decodedRef, err)
	}
	handle := SkillResourceHandleV1{
		SchemaVersion: SkillResourceHandleSchemaVersionV1,
		Skill:         ref, Path: "references/guide.md", Kind: ResourceKindReferenceV1,
		ContentDigest: strings.Repeat("d", 64), ContentSize: 3,
	}
	handlePayload, err := EncodeSkillResourceHandleV1(handle)
	if err != nil {
		t.Fatal(err)
	}
	decodedHandle, err := DecodeSkillResourceHandleV1(handlePayload)
	if err != nil || decodedHandle != handle {
		t.Fatalf("decoded handle=%+v err=%v", decodedHandle, err)
	}
	chunk := SkillResourceChunkV1{
		SchemaVersion: SkillResourceChunkSchemaVersionV1,
		HandleDigest:  DigestHandleV1(handle), Offset: 0, Data: []byte("abc"), EOF: true,
	}
	chunkPayload, err := EncodeSkillResourceChunkV1(chunk, handle)
	if err != nil {
		t.Fatal(err)
	}
	decodedChunk, err := DecodeSkillResourceChunkV1(chunkPayload, handle)
	if err != nil || !bytes.Equal(decodedChunk.Data, chunk.Data) ||
		decodedChunk.HandleDigest != chunk.HandleDigest || !decodedChunk.EOF {
		t.Fatalf("decoded chunk=%+v err=%v", decodedChunk, err)
	}
}

func TestSkillContractsRejectNonCanonicalAndExecutableShapes(t *testing.T) {
	ref := testSkillRefV1()
	payload, err := EncodeSkillRefV1(ref)
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		append([]byte(" "), payload...),
		bytes.Replace(payload, []byte(`"tenant_id":11`), []byte(`"tenant_id":1.1e1`), 1),
		bytes.Replace(payload, []byte(`"schema_version"`), []byte(`"unknown":true,"schema_version"`), 1),
		bytes.Replace(payload, []byte(ref.CapabilityID.String()), []byte(strings.ToUpper(ref.CapabilityID.String())), 1),
	}
	for _, candidate := range tests {
		if _, err := DecodeSkillRefV1(candidate); !errors.Is(err, ErrInvalidSkillContract) {
			t.Fatalf("payload=%s err=%v", candidate, err)
		}
	}

	ref.ContainsScripts = true
	if _, err := EncodeSkillRefV1(ref); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("script-bearing ref err=%v", err)
	}
	ref = testSkillRefV1()
	ref.Compatible = false
	if _, err := EncodeSkillRefV1(ref); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("incompatible ref err=%v", err)
	}
	handle := SkillResourceHandleV1{
		SchemaVersion: SkillResourceHandleSchemaVersionV1, Skill: testSkillRefV1(),
		Path: "scripts/run.sh", Kind: ResourceKindAssetV1,
		ContentDigest: strings.Repeat("d", 64), ContentSize: 1,
	}
	if _, err := EncodeSkillResourceHandleV1(handle); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("script path handle err=%v", err)
	}
}

func TestSkillResourceChunkBindsHandleOffsetAndEOF(t *testing.T) {
	handle := SkillResourceHandleV1{
		SchemaVersion: SkillResourceHandleSchemaVersionV1, Skill: testSkillRefV1(),
		Path: "assets/logo.png", Kind: ResourceKindAssetV1,
		ContentDigest: strings.Repeat("d", 64), ContentSize: 5,
	}
	base := SkillResourceChunkV1{
		SchemaVersion: SkillResourceChunkSchemaVersionV1,
		HandleDigest:  DigestHandleV1(handle), Offset: 0, Data: []byte("abc"), EOF: false,
	}
	if _, err := EncodeSkillResourceChunkV1(base, handle); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SkillResourceChunkV1){
		"wrong handle":       func(v *SkillResourceChunkV1) { v.HandleDigest = strings.Repeat("a", 64) },
		"premature eof":      func(v *SkillResourceChunkV1) { v.EOF = true },
		"empty continuation": func(v *SkillResourceChunkV1) { v.Data = nil },
		"past end":           func(v *SkillResourceChunkV1) { v.Offset = 4 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Data = bytes.Clone(base.Data)
			mutate(&candidate)
			if _, err := EncodeSkillResourceChunkV1(candidate, handle); !errors.Is(err, ErrInvalidSkillContract) {
				t.Fatalf("chunk=%+v err=%v", candidate, err)
			}
		})
	}
}

func TestSkillContractsDecodeSemanticFailuresAndHelperEdges(t *testing.T) {
	ref := testSkillRefV1()
	refPayload, err := EncodeSkillRefV1(ref)
	if err != nil {
		t.Fatal(err)
	}
	invalidRefPayload := bytes.Replace(refPayload, []byte(`"tenant_id":11`), []byte(`"tenant_id":0`), 1)
	if _, err := DecodeSkillRefV1(invalidRefPayload); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("semantic ref err=%v", err)
	}
	if got := DigestRefV1(SkillRefV1{}); got != "" {
		t.Fatalf("invalid ref digest=%q", got)
	}

	handle := SkillResourceHandleV1{
		SchemaVersion: SkillResourceHandleSchemaVersionV1, Skill: ref,
		Path: "references/guide.md", Kind: ResourceKindReferenceV1,
		ContentDigest: strings.Repeat("d", 64), ContentSize: 3,
	}
	handlePayload, err := EncodeSkillResourceHandleV1(handle)
	if err != nil {
		t.Fatal(err)
	}
	invalidHandlePayload := bytes.Replace(handlePayload, []byte(`"content_size":3`), []byte(`"content_size":-1`), 1)
	if _, err := DecodeSkillResourceHandleV1(invalidHandlePayload); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("semantic handle err=%v", err)
	}
	if _, err := DecodeSkillResourceHandleV1(nil); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("empty handle err=%v", err)
	}
	if got := DigestHandleV1(SkillResourceHandleV1{}); got != "" {
		t.Fatalf("invalid handle digest=%q", got)
	}

	chunk := SkillResourceChunkV1{
		SchemaVersion: SkillResourceChunkSchemaVersionV1,
		HandleDigest:  DigestHandleV1(handle), Offset: 0, Data: []byte("abc"), EOF: true,
	}
	chunkPayload, err := EncodeSkillResourceChunkV1(chunk, handle)
	if err != nil {
		t.Fatal(err)
	}
	invalidChunkPayload := bytes.Replace(chunkPayload, []byte(chunk.HandleDigest),
		[]byte(strings.Repeat("e", 64)), 1)
	if _, err := DecodeSkillResourceChunkV1(invalidChunkPayload, handle); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("semantic chunk err=%v", err)
	}
	if _, err := DecodeSkillResourceChunkV1(nil, handle); !errors.Is(err, ErrInvalidSkillContract) {
		t.Fatalf("empty chunk err=%v", err)
	}

	for name, candidate := range map[string]SkillResourceHandleV1{
		"backslash":        func() SkillResourceHandleV1 { v := handle; v.Path = `references\\guide.md`; return v }(),
		"hidden component": func() SkillResourceHandleV1 { v := handle; v.Path = "references/.secret"; return v }(),
		"skill md": func() SkillResourceHandleV1 {
			v := handle
			v.Path = "SKILL.md"
			v.Kind = ResourceKindSkillMDV1
			return v
		}(),
		"asset": func() SkillResourceHandleV1 {
			v := handle
			v.Path = "assets/logo.png"
			v.Kind = ResourceKindAssetV1
			return v
		}(),
		"unknown kind": func() SkillResourceHandleV1 { v := handle; v.Kind = "script"; return v }(),
	} {
		t.Run(name, func(t *testing.T) {
			err := candidate.Validate()
			wantValid := name == "skill md" || name == "asset"
			if (err == nil) != wantValid {
				t.Fatalf("handle=%+v err=%v wantValid=%t", candidate, err, wantValid)
			}
		})
	}
}

func testSkillRefV1() SkillRefV1 {
	return SkillRefV1{
		SchemaVersion: SkillRefSchemaVersionV1,
		TenantID:      11, OwnerUserID: 22,
		CapabilityID:        uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		CapabilityVersionID: uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		Version:             1, Visibility: VisibilityPersonalV1,
		PayloadDigest: strings.Repeat("a", 64), SkillMDDigest: strings.Repeat("b", 64),
		FileManifestDigest: strings.Repeat("c", 64), Compatible: true,
	}
}
