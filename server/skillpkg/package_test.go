package skillpkg

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

type zipTestFile struct {
	name string
	body []byte
	mode uint32
}

func makeZIP(t *testing.T, files ...zipTestFile) []byte {
	t.Helper()
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for _, file := range files {
		header := &zip.FileHeader{Name: file.name, Method: zip.Deflate}
		if file.mode != 0 {
			header.SetMode(fs.FileMode(file.mode))
		}
		writer, err := zw.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(file.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestParseZIPDeclarativePackageAndScriptIncompatibility(t *testing.T) {
	archive := makeZIP(t,
		zipTestFile{name: "SKILL.md", body: []byte("---\nname: market-watch\ndescription: Tracks markets\n---\nInstructions")},
		zipTestFile{name: "references/schema.md", body: []byte("# Schema")},
		zipTestFile{name: "assets/icon.svg", body: []byte("<svg></svg>")},
		zipTestFile{name: "scripts/run.sh", body: []byte("exit 0")},
	)
	parsed, err := ParseZIP(archive, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Manifest.Name != "market-watch" || parsed.Manifest.Compatible ||
		!parsed.Manifest.ContainsScript || len(parsed.Manifest.Files) != 4 {
		t.Fatalf("manifest=%+v", parsed.Manifest)
	}
	if len(parsed.ManifestJSON) == 0 || len(parsed.ManifestDigest) != 64 || len(parsed.ArchiveDigest) != 64 {
		t.Fatalf("digests manifest=%q archive=%q", parsed.ManifestDigest, parsed.ArchiveDigest)
	}
	for _, file := range parsed.Manifest.Files {
		if file.Kind == FileScript && file.Data != nil {
			t.Fatal("script bytes escaped the parser")
		}
	}
	again, err := ParseZIP(archive, Limits{})
	if err != nil || string(again.ManifestJSON) != string(parsed.ManifestJSON) ||
		again.ManifestDigest != parsed.ManifestDigest {
		t.Fatalf("non-deterministic parse err=%v", err)
	}
}

func TestParseZIPRejectsUnsafeShapes(t *testing.T) {
	base := zipTestFile{name: "SKILL.md", body: []byte("---\nname: safe\n---\nBody")}
	tests := []struct {
		name  string
		files []zipTestFile
		want  error
	}{
		{"traversal", []zipTestFile{base, {name: "../secret", body: []byte("x")}}, ErrUnsafeArchive},
		{"backslash", []zipTestFile{base, {name: `assets\secret`, body: []byte("x")}}, ErrUnsafeArchive},
		{"hidden", []zipTestFile{base, {name: "assets/.env", body: []byte("x")}}, ErrUnsafeArchive},
		{"unsupported root", []zipTestFile{base, {name: "README.md", body: []byte("x")}}, ErrUnsafeArchive},
		{"duplicate", []zipTestFile{base, base}, ErrUnsafeArchive},
		{"executable", []zipTestFile{base, {name: "assets/run", body: []byte("x"), mode: 0755}}, ErrUnsafeArchive},
		{"symlink", []zipTestFile{base, {name: "assets/link", body: []byte("target"), mode: uint32(fs.ModeSymlink | 0777)}}, ErrUnsafeArchive},
		{"secret", []zipTestFile{base, {name: "references/key.md", body: []byte("api_key = abcdefghijklmnopqrstuvwxyz")}}, ErrSecretDetected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseZIP(makeZIP(t, tc.files...), Limits{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
		})
	}
}

func TestParseZIPRejectsExpandedLimitsAndBadSkillMetadata(t *testing.T) {
	large := bytes.Repeat([]byte("z"), 4096)
	archive := makeZIP(t,
		zipTestFile{name: "SKILL.md", body: []byte("---\nname: safe\n---\nBody")},
		zipTestFile{name: "assets/large.txt", body: large},
	)
	_, err := ParseZIP(archive, Limits{MaxFileBytes: 1024})
	if !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("file limit error=%v", err)
	}

	for i, body := range [][]byte{
		[]byte("no frontmatter"),
		[]byte("---\nname: Not Valid\n---\nBody"),
		[]byte("---\nname: safe\n"),
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, parseErr := ParseZIP(makeZIP(t, zipTestFile{name: "SKILL.md", body: body}), Limits{})
			if !errors.Is(parseErr, ErrInvalidArchive) {
				t.Fatalf("error=%v", parseErr)
			}
		})
	}

	bomb := makeZIP(t,
		zipTestFile{name: "SKILL.md", body: []byte("---\nname: safe\n---\nBody")},
		zipTestFile{name: "assets/repeated.txt", body: bytes.Repeat([]byte("a"), 2<<20)},
	)
	if _, err := ParseZIP(bomb, Limits{MaxCompressionRatio: 10}); !errors.Is(err, ErrUnsafeArchive) {
		t.Fatalf("compression bomb error=%v", err)
	}
}
