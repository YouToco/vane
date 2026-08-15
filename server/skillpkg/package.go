// Package skillpkg parses declarative Agent Skill archives without executing
// any package content. It is deliberately independent from the Agent runtime.
package skillpkg

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	DefaultMaxArchiveBytes      = 8 << 20
	DefaultMaxUncompressedBytes = 16 << 20
	DefaultMaxFileBytes         = 4 << 20
	DefaultMaxSkillMDBytes      = 256 << 10
	DefaultMaxReferenceBytes    = 512 << 10
	DefaultMaxFiles             = 128
	DefaultMaxCompressionRatio  = 200
)

var (
	ErrInvalidArchive = errors.New("skillpkg: invalid archive")
	ErrUnsafeArchive  = errors.New("skillpkg: unsafe archive")
	ErrSecretDetected = errors.New("skillpkg: possible secret detected")

	identifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	secretPatterns    = []*regexp.Regexp{
		regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----`),
		regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
		regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`),
		regexp.MustCompile(`(?i)\b(?:api[_-]?key|access[_-]?token|client[_-]?secret|password)\b\s*[:=]\s*["']?[A-Za-z0-9_./+\-=]{16,}`),
	}
)

type Limits struct {
	MaxArchiveBytes      int64
	MaxUncompressedBytes int64
	MaxFileBytes         int64
	MaxSkillMDBytes      int64
	MaxReferenceBytes    int64
	MaxFiles             int
	MaxCompressionRatio  uint64
}

func DefaultLimits() Limits {
	return Limits{
		MaxArchiveBytes: DefaultMaxArchiveBytes, MaxUncompressedBytes: DefaultMaxUncompressedBytes,
		MaxFileBytes: DefaultMaxFileBytes, MaxSkillMDBytes: DefaultMaxSkillMDBytes,
		MaxReferenceBytes: DefaultMaxReferenceBytes, MaxFiles: DefaultMaxFiles,
		MaxCompressionRatio: DefaultMaxCompressionRatio,
	}
}

type FileKind string

const (
	FileSkillMD   FileKind = "skill_md"
	FileReference FileKind = "reference"
	FileAsset     FileKind = "asset"
	FileScript    FileKind = "script"
)

type File struct {
	Path   string   `json:"path"`
	Kind   FileKind `json:"kind"`
	Size   int64    `json:"size"`
	Digest string   `json:"digest"`
	Data   []byte   `json:"-"`
}

type Manifest struct {
	SchemaVersion  string `json:"schema_version"`
	Name           string `json:"name"`
	Description    string `json:"description,omitempty"`
	Compatible     bool   `json:"compatible"`
	ContainsScript bool   `json:"contains_scripts"`
	Files          []File `json:"files"`
}

type Package struct {
	Manifest       Manifest
	ManifestJSON   []byte
	ManifestDigest string
	ArchiveDigest  string
}

type frontMatter struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// ParseZIP validates and parses one root-level Skill package. Only SKILL.md,
// references/, assets/, and scripts/ are recognized. Script bytes are never
// returned to callers and their presence makes the package incompatible.
func ParseZIP(archive []byte, limits Limits) (Package, error) {
	limits = normalizedLimits(limits)
	if len(archive) == 0 || int64(len(archive)) > limits.MaxArchiveBytes {
		return Package{}, invalid(ErrInvalidArchive, "archive size is outside limits")
	}
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return Package{}, invalid(ErrInvalidArchive, "open zip: %v", err)
	}
	if len(zr.File) == 0 || len(zr.File) > limits.MaxFiles {
		return Package{}, invalid(ErrInvalidArchive, "file count is outside limits")
	}

	seen := make(map[string]struct{}, len(zr.File))
	files := make([]File, 0, len(zr.File))
	var total int64
	var skillMD []byte
	for _, entry := range zr.File {
		cleaned, kind, isDir, pathErr := validateEntryPath(entry.Name)
		if pathErr != nil {
			return Package{}, pathErr
		}
		if _, duplicate := seen[cleaned]; duplicate {
			return Package{}, invalid(ErrUnsafeArchive, "duplicate path %q", cleaned)
		}
		seen[cleaned] = struct{}{}
		mode := entry.Mode()
		if mode&(^mode.Perm()) != 0 && !mode.IsRegular() && !mode.IsDir() {
			return Package{}, invalid(ErrUnsafeArchive, "special file %q", cleaned)
		}
		if mode&0111 != 0 {
			return Package{}, invalid(ErrUnsafeArchive, "executable file %q", cleaned)
		}
		if isDir {
			continue
		}
		fileLimit := limits.MaxFileBytes
		switch kind {
		case FileSkillMD:
			fileLimit = limits.MaxSkillMDBytes
		case FileReference:
			fileLimit = limits.MaxReferenceBytes
		}
		if entry.UncompressedSize64 > uint64(fileLimit) {
			return Package{}, invalid(ErrUnsafeArchive, "file %q exceeds size limit", cleaned)
		}
		if entry.UncompressedSize64 > 1<<20 &&
			(entry.CompressedSize64 == 0 || entry.UncompressedSize64/entry.CompressedSize64 > limits.MaxCompressionRatio) {
			return Package{}, invalid(ErrUnsafeArchive, "file %q exceeds compression ratio", cleaned)
		}
		body, readErr := readEntry(entry, fileLimit)
		if readErr != nil {
			return Package{}, invalid(ErrUnsafeArchive, "read %q: %v", cleaned, readErr)
		}
		total += int64(len(body))
		if total > limits.MaxUncompressedBytes {
			return Package{}, invalid(ErrUnsafeArchive, "uncompressed package exceeds size limit")
		}
		if possibleSecret(body) {
			return Package{}, invalid(ErrSecretDetected, "file %q contains a credential-shaped value", cleaned)
		}
		sum := sha256.Sum256(body)
		parsed := File{Path: cleaned, Kind: kind, Size: int64(len(body)), Digest: hex.EncodeToString(sum[:])}
		if kind != FileScript {
			parsed.Data = body
		}
		files = append(files, parsed)
		if kind == FileSkillMD {
			skillMD = body
		}
	}
	if len(skillMD) == 0 {
		return Package{}, invalid(ErrInvalidArchive, "root SKILL.md is required")
	}
	meta, err := parseFrontMatter(skillMD)
	if err != nil {
		return Package{}, err
	}
	slices.SortFunc(files, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	containsScripts := false
	for _, file := range files {
		containsScripts = containsScripts || file.Kind == FileScript
	}
	manifest := Manifest{
		SchemaVersion: "vane.skill-package/v1", Name: meta.Name,
		Description: meta.Description, Compatible: !containsScripts,
		ContainsScript: containsScripts, Files: files,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return Package{}, invalid(ErrInvalidArchive, "encode manifest: %v", err)
	}
	manifestSum := sha256.Sum256(manifestJSON)
	archiveSum := sha256.Sum256(archive)
	return Package{
		Manifest: manifest, ManifestJSON: manifestJSON,
		ManifestDigest: hex.EncodeToString(manifestSum[:]),
		ArchiveDigest:  hex.EncodeToString(archiveSum[:]),
	}, nil
}

func normalizedLimits(l Limits) Limits {
	defaults := DefaultLimits()
	if l.MaxArchiveBytes <= 0 {
		l.MaxArchiveBytes = defaults.MaxArchiveBytes
	}
	if l.MaxUncompressedBytes <= 0 {
		l.MaxUncompressedBytes = defaults.MaxUncompressedBytes
	}
	if l.MaxFileBytes <= 0 {
		l.MaxFileBytes = defaults.MaxFileBytes
	}
	if l.MaxSkillMDBytes <= 0 {
		l.MaxSkillMDBytes = defaults.MaxSkillMDBytes
	}
	if l.MaxReferenceBytes <= 0 {
		l.MaxReferenceBytes = defaults.MaxReferenceBytes
	}
	if l.MaxFiles <= 0 {
		l.MaxFiles = defaults.MaxFiles
	}
	if l.MaxCompressionRatio == 0 {
		l.MaxCompressionRatio = defaults.MaxCompressionRatio
	}
	return l
}

func validateEntryPath(name string) (string, FileKind, bool, error) {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) ||
		strings.Contains(name, `\`) || strings.HasPrefix(name, "/") {
		return "", "", false, invalid(ErrUnsafeArchive, "invalid path %q", name)
	}
	isDir := strings.HasSuffix(name, "/")
	trimmed := strings.TrimSuffix(name, "/")
	cleaned := path.Clean(trimmed)
	if cleaned == "." || cleaned != trimmed || strings.HasPrefix(cleaned, "../") {
		return "", "", false, invalid(ErrUnsafeArchive, "non-canonical path %q", name)
	}
	for _, component := range strings.Split(cleaned, "/") {
		if component == "" || component == ".." || strings.HasPrefix(component, ".") {
			return "", "", false, invalid(ErrUnsafeArchive, "hidden or unsafe path %q", name)
		}
	}
	if cleaned == "SKILL.md" {
		return cleaned, FileSkillMD, isDir, nil
	}
	for prefix, kind := range map[string]FileKind{
		"references/": FileReference, "assets/": FileAsset, "scripts/": FileScript,
	} {
		if strings.HasPrefix(cleaned, prefix) || (isDir && cleaned == strings.TrimSuffix(prefix, "/")) {
			return cleaned, kind, isDir, nil
		}
	}
	return "", "", false, invalid(ErrUnsafeArchive, "unsupported package path %q", name)
}

func readEntry(entry *zip.File, limit int64) ([]byte, error) {
	reader, err := entry.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("expanded size exceeds limit")
	}
	return body, nil
}

func parseFrontMatter(skillMD []byte) (frontMatter, error) {
	if !utf8.Valid(skillMD) || bytes.IndexByte(skillMD, 0) >= 0 {
		return frontMatter{}, invalid(ErrInvalidArchive, "SKILL.md must be UTF-8 text")
	}
	normalized := bytes.ReplaceAll(skillMD, []byte("\r\n"), []byte("\n"))
	if !bytes.HasPrefix(normalized, []byte("---\n")) {
		return frontMatter{}, invalid(ErrInvalidArchive, "SKILL.md YAML frontmatter is required")
	}
	end := bytes.Index(normalized[4:], []byte("\n---\n"))
	if end < 0 {
		return frontMatter{}, invalid(ErrInvalidArchive, "SKILL.md frontmatter is not terminated")
	}
	var meta frontMatter
	if err := yaml.Unmarshal(normalized[4:4+end], &meta); err != nil {
		return frontMatter{}, invalid(ErrInvalidArchive, "parse SKILL.md frontmatter: %v", err)
	}
	meta.Name = strings.TrimSpace(meta.Name)
	meta.Description = strings.TrimSpace(meta.Description)
	if !identifierPattern.MatchString(meta.Name) {
		return frontMatter{}, invalid(ErrInvalidArchive, "skill name is invalid")
	}
	if len(meta.Description) > 2048 || !utf8.ValidString(meta.Description) || strings.ContainsRune(meta.Description, 0) {
		return frontMatter{}, invalid(ErrInvalidArchive, "skill description is invalid")
	}
	return meta, nil
}

func possibleSecret(body []byte) bool {
	for _, pattern := range secretPatterns {
		if pattern.Find(body) != nil {
			return true
		}
	}
	return false
}

func invalid(class error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", class, fmt.Sprintf(format, args...))
}
