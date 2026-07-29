package fetcher

import (
	"bufio"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

// These markers identify direct X/Twitter providers or X-specific credentials.
// Do not add generic "twitter" or "x api" markers: TikHub's own endpoint names
// and catalog tags legitimately contain both.
var forbiddenXProviderMarkers = []string{
	"api.twitter.com",
	"api.x.com",
	"syndication.twitter.com",
	"cdn.syndication.twimg.com",
	"publish.twitter.com",
	"twitter.com/i/api",
	"x.com/i/api",
	"twitterapi.io",
	"github.com/dghubble/go-twitter",
	"github.com/g8rswimmer/go-twitter",
	"github.com/chimeracoder/anaconda",
	"github.com/michimani/gotwi",
	"github.com/n0madic/twitter-scraper",
}

var forbiddenXCredentialName = regexp.MustCompile(
	`(?i)(?:^|[^a-z0-9])((?:x|twitter)_(?:api_(?:key|secret|token)|bearer_token|` +
		`access_token(?:_secret)?|client_(?:id|key|secret)|consumer_(?:key|secret)|` +
		`oauth_(?:token|secret)))(?:[^a-z0-9]|$)`,
)

func TestInvariant_XUserPostsUsesTikHubOnly(t *testing.T) {
	const endpointName = "twitter_web_fetch_user_post_tweet"

	template, ok := bindingTemplates[bindingKey{
		P: types.PlatformX, C: types.CapUserPosts,
	}]
	if !ok {
		t.Fatal("x/user_posts binding template is missing")
	}
	if template.Endpoint != endpointName {
		t.Fatalf("x/user_posts endpoint = %q, want TikHub endpoint %q",
			template.Endpoint, endpointName)
	}
	if len(template.Params) != 1 ||
		template.Params[0].Key != "screen_name" ||
		template.Params[0].FromConfig != "screen_name" ||
		!template.Params[0].Required {
		t.Fatalf("x/user_posts must require only screen_name: %+v", template.Params)
	}

	entry, ok := tikhubcatalog.Lookup(endpointName)
	if !ok {
		t.Fatalf("TikHub catalog endpoint %q is missing", endpointName)
	}
	if entry.Method != "GET" ||
		entry.Path != "/api/v1/twitter/web/fetch_user_post_tweet" ||
		entry.Platform != "twitter" {
		t.Fatalf("unexpected TikHub catalog entry: %+v", entry)
	}

	routes, err := NewRuntimeFetchRoutesV1(config.FetchConfig{}, nil, nil)
	if err != nil {
		t.Fatalf("build runtime routes: %v", err)
	}
	for _, route := range routes {
		capability := route.Capability
		if capability.Platform != string(types.PlatformX) ||
			capability.Capability != string(types.CapUserPosts) {
			continue
		}
		if capability.ImplementationVersion != runtimepolicy.CapabilityImplementationBindingV1 {
			t.Fatalf("x/user_posts implementation = %q, want %q",
				capability.ImplementationVersion,
				runtimepolicy.CapabilityImplementationBindingV1)
		}
		if capability.CredentialRef.ID != runtimepolicy.CredentialIDTikHubPrimaryV1 {
			t.Fatalf("x/user_posts credential = %q, want %q",
				capability.CredentialRef.ID,
				runtimepolicy.CredentialIDTikHubPrimaryV1)
		}
		if route.Binding == nil || route.RSS != nil ||
			route.ExaSearch != nil || route.ExaContents != nil {
			t.Fatal("x/user_posts must have exactly one binding executor")
		}
		return
	}
	t.Fatal("x/user_posts runtime route is missing")
}

func TestInvariant_NoDirectXProviderInProduction(t *testing.T) {
	root := repositoryRoot(t)
	var violations []string

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "testdata":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !isProductionProviderSurface(path) {
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".go") {
			goViolations, err := scanProductionGoFile(path)
			if err != nil {
				return err
			}
			for _, violation := range goViolations {
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					relative = path
				}
				violations = append(violations, relative+":"+violation)
			}
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		lineNumber := 0
		for scanner.Scan() {
			lineNumber++
			if marker, found := forbiddenXProviderMarker(scanner.Text()); found {
				relative, relErr := filepath.Rel(root, path)
				if relErr != nil {
					relative = path
				}
				violations = append(violations,
					relative+":"+strconv.Itoa(lineNumber)+": "+marker)
			}
		}
		scanErr := scanner.Err()
		closeErr := file.Close()
		if scanErr != nil {
			return scanErr
		}
		return closeErr
	})
	if err != nil {
		t.Fatalf("scan production provider surface: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("direct X/Twitter provider references are forbidden; use TikHub only:\n%s",
			strings.Join(violations, "\n"))
	}
}

func TestForbiddenXProviderMarker(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		blocked bool
	}{
		{"TikHub API", "https://api.tikhub.io/api/v1/twitter/web/fetch_user_post_tweet", false},
		{"TikHub endpoint", "twitter_web_fetch_user_post_tweet", false},
		{"content permalink", "https://x.com/OpenAI/status/123", false},
		{"Exa header", "x-api-key", false},
		{"official Twitter API", "https://api.twitter.com/2/users", true},
		{"official X API", "https://api.x.com/2/users", true},
		{"syndication API", "https://syndication.twitter.com/example", true},
		{"official credential", "VANE_TWITTER_BEARER_TOKEN", true},
		{"official api token", "X_API_TOKEN", true},
		{"official client ID", "VANE_X_CLIENT_ID", true},
		{"official consumer secret", "TWITTER_CONSUMER_SECRET", true},
		{"unrelated key suffix", "PREFIX_API_KEY", false},
		{"unrelated secret suffix", "LINUX_CLIENT_SECRET", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marker, blocked := forbiddenXProviderMarker(tt.value)
			if blocked != tt.blocked {
				t.Fatalf("forbiddenXProviderMarker(%q) = (%q, %v), want blocked=%v",
					tt.value, marker, blocked, tt.blocked)
			}
		})
	}
}

func forbiddenXProviderMarker(value string) (string, bool) {
	lower := strings.ToLower(value)
	for _, marker := range forbiddenXProviderMarkers {
		if strings.Contains(lower, marker) {
			return marker, true
		}
	}
	if match := forbiddenXCredentialName.FindStringSubmatch(value); len(match) > 1 {
		return strings.ToLower(match[1]), true
	}
	return "", false
}

func isProductionProviderSurface(path string) bool {
	name := filepath.Base(path)
	lowerName := strings.ToLower(name)
	if strings.HasSuffix(lowerName, "_test.go") {
		return false
	}
	switch filepath.Ext(lowerName) {
	case ".go", ".json", ".yaml", ".yml", ".toml", ".sh", ".ps1", ".service", ".conf":
		return true
	}
	return strings.HasPrefix(lowerName, ".env") ||
		strings.HasPrefix(lowerName, "dockerfile") ||
		lowerName == "makefile" ||
		lowerName == "go.mod" ||
		lowerName == "go.sum"
}

func scanProductionGoFile(path string) ([]string, error) {
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, nil, 0)
	if err != nil {
		return nil, err
	}
	tikHubAliases := make(map[string]struct{})
	var violations []string
	for _, spec := range parsed.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		if importPath != "github.com/YouToco/vane/tikhubinvoke" {
			continue
		}
		alias := "tikhubinvoke"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		if alias == "." {
			position := files.Position(spec.Pos())
			violations = append(violations,
				strconv.Itoa(position.Line)+": tikhubinvoke dot import")
			continue
		}
		tikHubAliases[alias] = struct{}{}
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			if value.Sel.Name != "WithBaseURL" {
				break
			}
			qualifier, ok := value.X.(*ast.Ident)
			if !ok {
				break
			}
			if _, ok := tikHubAliases[qualifier.Name]; ok {
				position := files.Position(value.Pos())
				violations = append(violations,
					strconv.Itoa(position.Line)+": tikhubinvoke.WithBaseURL")
			}
		case *ast.BinaryExpr:
			if value.Op != token.ADD {
				break
			}
			constant, ok := constantString(value)
			if !ok {
				break
			}
			if marker, found := forbiddenXProviderMarker(constant); found {
				position := files.Position(value.Pos())
				violations = append(violations,
					strconv.Itoa(position.Line)+": constant "+marker)
			}
		}
		return true
	})
	return violations, nil
}

func constantString(expression ast.Expr) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		unquoted, err := strconv.Unquote(value.Value)
		return unquoted, err == nil
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := constantString(value.X)
		right, rightOK := constantString(value.Y)
		return left + right, leftOK && rightOK
	default:
		return "", false
	}
}

func TestInvariant_XTikHubFailureDoesNotFallBack(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		delay      time.Duration
		redirect   bool
		contextTTL time.Duration
	}{
		{"unauthorized", http.StatusUnauthorized, `{"detail":"unauthorized"}`, 0, false, 0},
		{"forbidden", http.StatusForbidden, `{"detail":"forbidden"}`, 0, false, 0},
		{"rate limited", http.StatusTooManyRequests, `{"detail":"slow down"}`, 0, false, 0},
		{"server error", http.StatusInternalServerError, `{"detail":"upstream"}`, 0, false, 0},
		{"bad JSON", http.StatusOK, `{`, 0, false, 0},
		{"bad envelope", http.StatusOK, `{"code":500,"data":null}`, 0, false, 0},
		{"timeout", http.StatusOK, `{"code":200}`, 200 * time.Millisecond, false, 20 * time.Millisecond},
		{"redirect", http.StatusFound, "", 0, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var targetRequests atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				targetRequests.Add(1)
			}))
			defer target.Close()

			var tikHubRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				tikHubRequests.Add(1)
				if tt.delay > 0 {
					time.Sleep(tt.delay)
				}
				if tt.redirect {
					w.Header().Set("Location", target.URL)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			binding := NewBinding(
				config.FetchConfig{TikhubAPIKey: "test-key", TimeoutSeconds: 1},
				nil,
				nil,
				tikhubinvoke.WithBaseURL(server.URL),
			)
			multi := &Multi{binding: binding}
			ctx := context.Background()
			if tt.contextTTL > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.contextTTL)
				defer cancel()
			}
			_, err := multi.Fetch(ctx, types.FetchTarget{
				ID:         7,
				Platform:   types.PlatformX,
				Capability: types.CapUserPosts,
				Config:     []byte(`{"screen_name":"OpenAI"}`),
			})
			if err == nil {
				t.Fatal("TikHub failure must be explicit, got nil")
			}
			if got := tikHubRequests.Load(); got != 1 {
				t.Fatalf("TikHub attempts = %d, want exactly 1", got)
			}
			if got := targetRequests.Load(); got != 0 {
				t.Fatalf("redirect/fallback target requests = %d, want 0", got)
			}
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	return filepath.Dir(filepath.Dir(filename))
}
