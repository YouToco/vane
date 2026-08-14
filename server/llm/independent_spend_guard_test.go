package llm

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type observationShadowReference struct {
	file   string
	symbol string
	kind   string
}

type privateObservationShadowCall struct {
	file   string
	caller string
	callee string
	mode   string
}

// TestInvariant_ObservationShadowUsesDedicatedAPIs keeps operator-funded model
// spend off the normal exported request and CallMeta surfaces. Counting every
// production reference (not only calls) also rejects function-value aliases.
func TestInvariant_ObservationShadowUsesDedicatedAPIs(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate observation shadow guard")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	var references []observationShadowReference
	var privateCalls []privateObservationShadowCall
	fset := token.NewFileSet()
	err := filepath.WalkDir(repoRoot, func(
		path string, entry fs.DirEntry, err error,
	) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		references = append(references, findObservationShadowReferences(
			file, filepath.ToSlash(relative))...)
		privateCalls = append(privateCalls, findPrivateObservationShadowCalls(
			file, filepath.ToSlash(relative))...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []observationShadowReference{
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "qualify",
			kind:   "selector",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "QualifyObservationShadow",
			kind:   "definition",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "QualifyObservationShadow",
			kind:   "identifier",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "qualify",
			kind:   "selector",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "qualify",
			kind:   "definition",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "qualify",
			kind:   "identifier",
		},
		{
			file:   "eventqualifier/qualifier.go",
			symbol: "DoObservationShadow",
			kind:   "selector",
		},
		{
			file:   "llm/do.go",
			symbol: "do",
			kind:   "identifier",
		},
		{
			file:   "llm/do.go",
			symbol: "DoObservationShadow",
			kind:   "definition",
		},
		{
			file:   "llm/do.go",
			symbol: "DoObservationShadow",
			kind:   "identifier",
		},
		{
			file:   "llm/do.go",
			symbol: "do",
			kind:   "identifier",
		},
		{
			file:   "llm/do.go",
			symbol: "do",
			kind:   "definition",
		},
		{
			file:   "llm/do.go",
			symbol: "do",
			kind:   "identifier",
		},
		{
			file:   "workflow/activities.go",
			symbol: "QualifyObservationShadow",
			kind:   "selector",
		},
	}
	if !equalObservationShadowReferences(references, want) {
		t.Fatalf("observation shadow production references=%v, want %v",
			references, want)
	}

	wantPrivateCalls := []privateObservationShadowCall{
		{
			file: "eventqualifier/qualifier.go", caller: "Qualify",
			callee: "qualify", mode: "normal",
		},
		{
			file:   "eventqualifier/qualifier.go",
			caller: "QualifyObservationShadow",
			callee: "qualify", mode: "shadow",
		},
		{
			file: "llm/do.go", caller: "Do",
			callee: "do", mode: "normal",
		},
		{
			file: "llm/do.go", caller: "DoObservationShadow",
			callee: "do", mode: "shadow",
		},
	}
	if !equalPrivateObservationShadowCalls(privateCalls, wantPrivateCalls) {
		t.Fatalf("private observation shadow calls=%v, want %v",
			privateCalls, wantPrivateCalls)
	}
}

func TestObservationShadowGuardRejectsAliasMutations(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   []observationShadowReference
	}{
		{
			name: "selector call",
			source: `package workflow
func f(q interface{ QualifyObservationShadow() }) { q.QualifyObservationShadow() }`,
			want: []observationShadowReference{{
				file: "mutation.go", symbol: "QualifyObservationShadow", kind: "selector",
			}},
		},
		{
			name: "selector function value alias",
			source: `package eventqualifier
import "github.com/YouToco/vane/llm"
var shadow = llm.DoObservationShadow`,
			want: []observationShadowReference{{
				file: "mutation.go", symbol: "DoObservationShadow", kind: "selector",
			}},
		},
		{
			name: "same package function value alias",
			source: `package llm
var shadow = DoObservationShadow`,
			want: []observationShadowReference{{
				file: "mutation.go", symbol: "DoObservationShadow", kind: "identifier",
			}},
		},
		{
			name: "private llm function value alias",
			source: `package llm
var shadow = do`,
			want: []observationShadowReference{{
				file: "mutation.go", symbol: "do", kind: "identifier",
			}},
		},
		{
			name: "private qualifier function value alias",
			source: `package eventqualifier
func alias(q *Qualifier) { _ = q.qualify }`,
			want: []observationShadowReference{{
				file: "mutation.go", symbol: "qualify", kind: "selector",
			}},
		},
		{
			name: "unrelated symbol",
			source: `package workflow
func DoShadow() {}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := parser.ParseFile(
				token.NewFileSet(), "mutation.go", tt.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := findObservationShadowReferences(file, "mutation.go")
			if !equalObservationShadowReferences(got, tt.want) {
				t.Fatalf("references=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestObservationShadowGuardRejectsDirectPrivateCallMutations(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   []privateObservationShadowCall
	}{
		{
			name: "llm direct shadow call",
			source: `package llm
func bypass() { do(nil, nil, nil, CallMeta{}, Request{}, observationShadowSpend(nil)) }`,
			want: []privateObservationShadowCall{{
				file: "mutation.go", caller: "bypass",
				callee: "do", mode: "shadow",
			}},
		},
		{
			name: "qualifier direct shadow call",
			source: `package eventqualifier
func (q *Qualifier) bypass() { q.qualify(nil, Request{}, true) }`,
			want: []privateObservationShadowCall{{
				file: "mutation.go", caller: "bypass",
				callee: "qualify", mode: "shadow",
			}},
		},
		{
			name: "llm unexpected gate expression",
			source: `package llm
func bypass() { do(nil, nil, nil, CallMeta{}, Request{}, gate) }`,
			want: []privateObservationShadowCall{{
				file: "mutation.go", caller: "bypass",
				callee: "do", mode: "invalid",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := parser.ParseFile(
				token.NewFileSet(), "mutation.go", tt.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := findPrivateObservationShadowCalls(file, "mutation.go")
			if !equalPrivateObservationShadowCalls(got, tt.want) {
				t.Fatalf("private calls=%v, want %v", got, tt.want)
			}
		})
	}
}

func findObservationShadowReferences(
	file *ast.File,
	path string,
) []observationShadowReference {
	var references []observationShadowReference
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.FuncDecl:
			if isObservationShadowSymbol(typed.Name.Name) ||
				isPrivateObservationShadowSymbol(file.Name.Name, typed.Name.Name) {
				references = append(references, observationShadowReference{
					file: path, symbol: typed.Name.Name, kind: "definition",
				})
			}
		case *ast.SelectorExpr:
			if isObservationShadowSymbol(typed.Sel.Name) ||
				(file.Name.Name == "eventqualifier" &&
					typed.Sel.Name == "qualify") {
				references = append(references, observationShadowReference{
					file: path, symbol: typed.Sel.Name, kind: "selector",
				})
			}
			return false
		case *ast.Ident:
			if (file.Name.Name == "llm" && typed.Name == "DoObservationShadow") ||
				(file.Name.Name == "eventqualifier" &&
					typed.Name == "QualifyObservationShadow") ||
				isPrivateObservationShadowSymbol(file.Name.Name, typed.Name) {
				references = append(references, observationShadowReference{
					file: path, symbol: typed.Name, kind: "identifier",
				})
			}
		}
		return true
	})
	return references
}

func isObservationShadowSymbol(name string) bool {
	return name == "DoObservationShadow" ||
		name == "QualifyObservationShadow"
}

func isPrivateObservationShadowSymbol(packageName, name string) bool {
	return (packageName == "llm" && name == "do") ||
		(packageName == "eventqualifier" && name == "qualify")
}

func findPrivateObservationShadowCalls(
	file *ast.File,
	path string,
) []privateObservationShadowCall {
	var calls []privateObservationShadowCall
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch {
			case file.Name.Name == "llm" && calledIdent(call.Fun, "do"):
				calls = append(calls, privateObservationShadowCall{
					file: path, caller: function.Name.Name,
					callee: "do", mode: classifyPrivateDoCall(call),
				})
			case file.Name.Name == "eventqualifier" &&
				calledSelector(call.Fun, "qualify"):
				calls = append(calls, privateObservationShadowCall{
					file: path, caller: function.Name.Name,
					callee: "qualify", mode: classifyPrivateQualifyCall(call),
				})
			}
			return true
		})
	}
	return calls
}

func calledIdent(expr ast.Expr, name string) bool {
	identifier, ok := expr.(*ast.Ident)
	return ok && identifier.Name == name
}

func calledSelector(expr ast.Expr, name string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == name
}

func classifyPrivateDoCall(call *ast.CallExpr) string {
	if len(call.Args) == 0 {
		return "invalid"
	}
	last := call.Args[len(call.Args)-1]
	if identifier, ok := last.(*ast.Ident); ok && identifier.Name == "nil" {
		return "normal"
	}
	conversion, ok := last.(*ast.CallExpr)
	if ok && calledIdent(conversion.Fun, "observationShadowSpend") {
		return "shadow"
	}
	return "invalid"
}

func classifyPrivateQualifyCall(call *ast.CallExpr) string {
	if len(call.Args) == 0 {
		return "invalid"
	}
	last, ok := call.Args[len(call.Args)-1].(*ast.Ident)
	if !ok {
		return "invalid"
	}
	switch last.Name {
	case "false":
		return "normal"
	case "true":
		return "shadow"
	default:
		return "invalid"
	}
}

func equalObservationShadowReferences(
	got, want []observationShadowReference,
) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func equalPrivateObservationShadowCalls(
	got, want []privateObservationShadowCall,
) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
