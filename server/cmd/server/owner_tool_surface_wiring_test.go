package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
)

// TestOwnerToolSurfaceIsACompositionInvariant prevents a deployment flag or
// catalog fallback from restoring the retired owner tools. Dashboard and
// Feishu share the single owner loop; A2A starts from the public-only catalog.
func TestOwnerToolSurfaceIsACompositionInvariant(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate owner tool surface wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ownerBuilders, compatibilityBuilders, publicBuilders, ownerLanes int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch {
		case isPackageSelector(call.Fun, "agent", "BuildOwnerTools"):
			ownerBuilders++
		case isPackageSelector(call.Fun, "agent", "BuildTools"):
			compatibilityBuilders++
		case isPackageSelector(call.Fun, "agent", "BuildPublicResearchTools"):
			publicBuilders++
		case isPackageSelector(call.Fun, "agent", "NewChecked") && len(call.Args) == 1:
			literal, ok := call.Args[0].(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, keyOK := pair.Key.(*ast.Ident)
				value, valueOK := pair.Value.(*ast.Ident)
				if keyOK && valueOK && key.Name == "OwnerAgent" && value.Name == "true" {
					ownerLanes++
				}
			}
		}
		return true
	})
	if ownerBuilders != 1 || ownerLanes != 1 {
		t.Fatalf("owner builders/lanes=%d/%d, want one explicit invariant",
			ownerBuilders, ownerLanes)
	}
	if compatibilityBuilders != 0 {
		t.Fatalf("retired BuildTools consumers=%d, want zero",
			compatibilityBuilders)
	}
	if publicBuilders != 1 {
		t.Fatalf("A2A public catalog builders=%d, want one", publicBuilders)
	}
}

func TestA2AUsesOnlyAuthorizedPublicResearchCatalog(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate A2A tool surface wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	matches := 0
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isPackageSelector(call.Fun, "agent", "FilterAuthorizedTools") {
			return true
		}
		if len(call.Args) != 2 {
			t.Fatalf("FilterAuthorizedTools args=%d, want public catalog and scope",
				len(call.Args))
		}
		catalog, ok := call.Args[0].(*ast.CallExpr)
		if !ok || !isPackageSelector(catalog.Fun, "agent", "BuildPublicResearchTools") {
			t.Fatalf("A2A source catalog=%T, want BuildPublicResearchTools", call.Args[0])
		}
		scope, ok := call.Args[1].(*ast.SelectorExpr)
		if !ok {
			t.Fatalf("A2A authorization scope=%T, want AuthorizationA2AReadOnly",
				call.Args[1])
		}
		pkg, pkgOK := scope.X.(*ast.Ident)
		if !pkgOK || pkg.Name != "agent" ||
			scope.Sel.Name != "AuthorizationA2AReadOnly" {
			t.Fatalf("A2A authorization scope=%T, want AuthorizationA2AReadOnly",
				call.Args[1])
		}
		matches++
		return true
	})
	if matches != 1 {
		t.Fatalf("authorized A2A public catalog compositions=%d, want one", matches)
	}
}

// TestOwnerRuntimeGatePrecedesAllDurableAssembly proves the process cannot
// expose unconditional manage_tasks create while its Research V3 worker is
// dark. The Gate must run before the first Store is opened, and the owner
// catalog itself must remain unconditional rather than falling back to a
// canary or a hidden create tool.
func TestOwnerRuntimeGatePrecedesAllDurableAssembly(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate owner runtime Gate wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var gatePos, ownerBuilderPos token.Pos
	var firstStorePos token.Pos
	var conditionals []*ast.IfStmt
	ast.Inspect(file, func(node ast.Node) bool {
		switch current := node.(type) {
		case *ast.IfStmt:
			conditionals = append(conditionals, current)
		case *ast.CallExpr:
			switch {
			case isIdentCall(current.Fun, "requireOwnerAgentResearchV3Runtime"):
				if gatePos != token.NoPos {
					t.Fatal("owner runtime Gate is called more than once")
				}
				gatePos = current.Pos()
			case isPackageSelector(current.Fun, "store", "New"),
				isPackageSelector(current.Fun, "store", "NewServerRuntimeWithResearchRuntimeCapabilityAndEditRecovery"):
				if firstStorePos == token.NoPos || current.Pos() < firstStorePos {
					firstStorePos = current.Pos()
				}
			case isPackageSelector(current.Fun, "agent", "BuildOwnerTools"):
				ownerBuilderPos = current.Pos()
			}
		}
		return true
	})
	if gatePos == token.NoPos || firstStorePos == token.NoPos ||
		ownerBuilderPos == token.NoPos || gatePos >= firstStorePos ||
		gatePos >= ownerBuilderPos {
		t.Fatalf("unsafe startup order: gate=%d firstStore=%d ownerBuilder=%d",
			gatePos, firstStorePos, ownerBuilderPos)
	}
	for _, conditional := range conditionals {
		if conditional.Pos() < ownerBuilderPos && ownerBuilderPos < conditional.End() {
			t.Fatalf("owner BuildOwnerTools is conditional at %d; runtime Gate must guard startup instead",
				conditional.Pos())
		}
	}
}

func isIdentCall(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}

// TestSessionAdmissionIsSharedOnlyBySessionBearingLoops pins the composition
// boundary behind the single owner loop used by both chat and Dashboard. A2A
// RunOnce is sessionless and must keep an independent admission domain.
func TestSessionAdmissionIsSharedOnlyBySessionBearingLoops(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate session admission wiring test")
	}
	file, err := parser.ParseFile(token.NewFileSet(),
		filepath.Join(filepath.Dir(testFile), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	const coordinator = "sessionAdmission"
	constructors := 0
	totalLoops := 0
	injectedLoops := 0
	ownerInjected := false
	a2aInjected := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch current := node.(type) {
		case *ast.AssignStmt:
			if len(current.Lhs) != 1 || len(current.Rhs) != 1 {
				return true
			}
			name, nameOK := current.Lhs[0].(*ast.Ident)
			call, callOK := current.Rhs[0].(*ast.CallExpr)
			if nameOK && callOK && name.Name == coordinator &&
				isPackageSelector(call.Fun, "agent", "NewSessionAdmissionCoordinator") {
				constructors++
			}
		case *ast.CallExpr:
			if !isPackageSelector(current.Fun, "agent", "NewChecked") ||
				len(current.Args) != 1 {
				return true
			}
			literal, ok := current.Args[0].(*ast.CompositeLit)
			if !ok {
				return true
			}
			totalLoops++
			injected := false
			owner := false
			a2a := false
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := pair.Key.(*ast.Ident)
				if !ok {
					continue
				}
				switch key.Name {
				case "SessionAdmission":
					value, ok := pair.Value.(*ast.Ident)
					injected = ok && value.Name == coordinator
				case "OwnerAgent":
					value, ok := pair.Value.(*ast.Ident)
					owner = ok && value.Name == "true"
				case "SystemPrompt":
					a2a = true
				}
			}
			if injected {
				injectedLoops++
				ownerInjected = ownerInjected || owner
			}
			if a2a && injected {
				a2aInjected = true
			}
		}
		return true
	})
	if constructors != 1 || totalLoops != 2 || injectedLoops != 1 ||
		!ownerInjected || a2aInjected {
		t.Fatalf(
			"session admission constructors=%d loops=%d injected=%d owner=%v a2a=%v",
			constructors, totalLoops, injectedLoops, ownerInjected, a2aInjected,
		)
	}
}
