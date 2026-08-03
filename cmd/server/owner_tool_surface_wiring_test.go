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
// catalog fallback from restoring the retired owner tools. The one remaining
// BuildTools consumer is named and isolated as the Dashboard compatibility
// loop; A2A starts from the public-only catalog.
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
	if compatibilityBuilders != 1 {
		t.Fatalf("retained BuildTools consumers=%d, want one explicit Web compatibility loop",
			compatibilityBuilders)
	}
	if publicBuilders != 1 {
		t.Fatalf("A2A public catalog builders=%d, want one", publicBuilders)
	}
}

// TestSessionAdmissionIsSharedOnlyBySessionBearingLoops pins the composition
// boundary behind owner chat and Web Dashboard concurrency. They use separate
// Loop/catalog instances but mutate one active session per user, so both must
// receive the exact same coordinator. A2A RunOnce is sessionless and must keep
// an independent admission domain.
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
	if constructors != 1 || totalLoops != 3 || injectedLoops != 2 ||
		!ownerInjected || a2aInjected {
		t.Fatalf(
			"session admission constructors=%d loops=%d injected=%d owner=%v a2a=%v",
			constructors, totalLoops, injectedLoops, ownerInjected, a2aInjected,
		)
	}
}
