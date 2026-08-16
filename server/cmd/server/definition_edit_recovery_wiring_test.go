package main

import (
	"go/ast"
	"os"
	"strings"
	"testing"
)

func isPackageSelector(expr ast.Expr, packageName, method string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != method {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == packageName
}

func isReceiverSelector(expr ast.Expr, receiverName, method string) bool {
	return isPackageSelector(expr, receiverName, method)
}

func TestProductionRecoveryWiringIsNativeV3Only(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, forbidden := range []string{
		"task.NewCreationCoordinator(",
		"task.NewTaskDefinitionEditCoordinator(",
		"definitionEditCoordinator.",
		"ValidateTaskDefinitionEditRuntimeRoles(",
		"ValidateRuntimeEnvironment(",
		"creationCoordinator.RunRecovery(",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("production server retains legacy recovery surface %q", forbidden)
		}
	}
	ordered := []string{
		"task.NewResearchCreationCoordinatorV3(",
		"task.NewResearchTaskDefinitionEditCoordinatorV3(",
		"researchDefinitionEditCoordinator.RecoverStaleOnceV3(ctx)",
		"sched.ReconcileActions(ctx)",
		"researchDefinitionEditCoordinator.RunRecoveryV3(",
		"creationCoordinator.RunRecoveryV3(creationRecoveryCtx)",
		"w.Start()",
		"manager.Start(ctx)",
	}
	previous := -1
	for _, required := range ordered {
		index := strings.Index(source, required)
		if index < 0 || index <= previous {
			t.Fatalf("native V3 startup order missing %q: previous=%d current=%d",
				required, previous, index)
		}
		if strings.Count(source, required) != 1 {
			t.Fatalf("native V3 recovery wiring %q count=%d, want 1",
				required, strings.Count(source, required))
		}
		previous = index
	}
	// Worker startup, Telegram startup, A2A mount failure and normal shutdown
	// are the four post-recovery exits. Every one must drain both native V3
	// recovery loops before Store/Temporal teardown.
	if calls := strings.Count(source, "stopDefinitionEditRecovery()"); calls != 4 {
		t.Fatalf("stopDefinitionEditRecovery calls=%d, want four drains", calls)
	}
	if calls := strings.Count(source, "stopCreationRecovery()"); calls != 4 {
		t.Fatalf("stopCreationRecovery calls=%d, want four drains", calls)
	}
}
