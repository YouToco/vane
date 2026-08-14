package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoveryCallersUseOnlyTenantCatalogForCrossTenantDiscovery(t *testing.T) {
	repoRoot := filepath.Clean("..")
	retired := map[string]struct{}{
		"ListDueAgentSessionFactTenantIDs":           {},
		"ListDueTaskCreationReceiptTenantIDs":        {},
		"ListDueTaskDefinitionEditReceiptTenantIDs":  {},
		"ListNonterminalTaskDefinitionEditTenantIDs": {},
		"ListRecoverablePushEffectTenantIDs":         {},
		"ListStaleTaskCreationTenantIDs":             {},
		"ListStaleResearchTaskCreationTenantIDsV3":   {},
		"ListStaleTaskDefinitionEditTenantIDs":       {},
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if declaration, ok := node.(*ast.FuncDecl); ok {
				if _, forbidden := retired[declaration.Name.Name]; forbidden {
					t.Errorf("retired cross-tenant recovery scan %s remains declared at %s",
						declaration.Name.Name, fset.Position(declaration.Pos()))
				}
			}
			if field, ok := node.(*ast.Field); ok {
				for _, name := range field.Names {
					if _, forbidden := retired[name.Name]; forbidden {
						t.Errorf("retired cross-tenant recovery interface method %s remains at %s",
							name.Name, fset.Position(name.Pos()))
					}
				}
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, forbidden := retired[selector.Sel.Name]; forbidden {
				t.Errorf("retired cross-tenant recovery scan %s called at %s",
					selector.Sel.Name, fset.Position(selector.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryPayloadReadersEnterExplicitTenantRole(t *testing.T) {
	expect := map[string][]string{
		"schedule_commands.go":        {"func (s *Store) ListPendingScheduleCommands(", "beginRecoveryTenantRead("},
		"schedules.go":                {"func (s *Store) ListActiveSchedules(", "beginRecoveryTenantRead("},
		"schedule_reconcile.go":       {"func (s *Store) AcquireScheduleReconcile(", "beginRecoveryTenantRead("},
		"task_creation_operations.go": {"func (s *Store) ListStaleTaskCreationOperations(", "beginRecoveryTenantRead("},
		"task_creation_v3_saga.go":    {"func (s *Store) ListStaleResearchTaskCreationOperationsV3(", "beginRecoveryTenantRead("},
		"task_creation_receipts.go":   {"func (s *Store) ListDueTaskCreationReceipts(", "beginRecoveryTenantRead("},
	}
	for name, fragments := range expect {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, fragment := range fragments {
			if !strings.Contains(text, fragment) {
				t.Errorf("%s lost recovery tenant boundary fragment %q", name, fragment)
			}
		}
	}
	raw, err := os.ReadFile("recovery_tenant_catalog.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, fragment := range []string{
		"AccessMode: pgx.ReadOnly",
		"SET LOCAL search_path=pg_catalog,public",
		"SET LOCAL ROLE vane_app",
		"FROM public.tenants",
		"WHERE id>$1",
	} {
		if !strings.Contains(text, fragment) {
			t.Errorf("recovery tenant catalog lost boundary %q", fragment)
		}
	}
}
