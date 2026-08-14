package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

const goldenTaskRunSnapshotReferenceEnvelopeV1 = `{"schema_version":"vane.run-snapshot-ref/v1","snapshot_id":41,"identity":{"temporal_workflow_id":"wf-task-golden","temporal_run_id":"run-golden","run_kind":"scheduled","tenant_id":7,"user_id":11,"task_id":"task-golden"},"mode":"compiled","definition_digest":"1111111111111111111111111111111111111111111111111111111111111111","plan_digest":"2222222222222222222222222222222222222222222222222222222222222222","adaptive_version":0,"policy":{"capability_catalog_digest":"3333333333333333333333333333333333333333333333333333333333333333","tool_policy_digest":"4444444444444444444444444444444444444444444444444444444444444444","prompt_policy_digest":"5555555555555555555555555555555555555555555555555555555555555555","model_policy_digest":"6666666666666666666666666666666666666666666666666666666666666666","quota_policy_digest":"7777777777777777777777777777777777777777777777777777777777777777"},"planner_budget":{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0},"payload_digest":"8888888888888888888888888888888888888888888888888888888888888888"}`

const goldenTaskRunSnapshotReferenceDigestV1 = "2c592a9359a53458cc9460abe890a17874ac680193e938477fcd602e59dfcedc"

func goldenTaskRunSnapshotRowV1() taskRunSnapshot {
	return taskRunSnapshot{
		ID:                      41,
		TenantID:                7,
		UserID:                  11,
		TaskID:                  "task-golden",
		TemporalWorkflowID:      "wf-task-golden",
		TemporalRunID:           "run-golden",
		RunKind:                 types.RunSnapshotKind("scheduled"),
		Mode:                    types.ExecutionMode("compiled"),
		AdaptiveVersion:         0,
		CapabilityCatalogDigest: strings.Repeat("3", sha256.Size*2),
		ToolPolicyDigest:        strings.Repeat("4", sha256.Size*2),
		PromptPolicyDigest:      strings.Repeat("5", sha256.Size*2),
		ModelPolicyDigest:       strings.Repeat("6", sha256.Size*2),
		QuotaPolicyDigest:       strings.Repeat("7", sha256.Size*2),
		DefinitionDigest:        strings.Repeat("1", sha256.Size*2),
		PlanDigest:              strings.Repeat("2", sha256.Size*2),
		PayloadDigest:           strings.Repeat("8", sha256.Size*2),
		ReferenceDigest:         goldenTaskRunSnapshotReferenceDigestV1,
		ReferenceSchemaVersion:  "vane.run-snapshot-ref/v1",
		BudgetJSON: json.RawMessage(
			`{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0}`,
		),
	}
}

func TestValidScheduledTaskWorkflowExecutionIDV1(t *testing.T) {
	const taskID = "task-v1-0123456789abcdef"
	base := taskRunScheduledWorkflowPrefixV1 + taskID
	tests := []struct {
		name       string
		taskID     string
		workflowID string
		want       bool
	}{
		{name: "retained bare action ID", taskID: taskID, workflowID: base, want: true},
		{
			name:       "Temporal canonical UTC second",
			taskID:     taskID,
			workflowID: base + "-2026-07-24T15:52:40Z",
			want:       true,
		},
		{
			name:       "task ID may contain timestamp-like text",
			taskID:     taskID + "-2025-01-02T03:04:05Z",
			workflowID: base + "-2025-01-02T03:04:05Z-2026-07-24T15:52:40Z",
			want:       true,
		},
		{name: "empty task", workflowID: "wf-", want: false},
		{name: "wrong task", taskID: taskID, workflowID: "wf-other-2026-07-24T15:52:40Z"},
		{name: "broad prefix only", taskID: taskID, workflowID: base + "-attacker"},
		{name: "missing suffix", taskID: taskID, workflowID: base + "-"},
		{name: "lowercase UTC marker", taskID: taskID, workflowID: base + "-2026-07-24T15:52:40z"},
		{name: "numeric UTC offset", taskID: taskID, workflowID: base + "-2026-07-24T15:52:40+00:00"},
		{name: "fractional second", taskID: taskID, workflowID: base + "-2026-07-24T15:52:40.001Z"},
		{name: "invalid calendar date", taskID: taskID, workflowID: base + "-2026-02-30T15:52:40Z"},
		{name: "trailing bytes", taskID: taskID, workflowID: base + "-2026-07-24T15:52:40Z-extra"},
		{name: "control byte", taskID: taskID, workflowID: base + "-2026-07-24T15:52:40Z\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validScheduledTaskWorkflowExecutionIDV1(
				test.taskID, test.workflowID); got != test.want {
				t.Fatalf("validScheduledTaskWorkflowExecutionIDV1(%q, %q) = %v, want %v",
					test.taskID, test.workflowID, got, test.want)
			}
		})
	}
}

func TestValidTaskRunWorkflowExecutionIDV1_ManualCommandIdentity(t *testing.T) {
	const commandID = "3b5af7c5-229f-4542-b493-e17ff90593de"
	valid := types.ManualTaskWorkflowPrefix + commandID
	timestamped := valid + "-2026-07-30T12:24:59Z"
	tests := []struct {
		name       string
		workflowID string
		want       bool
	}{
		{name: "exact lowercase command UUID", workflowID: valid, want: true},
		{name: "timestamped lowercase command UUID", workflowID: timestamped, want: true},
		{name: "missing command", workflowID: types.ManualTaskWorkflowPrefix},
		{name: "non UUID", workflowID: types.ManualTaskWorkflowPrefix + "manual"},
		{name: "uppercase UUID", workflowID: types.ManualTaskWorkflowPrefix +
			strings.ToUpper(commandID)},
		{name: "trailing bytes", workflowID: valid + "-extra"},
		{name: "timestamp offset", workflowID: valid +
			"-2026-07-30T12:24:59+01:00"},
		{name: "timestamp trailing bytes", workflowID: timestamped + "-extra"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validTaskRunWorkflowExecutionIDV1(
				"task-any", test.workflowID); got != test.want {
				t.Fatalf("manual workflow validity=%v want=%v for %q",
					got, test.want, test.workflowID)
			}
		})
	}
}

// This is a hand-authored persisted row and reference envelope. Neither the
// expected bytes nor digest are produced by the current types contract, so an
// incompatible v1 representation change fails before old rows reach runtime.
func TestTaskRunSnapshotReferenceV1Golden(t *testing.T) {
	sum := sha256.Sum256([]byte(goldenTaskRunSnapshotReferenceEnvelopeV1))
	if got := hex.EncodeToString(sum[:]); got != goldenTaskRunSnapshotReferenceDigestV1 {
		t.Fatalf("test fixture digest is wrong: got %s want %s",
			got, goldenTaskRunSnapshotReferenceDigestV1)
	}

	row := goldenTaskRunSnapshotRowV1()
	got, err := row.safeRef()
	if err != nil {
		t.Fatalf("read hand-pinned v1 reference: %v", err)
	}
	want := types.RunSnapshotRef{
		SchemaVersion:      "vane.run-snapshot-ref/v1",
		SnapshotID:         41,
		TemporalWorkflowID: "wf-task-golden",
		TemporalRunID:      "run-golden",
		RunKind:            types.RunSnapshotKind("scheduled"),
		TenantID:           7,
		UserID:             11,
		TaskID:             "task-golden",
		Mode:               types.ExecutionMode("compiled"),
		DefinitionDigest:   strings.Repeat("1", sha256.Size*2),
		PlanDigest:         strings.Repeat("2", sha256.Size*2),
		AdaptiveVersion:    0,
		Policy: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: strings.Repeat("3", sha256.Size*2),
			ToolPolicyDigest:        strings.Repeat("4", sha256.Size*2),
			PromptPolicyDigest:      strings.Repeat("5", sha256.Size*2),
			ModelPolicyDigest:       strings.Repeat("6", sha256.Size*2),
			QuotaPolicyDigest:       strings.Repeat("7", sha256.Size*2),
		},
		PlannerBudget:   types.PlannerBudget{},
		PayloadDigest:   strings.Repeat("8", sha256.Size*2),
		ReferenceDigest: goldenTaskRunSnapshotReferenceDigestV1,
	}
	if got != want {
		t.Fatalf("pinned v1 projection drifted:\n got %#v\nwant %#v", got, want)
	}
}

func TestTaskRunSnapshotReferenceV1RejectsCorruption(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*taskRunSnapshot)
	}{
		{
			name: "unknown schema",
			mutate: func(row *taskRunSnapshot) {
				row.ReferenceSchemaVersion = "vane.run-snapshot-ref/v2"
			},
		},
		{
			name: "reference digest mismatch",
			mutate: func(row *taskRunSnapshot) {
				row.ReferenceDigest = strings.Repeat("9", sha256.Size*2)
			},
		},
		{
			name: "bound field mutation",
			mutate: func(row *taskRunSnapshot) {
				row.DefinitionDigest = strings.Repeat("a", sha256.Size*2)
			},
		},
		{
			name: "zero content digest",
			mutate: func(row *taskRunSnapshot) {
				row.PayloadDigest = strings.Repeat("0", sha256.Size*2)
			},
		},
		{
			name: "nonzero compiled budget",
			mutate: func(row *taskRunSnapshot) {
				row.BudgetJSON = json.RawMessage(
					`{"max_planner_rounds":1,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0}`,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := goldenTaskRunSnapshotRowV1()
			tt.mutate(&row)
			if got, err := row.safeRef(); err == nil {
				t.Fatalf("corrupt v1 row must fail closed, got %#v", got)
			}
		})
	}
}

func TestTaskRunSnapshotReferenceV1LegalTextBoundaries(t *testing.T) {
	row := goldenTaskRunSnapshotRowV1()
	row.TemporalWorkflowID = strings.Repeat("w", maxTaskRunReferenceBytesV1)
	row.TemporalRunID = strings.Repeat("r", maxTaskRunReferenceBytesV1)
	row.TaskID = strings.Repeat("t", maxTaskRunReferenceTaskIDV1)
	sealed, err := sealTaskRunSnapshotReferenceV1(&row, taskRunBudgetV1{})
	if err != nil {
		t.Fatalf("seal legal v1 reference text maxima: %v", err)
	}
	row.ReferenceDigest = sealed.ReferenceDigest
	if _, err := row.safeRef(); err != nil {
		t.Fatalf("legal v1 reference text maxima became unreadable: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*taskRunSnapshot)
	}{
		{"workflow id", func(s *taskRunSnapshot) { s.TemporalWorkflowID += "x" }},
		{"run id", func(s *taskRunSnapshot) { s.TemporalRunID += "x" }},
		{"task id", func(s *taskRunSnapshot) { s.TaskID += "x" }},
	}
	for _, tt := range tests {
		t.Run(tt.name+" max+1", func(t *testing.T) {
			over := row
			tt.mutate(&over)
			if _, err := sealTaskRunSnapshotReferenceV1(&over, taskRunBudgetV1{}); err == nil {
				t.Fatalf("v1 reference accepted %s at max+1", tt.name)
			}
		})
	}
}

func TestReadTaskRunBudgetV1RejectsWireDrift(t *testing.T) {
	const canonical = `{"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,"max_cost_micro_usd":0,"duration_ms":0}`
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "canonical v1", raw: canonical, ok: true},
		{
			name: "case-folded alias",
			raw:  strings.Replace(canonical, `"max_planner_rounds"`, `"MAX_PLANNER_ROUNDS"`, 1),
		},
		{
			name: "escaped alias",
			raw:  strings.Replace(canonical, `"max_planner_rounds"`, `"\u006dax_planner_rounds"`, 1),
		},
		{
			name: "unknown field",
			raw:  strings.Replace(canonical, `}`, `,"future_limit":0}`, 1),
		},
		{
			name: "duplicate field",
			raw:  strings.Replace(canonical, `"max_tool_calls":0`, `"max_tool_calls":0,"max_tool_calls":0`, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget, encoded, err := readTaskRunBudgetV1(json.RawMessage(tt.raw))
			if tt.ok {
				if err != nil {
					t.Fatalf("read canonical v1 budget: %v", err)
				}
				if budget != (taskRunBudgetV1{}) || string(encoded) != canonical {
					t.Fatalf("unexpected canonical v1 budget: budget=%+v encoded=%s", budget, encoded)
				}
				return
			}
			if err == nil {
				t.Fatalf("v1 budget wire drift must fail closed: budget=%+v encoded=%s", budget, encoded)
			}
		})
	}
}

func TestTaskRunSnapshotReferenceV1ReaderIsPinnedByAST(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate reference test")
	}
	sourceFile := strings.TrimSuffix(thisFile, "_test.go") + ".go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse v1 reference reader: %v", err)
	}

	forbiddenIdentifiers := map[string]struct{}{
		"canonicalTaskRunBudget":        {},
		"maxTaskRunReferenceBytes":      {},
		"maxTaskRunJSONBytes":           {},
		"taskRunSnapshotPayloadVersion": {},
	}
	forbiddenTypeSelectors := map[string]struct{}{
		"ExecutionModeCompiled":    {},
		"RunSnapshotKindScheduled": {},
		"RunSnapshotSchemaVersion": {},
	}
	currentDTOSelectors := map[string]struct{}{
		"ExecutionMode":        {},
		"PlannerBudget":        {},
		"RunSnapshotKind":      {},
		"RuntimePolicyDigests": {},
	}
	forbiddenCalls := map[string]struct{}{
		"ReferenceDigest": {},
		"Seal":            {},
		"Validate":        {},
		"ValidateFor":     {},
	}
	expectedPinnedConstants := map[string]string{
		"taskRunReferenceSchemaVersionV1":  `"vane.run-snapshot-ref/v1"`,
		"taskRunReferenceKindV1":           `"scheduled"`,
		"taskRunReferenceModeV1":           `"compiled"`,
		"taskRunScheduledWorkflowPrefixV1": `"wf-"`,
		"maxTaskRunReferenceBytesV1":       `512`,
		"maxTaskRunReferenceTaskIDV1":      `255`,
	}
	seenPinnedConstants := make(map[string]bool, len(expectedPinnedConstants))
	var violations []string
	record := func(pos token.Pos, detail string) {
		position := fset.Position(pos)
		violations = append(violations, position.String()+": "+detail)
	}

	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction {
			if function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.Ident:
					if _, forbidden := forbiddenIdentifiers[n.Name]; forbidden {
						record(n.Pos(), "current identifier "+n.Name)
					}
				case *ast.CallExpr:
					selector, isSelector := n.Fun.(*ast.SelectorExpr)
					if isSelector {
						if _, forbidden := forbiddenCalls[selector.Sel.Name]; forbidden {
							record(selector.Pos(), "current helper/method "+selector.Sel.Name)
						}
					}
				case *ast.SelectorExpr:
					packageName, isPackage := n.X.(*ast.Ident)
					if isPackage && packageName.Name == "types" {
						if _, forbidden := forbiddenTypeSelectors[n.Sel.Name]; forbidden {
							record(n.Pos(), "current types constant "+n.Sel.Name)
						}
						if _, currentDTO := currentDTOSelectors[n.Sel.Name]; currentDTO &&
							function.Name.Name != "toCurrent" {
							record(n.Pos(), "current DTO conversion outside toCurrent: "+n.Sel.Name)
						}
					}
				case *ast.CompositeLit:
					selector, isSelector := n.Type.(*ast.SelectorExpr)
					if !isSelector {
						break
					}
					packageName, isPackage := selector.X.(*ast.Ident)
					if isPackage && packageName.Name == "types" &&
						selector.Sel.Name == "RunSnapshotRef" && len(n.Elts) != 0 &&
						function.Name.Name != "toCurrent" {
						record(n.Pos(), "current RunSnapshotRef conversion outside toCurrent")
					}
				}
				return true
			})
			continue
		}

		general, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral {
			continue
		}
		if general.Tok == token.CONST {
			for _, specification := range general.Specs {
				valueSpec, isValue := specification.(*ast.ValueSpec)
				if !isValue {
					continue
				}
				for i, name := range valueSpec.Names {
					want, pinned := expectedPinnedConstants[name.Name]
					if !pinned || i >= len(valueSpec.Values) {
						continue
					}
					var encoded bytes.Buffer
					if err := format.Node(&encoded, fset, valueSpec.Values[i]); err != nil {
						t.Fatalf("format pinned constant %s: %v", name.Name, err)
					}
					seenPinnedConstants[name.Name] = true
					if encoded.String() != want {
						record(name.Pos(), name.Name+" = "+encoded.String()+", want "+want)
					}
				}
			}
		}
		ast.Inspect(general, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				if _, forbidden := forbiddenIdentifiers[n.Name]; forbidden {
					record(n.Pos(), "current identifier "+n.Name)
				}
			case *ast.SelectorExpr:
				packageName, isPackage := n.X.(*ast.Ident)
				if isPackage && packageName.Name == "types" {
					if _, forbidden := forbiddenTypeSelectors[n.Sel.Name]; forbidden {
						record(n.Pos(), "current types constant "+n.Sel.Name)
					}
					if _, currentDTO := currentDTOSelectors[n.Sel.Name]; currentDTO {
						record(n.Pos(), "current DTO in frozen v1 declaration: "+n.Sel.Name)
					}
				}
			}
			return true
		})
		for _, specification := range general.Specs {
			typeSpec, isType := specification.(*ast.TypeSpec)
			if isType && strings.HasSuffix(typeSpec.Name.Name, "V1") && typeSpec.Assign.IsValid() {
				record(typeSpec.Pos(), "pinned v1 type must not be an alias")
			}
		}
	}
	for name := range expectedPinnedConstants {
		if !seenPinnedConstants[name] {
			violations = append(violations, "missing pinned v1 constant: "+name)
		}
	}

	if len(violations) != 0 {
		t.Fatalf("pinned v1 reference reader depends on current contract: %v", violations)
	}
}

func TestTaskRunSnapshotReferenceV1ExpectedBinding(t *testing.T) {
	row := goldenTaskRunSnapshotRowV1()
	ref, err := sealTaskRunSnapshotReferenceV1(&row, taskRunBudgetV1{})
	if err != nil {
		t.Fatal(err)
	}
	expected := types.RunIdentity{
		TemporalWorkflowID: "wf-task-golden",
		TemporalRunID:      "run-golden",
		RunKind:            types.RunSnapshotKind("scheduled"),
		TenantID:           7,
		UserID:             11,
		TaskID:             "task-golden",
	}
	pinned, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil {
		t.Fatalf("validate exact v1 reference: %v", err)
	}
	if pinned.ReferenceDigest != goldenTaskRunSnapshotReferenceDigestV1 {
		t.Fatalf("pinned digest = %q, want %q",
			pinned.ReferenceDigest, goldenTaskRunSnapshotReferenceDigestV1)
	}

	tests := []struct {
		name   string
		ref    types.RunSnapshotRef
		expect types.RunIdentity
	}{
		{name: "other run", ref: ref, expect: func() types.RunIdentity {
			mutated := expected
			mutated.TemporalRunID = "other-run"
			return mutated
		}()},
		{name: "wrong workflow convention", ref: ref, expect: func() types.RunIdentity {
			mutated := expected
			mutated.TemporalWorkflowID = "future-task-golden"
			return mutated
		}()},
		{name: "unsupported reference schema", ref: func() types.RunSnapshotRef {
			mutated := ref
			mutated.SchemaVersion = "vane.run-snapshot-ref/v2"
			return mutated
		}(), expect: expected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateTaskRunSnapshotReferenceForExpectedV1(
				tt.ref, tt.expect); err == nil {
				t.Fatal("v1 expected binding accepted a mismatch")
			}
		})
	}
}

func TestValidateStoredTaskRunSnapshotUsesOnlyPinnedReaders(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate reference test")
	}
	sourceFile := filepath.Join(filepath.Dir(thisFile), "task_run_snapshots.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse snapshot store: %v", err)
	}

	forbiddenIdentifiers := map[string]struct{}{
		"canonicalTaskRunBudget":     {},
		"canonicalizeTaskRunPayload": {},
		"digestTaskRunPolicies":      {},
		"validTaskRunTaskID":         {},
		"validTaskRunReference":      {},
		"validSHA256Digest":          {},
	}
	forbiddenTypeSelectors := map[string]struct{}{
		"ExecutionModeCompiled":    {},
		"RunSnapshotKindScheduled": {},
		"RunSnapshotSchemaVersion": {},
	}
	forbiddenMethods := map[string]struct{}{
		"Seal": {}, "Valid": {}, "Validate": {}, "ValidateFor": {}, "ReferenceDigest": {},
	}
	requiredCalls := map[string]bool{
		"safeRef": false, "readTaskRunSnapshotPayload": false, "readTaskRunBudgetV1": false,
	}
	var target *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction && function.Name.Name == "validateStoredTaskRunSnapshotV1" {
			target = function
			break
		}
	}
	if target == nil || target.Body == nil {
		t.Fatal("validateStoredTaskRunSnapshotV1 is missing")
	}
	var violations []string
	ast.Inspect(target.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Ident:
			if _, forbidden := forbiddenIdentifiers[n.Name]; forbidden {
				violations = append(violations, fset.Position(n.Pos()).String()+": "+n.Name)
			}
			if _, required := requiredCalls[n.Name]; required {
				requiredCalls[n.Name] = true
			}
		case *ast.SelectorExpr:
			if pkg, isPackage := n.X.(*ast.Ident); isPackage && pkg.Name == "types" {
				if _, forbidden := forbiddenTypeSelectors[n.Sel.Name]; forbidden {
					violations = append(violations,
						fset.Position(n.Pos()).String()+": types."+n.Sel.Name)
				}
			}
		case *ast.CallExpr:
			if selector, isSelector := n.Fun.(*ast.SelectorExpr); isSelector {
				if _, forbidden := forbiddenMethods[selector.Sel.Name]; forbidden {
					violations = append(violations,
						fset.Position(selector.Pos()).String()+": "+selector.Sel.Name)
				}
				if _, required := requiredCalls[selector.Sel.Name]; required {
					requiredCalls[selector.Sel.Name] = true
				}
			}
		}
		return true
	})
	for name, found := range requiredCalls {
		if !found {
			violations = append(violations, "missing pinned reader call: "+name)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("stored v1 validation bypasses pinned readers: %v", violations)
	}
}
