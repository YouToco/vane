package store

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var agentActionEntryPoints = map[string]bool{
	"ActivateAgentActionContinuation":         true,
	"RollbackAgentActionContinuation":         true,
	"ConfirmAgentActionContinuation":          true,
	"CancelAgentActionContinuation":           true,
	"AcquireAgentActionContinuation":          true,
	"ProjectAgentActionContinuation":          true,
	"ReleaseAgentActionContinuation":          true,
	"ListDueAgentActionContinuationTenantIDs": true,
	"ListDueAgentActionContinuations":         true,
}

var agentActionGenericExecution = map[string]bool{
	"Execute":               true,
	"GetActiveAgentSession": true,
	"EnableSource":          true,
}

func TestAgentActionContinuationRemainsDarkAndExact(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Agent action guard")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(testFile), ".."))
	storeRoot := filepath.Dir(testFile)
	files, err := parseAgentActionProductionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	for path, file := range files {
		violations = append(
			violations,
			agentActionForbiddenEntryReferences(path, storeRoot, file)...,
		)
	}
	violations = append(
		violations,
		agentActionReachableRecoveryViolations(files, storeRoot)...,
	)
	violations = append(
		violations,
		agentActionRoleEntryViolations(files, storeRoot)...,
	)
	violations = append(
		violations,
		agentActionRoleHelperReferenceViolations(files, storeRoot)...,
	)
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf(
			"dark exact Agent action boundary escaped:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

func TestAgentActionRecoveryGuardRejectsGenericExecutionMutations(
	t *testing.T,
) {
	mutations := map[string]string{
		"direct generic execute": `package store
type Store struct{}
func (s *Store) ProjectAgentActionContinuation(tool interface{ Execute() }) {
	tool.Execute()
}`,
		"method value alias": `package store
type Store struct{}
func (s *Store) ProjectAgentActionContinuation(tool interface{ Execute() }) {
	exec := tool.Execute
	exec()
}`,
		"method expression": `package store
type tool struct{}
func (tool) Execute() {}
type Store struct{}
func (s *Store) ProjectAgentActionContinuation() {
	exec := tool.Execute
	exec(tool{})
}`,
		"third helper": `package store
type Store struct{}
func (s *Store) ProjectAgentActionContinuation() { hidden(s) }
func hidden(s interface{ EnableSource() }) { effect := s.EnableSource; effect() }`,
		"current session lookup": `package store
type Store struct{}
func (s *Store) ProjectAgentActionContinuation(
	sessions interface{ GetActiveAgentSession() },
) {
	lookup := sessions.GetActiveAgentSession
	lookup()
}`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			file := parseAgentActionMutation(t, source)
			files := map[string]*ast.File{
				filepath.Clean("store/mutation.go"): file,
			}
			got := agentActionReachableRecoveryViolations(
				files, filepath.Clean("store"),
			)
			if len(got) == 0 {
				t.Fatal("generic execution mutation escaped guard")
			}
		})
	}
}

func TestAgentActionDarkGuardRejectsEntryPointAliases(t *testing.T) {
	mutations := map[string]string{
		"method value": `package escape
func escape(s interface{ ProjectAgentActionContinuation() }) {
	f := s.ProjectAgentActionContinuation
	f()
}`,
		"method expression": `package escape
type Store struct{}
func escape() { _ = (*Store).AcquireAgentActionContinuation }`,
		"interface": `package escape
type route interface { ConfirmAgentActionContinuation() }`,
		"package helper": `package escape
func ProjectAgentActionContinuation() {}`,
	}
	for name, source := range mutations {
		t.Run(name, func(t *testing.T) {
			file := parseAgentActionMutation(t, source)
			got := agentActionForbiddenEntryReferences(
				filepath.Clean("escape/mutation.go"),
				filepath.Clean("store"),
				file,
			)
			if len(got) == 0 {
				t.Fatal("entry-point mutation escaped dark guard")
			}
		})
	}
}

func TestAgentActionDarkGuardRejectsRoleHelperEscapes(t *testing.T) {
	for name, source := range map[string]string{
		"third file direct": `package store
func escaped() { setAgentActionContinuatorContext() }`,
		"third file alias": `package store
func escaped() {
	enter := setAgentActionOperatorContext
	enter()
}`,
	} {
		t.Run(name, func(t *testing.T) {
			file := parseAgentActionMutation(t, source)
			files := map[string]*ast.File{
				filepath.Clean("store/escaped.go"): file,
			}
			got := agentActionRoleHelperReferenceViolations(
				files, filepath.Clean("store"),
			)
			if len(got) == 0 {
				t.Fatal("role helper escape passed guard")
			}
		})
	}
}

func TestAgentActionFrozenEnableSourceMatchesRegisteredToolGolden(
	t *testing.T,
) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Agent action golden")
	}
	toolsPath := filepath.Clean(filepath.Join(
		filepath.Dir(testFile), "..", "agent", "tools.go",
	))
	raw, err := os.ReadFile(toolsPath)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := agentActionRegisteredEnableSourceContract(raw)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := freezeEnableSourceAction(42)
	if err != nil {
		t.Fatal(err)
	}
	wantSpec := `{"description":"重新启用一个因连续抓取失败被自动暂停的信源：置回正常、清零失败计数、立即恢复抓取。source_id 可先用 list_sources 查看状态。","name":"enable_source","parameters":{"properties":{"source_id":{"description":"要重新启用的信源 id（连续抓取失败被自动暂停的源，可先用 list_sources 查看状态）","type":"integer"}},"required":["source_id"],"type":"object"},"version":"vane.agent-tool-spec/v1"}`
	wantPolicy := `{"authorization":"owner","budget":"none","concurrency":"sequential","confirmation":"required","effects":["state_write"],"retry":"none","version":"vane.agent-tool-policy/v1"}`
	if string(frozen.toolSpec) != wantSpec ||
		string(frozen.toolPolicy) != wantPolicy ||
		agentActionAdapterVersion != "vane.enable-source/postgres/v1" {
		t.Fatalf(
			"frozen enable_source contract drifted: spec=%s policy=%s adapter=%s",
			frozen.toolSpec, frozen.toolPolicy, agentActionAdapterVersion,
		)
	}
	var frozenSpec struct {
		Name        string         `json:"name"`
		Description string         `json:"description"`
		Parameters  map[string]any `json:"parameters"`
	}
	if err := json.Unmarshal(frozen.toolSpec, &frozenSpec); err != nil {
		t.Fatal(err)
	}
	if frozenSpec.Name != registered.name ||
		frozenSpec.Description != registered.description ||
		!agentActionJSONEqual(frozenSpec.Parameters, registered.schema) ||
		!registered.ownerStateWriteConfirmation {
		t.Fatalf(
			"registered/frozen contract mismatch: registered=%+v frozen=%+v",
			registered, frozenSpec,
		)
	}

	for name, mutation := range map[string][]byte{
		"required": agentActionMutateEnableSourceSchema(
			t, raw, `"required": ["source_id"]`, `"required": []`,
		),
		"root type": agentActionMutateEnableSourceSchema(
			t, raw, `"type": "object"`, `"type": "array"`,
		),
	} {
		t.Run("reject schema mutation "+name, func(t *testing.T) {
			mutated, err := agentActionRegisteredEnableSourceContract(
				mutation,
			)
			if err == nil &&
				agentActionJSONEqual(
					frozenSpec.Parameters, mutated.schema,
				) {
				t.Fatal("schema mutation still matches frozen contract")
			}
		})
	}
}

func agentActionMutateEnableSourceSchema(
	t *testing.T,
	raw []byte,
	old, replacement string,
) []byte {
	t.Helper()
	source := string(raw)
	start := strings.Index(source, "const enableSourceSchema = `")
	if start < 0 {
		t.Fatal("enableSourceSchema const is absent")
	}
	bodyStart := start + len("const enableSourceSchema = `")
	bodyEnd := strings.Index(source[bodyStart:], "`")
	if bodyEnd < 0 {
		t.Fatal("enableSourceSchema const is unterminated")
	}
	bodyEnd += bodyStart
	body := source[bodyStart:bodyEnd]
	mutated := strings.Replace(body, old, replacement, 1)
	if mutated == body {
		t.Fatalf("enableSourceSchema mutation target is absent: %q", old)
	}
	return []byte(source[:bodyStart] + mutated + source[bodyEnd:])
}

type registeredEnableSourceContract struct {
	name                        string
	description                 string
	schema                      map[string]any
	ownerStateWriteConfirmation bool
}

func agentActionRegisteredEnableSourceContract(
	raw []byte,
) (registeredEnableSourceContract, error) {
	file, err := parser.ParseFile(
		token.NewFileSet(), "agent/tools.go", raw, 0,
	)
	if err != nil {
		return registeredEnableSourceContract{}, err
	}
	var contract registeredEnableSourceContract
	for _, declaration := range file.Decls {
		switch value := declaration.(type) {
		case *ast.GenDecl:
			for _, specification := range value.Specs {
				spec, ok := specification.(*ast.ValueSpec)
				if !ok || len(spec.Names) != 1 ||
					spec.Names[0].Name != "enableSourceSchema" ||
					len(spec.Values) != 1 {
					continue
				}
				literal, ok := spec.Values[0].(*ast.BasicLit)
				if !ok {
					continue
				}
				schema, err := strconv.Unquote(literal.Value)
				if err != nil {
					return contract, err
				}
				if err := json.Unmarshal(
					[]byte(schema), &contract.schema,
				); err != nil {
					return contract, err
				}
			}
		case *ast.FuncDecl:
			if value.Recv == nil || len(value.Recv.List) != 1 {
				continue
			}
			receiver, ok := value.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			receiverName, ok := receiver.X.(*ast.Ident)
			if !ok || receiverName.Name != "enableSourceTool" {
				continue
			}
			for _, statement := range value.Body.List {
				returned, ok := statement.(*ast.ReturnStmt)
				if !ok || len(returned.Results) != 1 {
					continue
				}
				literal, ok := returned.Results[0].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					return contract, err
				}
				switch value.Name.Name {
				case "Name":
					contract.name = text
				case "Description":
					contract.description = text
				}
			}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		identifier, ok := call.Fun.(*ast.Ident)
		if !ok || identifier.Name != "newToolSpec" ||
			len(call.Args) != 2 {
			return true
		}
		unary, ok := call.Args[0].(*ast.UnaryExpr)
		if !ok {
			return true
		}
		composite, ok := unary.X.(*ast.CompositeLit)
		if !ok {
			return true
		}
		toolType, ok := composite.Type.(*ast.Ident)
		if !ok || toolType.Name != "enableSourceTool" {
			return true
		}
		policy, ok := call.Args[1].(*ast.CallExpr)
		if !ok {
			return true
		}
		policyName, ok := policy.Fun.(*ast.Ident)
		if !ok || policyName.Name != "ownerPolicy" ||
			len(policy.Args) != 3 {
			return true
		}
		effects, ok := policy.Args[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		effectName, effectOK := effects.Fun.(*ast.Ident)
		confirmation, confirmationOK := policy.Args[1].(*ast.Ident)
		budget, budgetOK := policy.Args[2].(*ast.Ident)
		if effectOK && effectName.Name == "Effects" &&
			len(effects.Args) == 1 &&
			agentActionIdentName(effects.Args[0]) == "EffectStateWrite" &&
			confirmationOK &&
			confirmation.Name == "ConfirmationRequired" &&
			budgetOK && budget.Name == "BudgetNone" {
			contract.ownerStateWriteConfirmation = true
		}
		return true
	})
	if contract.name == "" || contract.description == "" ||
		contract.schema == nil {
		return contract, fmt.Errorf("incomplete registered enable_source contract")
	}
	return contract, nil
}

func agentActionIdentName(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func agentActionJSONEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil &&
		string(leftJSON) == string(rightJSON)
}

func parseAgentActionMutation(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(
		token.NewFileSet(), "mutation.go", source, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func parseAgentActionProductionFiles(
	root string,
) (map[string]*ast.File, error) {
	files := make(map[string]*ast.File)
	err := filepath.WalkDir(root, func(
		path string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "third_party", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(
			token.NewFileSet(), path, raw, 0,
		)
		if err != nil {
			return err
		}
		files[filepath.Clean(path)] = file
		return nil
	})
	return files, err
}

func agentActionForbiddenEntryReferences(
	path, storeRoot string,
	file *ast.File,
) []string {
	var violations []string
	allowedDefinitions := make(map[*ast.Ident]bool)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !agentActionEntryPoints[function.Name.Name] {
			continue
		}
		if filepath.Dir(path) == filepath.Clean(storeRoot) &&
			(filepath.Base(path) == "agent_action_continuation.go" ||
				filepath.Base(path) == "agent_action_projection.go") &&
			function.Recv != nil {
			allowedDefinitions[function.Name] = true
			continue
		}
		violations = append(
			violations,
			fmt.Sprintf("%s: forbidden declaration %s", path,
				function.Name.Name),
		)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok || !agentActionEntryPoints[identifier.Name] ||
			allowedDefinitions[identifier] {
			return true
		}
		violations = append(
			violations,
			fmt.Sprintf("%s: forbidden reference %s", path,
				identifier.Name),
		)
		return true
	})
	return violations
}

func agentActionReachableRecoveryViolations(
	files map[string]*ast.File,
	storeRoot string,
) []string {
	type functionRef struct {
		path string
		decl *ast.FuncDecl
	}
	functions := make(map[string][]functionRef)
	var queue []functionRef
	for path, file := range files {
		if filepath.Dir(path) != filepath.Clean(storeRoot) {
			continue
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ref := functionRef{path: path, decl: function}
			functions[function.Name.Name] = append(
				functions[function.Name.Name], ref,
			)
			if agentActionEntryPoints[function.Name.Name] {
				queue = append(queue, ref)
			}
		}
	}
	seen := make(map[*ast.FuncDecl]bool)
	var violations []string
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]
		if seen[ref.decl] {
			continue
		}
		seen[ref.decl] = true
		ast.Inspect(ref.decl.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Ident:
				if agentActionGenericExecution[value.Name] {
					violations = append(
						violations,
						fmt.Sprintf("%s:%s reaches %s",
							ref.path, ref.decl.Name.Name, value.Name),
					)
				}
				for _, next := range functions[value.Name] {
					if !seen[next.decl] {
						queue = append(queue, next)
					}
				}
			case *ast.BasicLit:
				for name := range agentActionGenericExecution {
					if strings.Contains(value.Value, name) {
						violations = append(
							violations,
							fmt.Sprintf("%s:%s has dynamic %s reference",
								ref.path, ref.decl.Name.Name, name),
						)
					}
				}
			}
			return true
		})
	}
	return violations
}

func agentActionRoleEntryViolations(
	files map[string]*ast.File,
	storeRoot string,
) []string {
	var violations []string
	want := filepath.Join(
		filepath.Clean(storeRoot), "agent_action_continuation.go",
	)
	roleEntries := map[string]int{
		"SET LOCAL ROLE vane_agent_action_operator":    0,
		"SET LOCAL ROLE vane_agent_action_continuator": 0,
	}
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			for role := range roleEntries {
				if strings.Contains(literal.Value, role) {
					roleEntries[role]++
					if path != want {
						violations = append(
							violations,
							fmt.Sprintf("%s: %s escaped controller",
								path, role),
						)
					}
				}
			}
			return true
		})
	}
	for role, count := range roleEntries {
		if count != 1 {
			violations = append(
				violations,
				fmt.Sprintf("%s count=%d want=1", role, count),
			)
		}
	}
	return violations
}

func agentActionRoleHelperReferenceViolations(
	files map[string]*ast.File,
	storeRoot string,
) []string {
	wantDefinitions := filepath.Join(
		filepath.Clean(storeRoot), "agent_action_continuation.go",
	)
	allowedCallers := map[string]map[string]bool{
		"setAgentActionOperatorContext": {
			"ActivateAgentActionContinuation": true,
			"RollbackAgentActionContinuation": true,
		},
		"setAgentActionContinuatorContext": {
			"decideAgentActionContinuation":  true,
			"AcquireAgentActionContinuation": true,
			"ProjectAgentActionContinuation": true,
			"ReleaseAgentActionContinuation": true,
		},
	}
	allowedFiles := map[string]map[string]bool{
		"setAgentActionOperatorContext": {
			wantDefinitions: true,
		},
		"setAgentActionContinuatorContext": {
			wantDefinitions: true,
			filepath.Join(
				filepath.Clean(storeRoot),
				"agent_action_projection.go",
			): true,
		},
	}
	var violations []string
	for path, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			for helper, callers := range allowedCallers {
				if function.Name.Name == helper {
					if path != wantDefinitions {
						violations = append(
							violations,
							fmt.Sprintf("%s: role helper %s redeclared",
								path, helper),
						)
					}
					continue
				}
				var references, directCalls int
				ast.Inspect(function.Body, func(node ast.Node) bool {
					switch value := node.(type) {
					case *ast.Ident:
						if value.Name == helper {
							references++
						}
					case *ast.CallExpr:
						identifier, ok := value.Fun.(*ast.Ident)
						if ok && identifier.Name == helper {
							directCalls++
						}
					}
					return true
				})
				if references == 0 {
					continue
				}
				if !allowedFiles[helper][path] ||
					!callers[function.Name.Name] ||
					references != directCalls {
					violations = append(
						violations,
						fmt.Sprintf(
							"%s:%s has role helper %s refs/direct=%d/%d",
							path, function.Name.Name, helper,
							references, directCalls,
						),
					)
				}
			}
		}
	}
	return violations
}
