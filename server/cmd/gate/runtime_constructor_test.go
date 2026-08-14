package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestGateUsesNonOwnerServerRuntimeStore(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var runtimeCalls, ownerEraCalls int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "store" {
			return true
		}
		switch selector.Sel.Name {
		case "NewServerRuntime":
			runtimeCalls++
		case "New":
			ownerEraCalls++
		}
		return true
	})
	if runtimeCalls != 1 || ownerEraCalls != 0 {
		t.Fatalf("gate Store constructors: NewServerRuntime=%d New=%d",
			runtimeCalls, ownerEraCalls)
	}
}

func TestExplicitPrincipalFromMembershipsFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		userID      int64
		memberships []types.Membership
		wantCode    types.ErrCode
		wantTenant  int64
	}{
		{name: "invalid user", userID: -1, wantCode: types.CodeValidation},
		{name: "no membership", userID: 7, wantCode: types.CodeNotFound},
		{name: "multiple memberships", userID: 7, memberships: []types.Membership{
			{TenantID: 2, UserID: 7}, {TenantID: 3, UserID: 7},
		}, wantCode: types.CodeConflict},
		{name: "exact membership", userID: 7, memberships: []types.Membership{
			{TenantID: 9, UserID: 7},
		}, wantTenant: 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := explicitPrincipalFromMemberships(tc.userID, tc.memberships)
			if tc.wantCode != "" {
				if types.CodeOf(err) != tc.wantCode {
					t.Fatalf("error code=%s, want %s (err=%v)", types.CodeOf(err), tc.wantCode, err)
				}
				return
			}
			if err != nil || int64(got.TenantID) != tc.wantTenant || got.UserID != tc.userID {
				t.Fatalf("principal=%+v err=%v", got, err)
			}
		})
	}
}
