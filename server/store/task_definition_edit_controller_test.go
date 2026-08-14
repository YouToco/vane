package store

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestLoadTaskDefinitionEditProposalBasis(t *testing.T) {
	f := newTaskDefinitionEditEntrypointFixture(t, true)

	tenantID, status, version, digest, definition, creation, err :=
		f.store.LoadTaskDefinitionEditProposalBasis(
			t.Context(), f.base.UserID, f.base.TaskID,
		)
	if err != nil {
		t.Fatalf("LoadTaskDefinitionEditProposalBasis() error = %v", err)
	}
	if tenantID != f.base.TenantID ||
		status != types.ScheduleStatusActive ||
		version != f.baseRecord.Version ||
		digest != f.baseRecord.Digest ||
		definition.TenantID != f.base.TenantID ||
		definition.UserID != f.base.UserID ||
		definition.TaskID != f.base.TaskID ||
		len(creation) == 0 {
		t.Fatalf("proposal basis scope drifted: tenant=%d status=%s version=%d digest=%s definition=%+v creation=%+v",
			tenantID, status, version, digest, definition, creation)
	}
	payload, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, f.baseRecord.Payload) {
		t.Fatalf("loaded approved definition bytes differ:\ngot  %s\nwant %s",
			payload, f.baseRecord.Payload)
	}

	if _, _, _, _, _, _, err := f.store.LoadTaskDefinitionEditProposalBasis(
		t.Context(), f.base.UserID+999, f.base.TaskID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user basis error = %v, want NotFound", err)
	}
}

func TestLoadTaskDefinitionEditOperationByActor(t *testing.T) {
	f := newTaskDefinitionEditOperationFixture(t)
	loaded, err := f.state.store.LoadTaskDefinitionEditOperationByActor(
		t.Context(), f.op.ID, f.state.userID,
	)
	if err != nil {
		t.Fatalf("LoadTaskDefinitionEditOperationByActor() error = %v", err)
	}
	if loaded.Scope() != f.op.Scope() {
		t.Fatalf("loaded scope = %+v, want %+v", loaded.Scope(), f.op.Scope())
	}
	if _, err := f.state.store.LoadTaskDefinitionEditOperationByActor(
		t.Context(), f.op.ID, f.state.userID+999,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-user operation error = %v, want NotFound", err)
	}

	if _, err := f.state.store.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		f.op.TenantID, f.op.UserID,
	); err != nil {
		t.Fatalf("remove operation actor membership: %v", err)
	}
	if _, err := f.state.store.LoadTaskDefinitionEditOperationByActor(
		t.Context(), f.op.ID, f.op.UserID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("revoked membership operation error = %v, want NotFound", err)
	}

	if _, err := f.state.store.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		f.op.TenantID, f.op.UserID,
	); err != nil {
		t.Fatalf("restore operation actor membership: %v", err)
	}
	if _, err := f.state.store.pool.Exec(t.Context(),
		`UPDATE tenants SET status='suspended' WHERE id=$1`,
		f.op.TenantID,
	); err != nil {
		t.Fatalf("suspend operation tenant: %v", err)
	}
	t.Cleanup(func() {
		if _, err := f.state.store.pool.Exec(context.Background(),
			`UPDATE tenants SET status='active' WHERE id=$1`,
			f.op.TenantID,
		); err != nil {
			t.Errorf("restore operation tenant: %v", err)
		}
	})
	if _, err := f.state.store.LoadTaskDefinitionEditOperationByActor(
		t.Context(), f.op.ID, f.op.UserID,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("inactive tenant operation error = %v, want NotFound", err)
	}
}
