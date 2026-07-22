package store

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestResolveTaskCreationSources(t *testing.T) {
	st := tenantTestStore(t)
	ctx := t.Context()

	newScopedUser := func(name string) (int64, int64) {
		t.Helper()
		user, err := st.UpsertUserByOpenID(
			ctx, "ou_task_source_ref_"+name+"_"+uuid.NewString(), name,
		)
		if err != nil {
			t.Fatalf("create %s user: %v", name, err)
		}
		var tenantID int64
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants (status, plan) VALUES ($1, 'free') RETURNING id`,
			types.TenantStatusActive,
		).Scan(&tenantID); err != nil {
			t.Fatalf("create %s tenant: %v", name, err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
			tenantID, user.ID,
		); err != nil {
			t.Fatalf("create %s membership: %v", name, err)
		}
		return tenantID, user.ID
	}
	ownerTenantID, ownerUserID := newScopedUser("owner")
	foreignTenantID, foreignUserID := newScopedUser("foreign")
	otherUser, err := st.UpsertUserByOpenID(
		ctx, "ou_task_source_ref_member_"+uuid.NewString(), "same tenant member",
	)
	if err != nil {
		t.Fatalf("create same-tenant member: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'member')`,
		ownerTenantID, otherUser.ID,
	); err != nil {
		t.Fatalf("create same-tenant membership: %v", err)
	}

	allSourceIDs := make([]int64, 0, 7)
	newSource := func(label string) int64 {
		t.Helper()
		id, created, err := st.GetOrCreateSource(ctx, &types.Source{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
			Title:  "source " + label,
			URL:    "vane://web/search?q=task-source-ref-" + label + "-" + uuid.NewString(),
			Config: json.RawMessage(`{"query":"` + label + `"}`),
		})
		if err != nil || !created {
			t.Fatalf("create source %s: id=%d created=%v err=%v", label, id, created, err)
		}
		allSourceIDs = append(allSourceIDs, id)
		return id
	}
	firstID := newSource("first")
	secondID := newSource("second")
	foreignID := newSource("foreign")
	otherUserID := newSource("other-user")
	inactiveSubscriptionID := newSource("inactive-subscription")
	disabledSourceID := newSource("disabled")
	unsubscribedID := newSource("unsubscribed")

	for _, sourceID := range []int64{firstID, secondID, inactiveSubscriptionID, disabledSourceID} {
		if err := st.AddSubscription(ctx, ownerUserID, sourceID); err != nil {
			t.Fatalf("subscribe owner to %d: %v", sourceID, err)
		}
	}
	if err := st.AddSubscription(ctx, foreignUserID, foreignID); err != nil {
		t.Fatalf("subscribe foreign user: %v", err)
	}
	if err := st.AddSubscription(ctx, otherUser.ID, otherUserID); err != nil {
		t.Fatalf("subscribe same-tenant other user: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE subscriptions SET status = 'inactive'
		  WHERE tenant_id = $1 AND user_id = $2 AND source_id = $3`,
		ownerTenantID, ownerUserID, inactiveSubscriptionID,
	); err != nil {
		t.Fatalf("make subscription inactive: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE sources SET status = $2 WHERE id = $1`,
		disabledSourceID, types.SourceStatusDisabled,
	); err != nil {
		t.Fatalf("disable source: %v", err)
	}
	// Same user, second tenant, and a subscription that exists only there. This
	// makes sub.tenant_id part of the tested security property: removing that
	// predicate from the resolver would incorrectly authorize foreignID while
	// resolving under ownerTenantID.
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'member')`,
		foreignTenantID, ownerUserID,
	); err != nil {
		t.Fatalf("create same-user foreign membership: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO subscriptions (tenant_id, user_id, source_id)
		 VALUES ($1, $2, $3)`,
		foreignTenantID, ownerUserID, foreignID,
	); err != nil {
		t.Fatalf("create same-user foreign subscription: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM subscriptions WHERE tenant_id = ANY($1)`,
			[]int64{ownerTenantID, foreignTenantID})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE tenant_id = ANY($1)`,
			[]int64{ownerTenantID, foreignTenantID})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM sources WHERE id = ANY($1)`, allSourceIDs)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM users WHERE id = ANY($1)`,
			[]int64{ownerUserID, foreignUserID, otherUser.ID})
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM tenants WHERE id = ANY($1)`,
			[]int64{ownerTenantID, foreignTenantID})
	})

	t.Run("preserves requested order and values", func(t *testing.T) {
		resolved, err := st.ResolveTaskCreationSources(
			t.Context(), ownerTenantID, ownerUserID, []int64{secondID, firstID},
		)
		if err != nil {
			t.Fatalf("ResolveTaskCreationSources: %v", err)
		}
		var firstConfig struct {
			Query string `json:"query"`
		}
		if len(resolved) == 2 {
			if err := json.Unmarshal(resolved[1].Config, &firstConfig); err != nil {
				t.Fatalf("decode resolved config: %v", err)
			}
		}
		if len(resolved) != 2 || resolved[0].ID != secondID || resolved[1].ID != firstID ||
			resolved[0].Title != "source second" ||
			firstConfig.Query != "first" {
			t.Fatalf("resolved order/values=%+v", resolved)
		}
	})

	denials := []struct {
		name     string
		tenantID int64
		userID   int64
		sourceID int64
	}{
		{name: "missing", tenantID: ownerTenantID, userID: ownerUserID, sourceID: 1<<62 - 1},
		{name: "same user cross tenant subscription", tenantID: ownerTenantID, userID: ownerUserID, sourceID: foreignID},
		{name: "other user same tenant", tenantID: ownerTenantID, userID: ownerUserID, sourceID: otherUserID},
		{name: "inactive subscription", tenantID: ownerTenantID, userID: ownerUserID, sourceID: inactiveSubscriptionID},
		{name: "disabled source", tenantID: ownerTenantID, userID: ownerUserID, sourceID: disabledSourceID},
		{name: "unsubscribed source", tenantID: ownerTenantID, userID: ownerUserID, sourceID: unsubscribedID},
		{name: "tenant user mismatch", tenantID: foreignTenantID, userID: ownerUserID, sourceID: firstID},
	}
	var genericError string
	for _, tc := range denials {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.ResolveTaskCreationSources(
				t.Context(), tc.tenantID, tc.userID, []int64{tc.sourceID},
			)
			if !errors.Is(err, types.ErrNotFound) || types.CodeOf(err) != types.CodeNotFound {
				t.Fatalf("denial must be generic not-found: %v", err)
			}
			if genericError == "" {
				genericError = err.Error()
			} else if err.Error() != genericError {
				t.Fatalf("denial oracle leaked case: got=%q want=%q", err.Error(), genericError)
			}
		})
	}

	for _, tenantState := range []struct {
		name    string
		apply   string
		restore string
	}{
		{
			name:    "suspended tenant",
			apply:   `UPDATE tenants SET status = 'suspended' WHERE id = $1`,
			restore: `UPDATE tenants SET status = 'active' WHERE id = $1`,
		},
		{
			name:    "deleted tenant",
			apply:   `UPDATE tenants SET deleted_at = now() WHERE id = $1`,
			restore: `UPDATE tenants SET deleted_at = NULL WHERE id = $1`,
		},
	} {
		t.Run(tenantState.name, func(t *testing.T) {
			if _, err := st.pool.Exec(t.Context(), tenantState.apply, ownerTenantID); err != nil {
				t.Fatalf("apply tenant state: %v", err)
			}
			defer func() {
				restoreCtx, cancel := cleanupContext()
				defer cancel()
				if _, err := st.pool.Exec(restoreCtx, tenantState.restore, ownerTenantID); err != nil {
					t.Errorf("restore tenant state: %v", err)
				}
			}()
			_, err := st.ResolveTaskCreationSources(
				t.Context(), ownerTenantID, ownerUserID, []int64{firstID},
			)
			if !errors.Is(err, types.ErrNotFound) || err.Error() != genericError {
				t.Fatalf("tenant-state denial leaked: err=%v generic=%q", err, genericError)
			}
		})
	}

	t.Run("invalid ids are rejected before query", func(t *testing.T) {
		tooMany := make([]int64, maxTaskCreationSourceReferences+1)
		for i := range tooMany {
			tooMany[i] = int64(i + 1)
		}
		for _, sourceIDs := range [][]int64{nil, {0}, {firstID, firstID}, tooMany} {
			if _, err := st.ResolveTaskCreationSources(
				t.Context(), ownerTenantID, ownerUserID, sourceIDs,
			); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("ids=%v err=%v", sourceIDs, err)
			}
		}
	})
}
