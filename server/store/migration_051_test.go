package store

import (
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/pusheffect"
)

func TestMigration051RestrictedPrimitiveAndUnusedDown(t *testing.T) {
	f := pushBatchAuthorityFixture(t)
	ctx := t.Context()
	if _, err := f.provider.UpTo(ctx, 51); err != nil {
		t.Fatalf("migrate to 051: %v", err)
	}
	var (
		coordinator, public, securityDefiner, fixedPath bool
	)
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  has_function_privilege(
		    'vane_push_effect_coordinator',
		    'block_expired_unclaimed_push_effect_v1(' ||
		    'text,bigint,bigint,bigint,text,bigint)','EXECUTE'),
		  has_function_privilege(
		    'public',
		    'block_expired_unclaimed_push_effect_v1(' ||
		    'text,bigint,bigint,bigint,text,bigint)','EXECUTE'),
		  p.prosecdef,
		  p.proconfig @> ARRAY['search_path=pg_catalog, public']::text[]
		  FROM pg_proc p
		  JOIN pg_namespace n ON n.oid=p.pronamespace
		 WHERE n.nspname='public'
		   AND p.proname='block_expired_unclaimed_push_effect_v1'`,
	).Scan(&coordinator, &public, &securityDefiner, &fixedPath); err != nil {
		t.Fatal(err)
	}
	if !coordinator || public || !securityDefiner || !fixedPath {
		t.Fatalf("051 primitive ACL=%v/%v secdef=%v path=%v",
			coordinator, public, securityDefiner, fixedPath)
	}
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("unused 051 down: %v", err)
	}
	var version int
	var exists bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COALESCE(max(version_id),0)
		     FROM goose_db_version WHERE is_applied),
		  to_regprocedure(
		    'block_expired_unclaimed_push_effect_v1(' ||
		    'text,bigint,bigint,bigint,text,bigint)') IS NOT NULL`,
	).Scan(&version, &exists); err != nil {
		t.Fatal(err)
	}
	if version != 50 || exists {
		t.Fatalf("051 Down version=%d function exists=%v", version, exists)
	}
}

func TestMigration051DownRefusesDurableTerminalState(t *testing.T) {
	f, effect := shortWindowPreparedPushEffect(t)
	blocked, err := f.store.BlockExpiredUnclaimedPushEffect(
		t.Context(),
		pusheffect.ExpiryResolution{
			Scope: effect.Scope(), ExpectedFence: effect.Fence,
			ExpectedTaskID: effect.TaskID, RequiredWindow: time.Minute,
		},
	)
	if err != nil || !blocked {
		t.Fatalf("seed durable terminal=%v/%v", blocked, err)
	}
	_, err = f.provider.Down(t.Context())
	if err == nil ||
		!strings.Contains(err.Error(),
			"refusing downgrade while expired push effect terminal state exists") {
		t.Fatalf("051 durable Down error=%v", err)
	}
}
