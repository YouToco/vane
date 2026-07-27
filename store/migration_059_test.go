package store

import "testing"

func TestMigration059ProposerHasOnlyCreateCapabilities(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 59); err != nil {
		t.Fatalf("migrate to 059: %v", err)
	}
	var (
		canRootInsert, canContinuationInsert, canAuthorityInsert bool
		canRootUpdate, canContinuationUpdate, canAuthorityUpdate bool
		canRootDelete, canSourceRead, canSessionRead             bool
		canSequenceUse, canUnusedContinuationRead                bool
		canUnusedAuthorityRead                                   bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  has_column_privilege(
		    'vane_agent_action_proposer','pending_actions',
		    'execution_version','INSERT'),
		  has_column_privilege(
		    'vane_agent_action_proposer','agent_action_continuations',
		    'canonical_args','INSERT'),
		  has_column_privilege(
		    'vane_agent_action_proposer',
		    'agent_action_continuation_authority_events',
		    'generation','INSERT'),
		  has_any_column_privilege(
		    'vane_agent_action_proposer','pending_actions','UPDATE'),
		  has_any_column_privilege(
		    'vane_agent_action_proposer','agent_action_continuations',
		    'UPDATE'),
		  has_any_column_privilege(
		    'vane_agent_action_proposer',
		    'agent_action_continuation_authority_events',
		    'UPDATE'),
		  has_table_privilege(
		    'vane_agent_action_proposer','pending_actions','DELETE'),
		  has_table_privilege(
		    'vane_agent_action_proposer','sources','SELECT'),
		  has_table_privilege(
		    'vane_agent_action_proposer','agent_sessions','SELECT'),
		  has_sequence_privilege(
		    'vane_agent_action_proposer',
		    'agent_action_continuation_authority_events_id_seq',
		    'USAGE'),
		  has_column_privilege(
		    'vane_agent_action_proposer','agent_action_continuations',
		    'tool_name','SELECT'),
		  has_column_privilege(
		    'vane_agent_action_proposer',
		    'agent_action_continuation_authority_events',
		    'tenant_id','SELECT')`,
	).Scan(
		&canRootInsert, &canContinuationInsert, &canAuthorityInsert,
		&canRootUpdate, &canContinuationUpdate, &canAuthorityUpdate,
		&canRootDelete, &canSourceRead, &canSessionRead,
		&canSequenceUse, &canUnusedContinuationRead,
		&canUnusedAuthorityRead,
	); err != nil {
		t.Fatal(err)
	}
	if !canRootInsert || !canContinuationInsert ||
		!canAuthorityInsert || !canSequenceUse ||
		canRootUpdate || canContinuationUpdate || canAuthorityUpdate ||
		canRootDelete || canSourceRead || canSessionRead ||
		canUnusedContinuationRead || canUnusedAuthorityRead {
		t.Fatalf(
			"proposer capabilities root/continuation/authority/sequence="+
				"%t/%t/%t/%t update/delete/source/session/unused="+
				"%t/%t/%t/%t/%t/%t/%t/%t",
			canRootInsert, canContinuationInsert, canAuthorityInsert,
			canSequenceUse, canRootUpdate, canContinuationUpdate,
			canAuthorityUpdate, canRootDelete, canSourceRead,
			canSessionRead, canUnusedContinuationRead,
			canUnusedAuthorityRead,
		)
	}
	if _, err := provider.DownTo(ctx, 58); err != nil {
		t.Fatalf("059 Down: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
		  has_table_privilege(
		    'vane_agent_action_proposer','pending_actions','SELECT,INSERT'),
		  has_table_privilege(
		    'vane_agent_action_proposer',
		    'agent_action_continuations','SELECT,INSERT')`,
	).Scan(&canRootInsert, &canContinuationInsert); err != nil {
		t.Fatal(err)
	}
	if canRootInsert || canContinuationInsert {
		t.Fatal("059 Down retained proposer table capability")
	}
}
