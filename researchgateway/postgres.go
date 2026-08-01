package researchgateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/store"
)

const gatewayDBRoleV1 = "vane_research_llm_gateway"

type PostgresRepositoryV1 struct{ pool *pgxpool.Pool }

func NewPostgresRepositoryV1(ctx context.Context, databaseURL string) (*PostgresRepositoryV1, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("research gateway database URL: %w", err)
	}
	config.MaxConns, config.MinConns = 8, 1
	config.AfterConnect = store.ValidateResearchGatewayConnectionV1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresRepositoryV1{pool: pool}, nil
}

func (r *PostgresRepositoryV1) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

func (r *PostgresRepositoryV1) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+gatewayDBRoleV1); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return tx, nil
}

func (r *PostgresRepositoryV1) Claim(ctx context.Context, binding ExecuteRequestV1) (ClaimV1, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return ClaimV1{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var claim ClaimV1
	err = tx.QueryRow(ctx, `SELECT out_first_writer,out_settled,out_tenant_id,
		out_user_id,out_task_id,out_run_snapshot_id,out_trace_id,out_stage,
		out_system_prompt,out_user_prompt,out_provider,out_model,
		out_endpoint_id,out_endpoint_generation,out_credential_id,out_credential_generation,
		out_temperature,
		out_max_tokens,out_disable_thinking
		FROM claim_research_llm_gateway_request_v2($1,$2,$3)`, binding.ReservationID,
		binding.RequestDigest, binding.RunCapability).Scan(&claim.FirstWriter,
		&claim.Settled, &claim.Request.TenantID, &claim.Request.UserID,
		&claim.Request.TaskID, &claim.Request.RunSnapshotID, &claim.Request.TraceID,
		&claim.Request.Stage, &claim.Request.SystemPrompt, &claim.Request.UserPrompt,
		&claim.Request.Provider, &claim.Request.Model, &claim.Request.Endpoint.ID,
		&claim.Request.Endpoint.Generation, &claim.Request.CredentialRef.ID,
		&claim.Request.CredentialRef.Generation, &claim.Request.Temperature,
		&claim.Request.MaxTokens, &claim.Request.DisableThinking)
	if err != nil {
		return ClaimV1{}, err
	}
	claim.Request.ReservationID = binding.ReservationID
	claim.Request.RequestDigest = binding.RequestDigest
	if err := tx.Commit(ctx); err != nil {
		return ClaimV1{}, err
	}
	return claim, nil
}

func (r *PostgresRepositoryV1) Recover(ctx context.Context, binding ExecuteRequestV1) (bool, error) {
	tx, err := r.begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT recover_research_llm_gateway_request_v2($1,$2,$3)`,
		binding.ReservationID, binding.RequestDigest, binding.RunCapability); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55000" {
			return false, nil
		}
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *PostgresRepositoryV1) Settle(ctx context.Context, binding ExecuteRequestV1,
	frozen FrozenRequestV1, settlement SettlementV1) error {
	tx, err := r.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	call := settlement.Measured.Call
	if _, err := tx.Exec(ctx, `SELECT * FROM settle_research_llm_gateway_request_v2(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		binding.ReservationID, binding.RequestDigest, binding.RunCapability,
		call.Completion, call.Model, call.PromptTokens, call.CompletionTokens,
		call.PromptCacheHitTokens, call.PromptCacheMissTokens, call.ReasoningTokens,
		call.LatencyMs, call.PrefixCacheHit, call.Error, settlement.Measured.Attempted,
		settlement.Measured.UsageKnown, settlement.Measured.DefinitelyZeroUsage,
		settlement.Outcome, settlement.ErrorCode); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
