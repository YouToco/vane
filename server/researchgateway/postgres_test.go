package researchgateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryV1ClaimScansFrozenRoutePostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	admin, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	databaseName := "vane_gateway_claim_" + hex.EncodeToString(suffix[:])
	if _, err := admin.Exec(t.Context(), `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_research_llm_gateway') THEN
			CREATE ROLE vane_research_llm_gateway NOLOGIN NOINHERIT;
		END IF;
	END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), `CREATE DATABASE "`+databaseName+`"`); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + databaseName
	scratchURL := parsed.String()
	var scratch *pgxpool.Pool
	t.Cleanup(func() {
		if scratch != nil {
			scratch.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname=$1 AND pid<>pg_backend_pid()`, databaseName)
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+databaseName+`"`)
	})
	scratch, err = pgxpool.New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	functionSQL := fmt.Sprintf(`CREATE FUNCTION claim_research_llm_gateway_request_v2(
		BIGINT,TEXT,TEXT) RETURNS TABLE(
		 out_first_writer BOOLEAN,out_settled BOOLEAN,
		 out_tenant_id BIGINT,out_user_id BIGINT,out_task_id TEXT,
		 out_run_snapshot_id BIGINT,out_trace_id TEXT,out_stage TEXT,
		 out_system_prompt TEXT,out_user_prompt TEXT,out_provider TEXT,out_model TEXT,
		 out_endpoint_id TEXT,out_endpoint_generation BIGINT,
		 out_credential_id TEXT,out_credential_generation BIGINT,
		 out_temperature REAL,out_max_tokens INTEGER,out_disable_thinking BOOLEAN)
		LANGUAGE SQL SECURITY DEFINER SET search_path=pg_catalog,public AS $body$
		 SELECT true,false,11::bigint,12::bigint,'task',13::bigint,'trace','planner',
		 'system','user','deepseek','deepseek-chat','deepseek-compatible-primary',
		 7::bigint,'llm-primary',9::bigint,0.2::real,128,false $body$;
		REVOKE ALL ON FUNCTION claim_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
		 FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION claim_research_llm_gateway_request_v2(BIGINT,TEXT,TEXT)
		 TO vane_research_llm_gateway;`)
	if _, err := scratch.Exec(t.Context(), functionSQL); err != nil {
		t.Fatal(err)
	}
	repository := &PostgresRepositoryV1{pool: scratch}
	claim, err := repository.Claim(t.Context(), ExecuteRequestV1{
		ReservationID: 13, RequestDigest: testDigestV1, RunCapability: testCapabilityV1})
	if err != nil {
		t.Fatal(err)
	}
	if !claim.FirstWriter || claim.Settled || claim.Request.Endpoint.Generation != 7 ||
		claim.Request.CredentialRef.Generation != 9 ||
		string(claim.Request.Provider) != "deepseek" {
		t.Fatalf("claim route was not scanned exactly: %+v", claim)
	}
}
