package store

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
)

const (
	researchRunSpendReservationSchemaV3 = "vane.research-run-step-spend-reservation/v1"
	researchRunSpendSettlementSchemaV3  = "vane.research-run-step-spend-settlement/v1"
	researchRunQuotaUnitsV3             = 1.0
)

type ResearchRunStepSpendReservationV3 struct {
	ID                   int64
	StartedStepID        int64
	RunSnapshotID        int64
	PlanID               int64
	PlanDigest           string
	Ordinal              int
	InvocationID         string
	ToolName             string
	RequestDigest        string
	QuotaBucket          QuotaBucket
	ReservedQuotaUnits   float64
	ReservedCostMicroUSD int64
}

// ResearchProviderCallV3 is the exact provider accounting receipt kept inside
// an Activity. It is persisted with a terminal step and never crosses Temporal
// history. QuotaUnits counts provider invocations; UsageQuantity is the
// provider's pricing quantity and deliberately has a separate unit.
type ResearchProviderCallV3 struct {
	TraceID       string
	Provider      string
	UsageQuantity float64
	QuotaUnits    float64
	HTTPStatus    *int
	DurationMS    int
	Attempted     bool
	CostKnown     bool
	CostMicroUSD  int64
	PricingStatus string
	CostCurrency  string
}

func validateResearchRunSpendSettlementV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	runSnapshotID, planID int64, planDigest string,
	terminal ResearchRunStepReceiptV3,
	expectedProvider *ResearchProviderCallV3,
) error {
	var reservationID, settlementID int64
	var actualQuotaUnits float64
	var actualCostMicroUSD int64
	var usageQuantity *float64
	var providerCostMicroUSD *int64
	var pricingStatus, currency string
	var toolCallID *int64
	var traceID, provider *string
	var callPricingStatus, callCurrency *string
	var httpStatus *int
	var durationMS *int
	err := tx.QueryRow(ctx,
		`SELECT reservation.id,settlement.id,
		        settlement.actual_quota_units::double precision,
		        settlement.actual_cost_micro_usd,settlement.pricing_status,
		        settlement.cost_currency,settlement.tool_call_id,
		        call.trace_id,call.provider,call.usage_quantity::double precision,
		        call.http_status,call.duration_ms,
		        (call.cost_amount*1000000)::bigint,
		        call.pricing_status,call.cost_currency
		   FROM research_run_step_spend_reservations reservation
		   JOIN research_run_step_spend_settlements settlement
		     ON settlement.reservation_id=reservation.id
		   LEFT JOIN tool_calls call ON call.id=settlement.tool_call_id
		  WHERE reservation.tenant_id=$1 AND reservation.user_id=$2
		    AND reservation.task_id=$3 AND reservation.run_snapshot_id=$4
		    AND reservation.plan_id=$5 AND reservation.temporal_run_id=$6
		    AND reservation.plan_digest=$7 AND reservation.step_ordinal=$8
		    AND reservation.invocation_id=$9 AND reservation.tool_name=$10
		    AND reservation.request_digest=$11
		    AND settlement.terminal_step_id=$12 AND settlement.outcome=$13
		    AND settlement.actual_cost_micro_usd=$14`,
		identity.TenantID, identity.UserID, identity.TaskID, runSnapshotID,
		planID, identity.TemporalRunID, planDigest,
		terminal.Ordinal, terminal.InvocationID, terminal.ToolName,
		terminal.RequestDigest, terminal.StepID, string(terminal.Phase),
		terminal.CostMicroUSD,
	).Scan(&reservationID, &settlementID, &actualQuotaUnits,
		&actualCostMicroUSD, &pricingStatus, &currency, &toolCallID,
		&traceID, &provider, &usageQuantity, &httpStatus, &durationMS,
		&providerCostMicroUSD, &callPricingStatus, &callCurrency)
	if errors.Is(err, pgx.ErrNoRows) {
		return researchRunIntegrityError()
	}
	if err != nil {
		return researchRunDatabaseError("validate research spend settlement", err)
	}
	if reservationID <= 0 || settlementID <= 0 ||
		(actualQuotaUnits != 0 && actualQuotaUnits != researchRunQuotaUnitsV3) ||
		actualCostMicroUSD != terminal.CostMicroUSD ||
		currency != "USD" || pricingStatus == "" {
		return researchRunIntegrityError()
	}
	if terminal.Phase == ResearchRunStepCompletedV3 {
		if toolCallID == nil || traceID == nil || provider == nil ||
			httpStatus == nil || durationMS == nil || usageQuantity == nil ||
			providerCostMicroUSD == nil ||
			*providerCostMicroUSD != terminal.CostMicroUSD {
			return researchRunIntegrityError()
		}
	} else if terminal.Phase != ResearchRunStepFailedV3 &&
		terminal.Phase != ResearchRunStepIndeterminateV3 {
		return researchRunIntegrityError()
	}
	if (toolCallID == nil && actualQuotaUnits != 0) ||
		(toolCallID != nil && actualQuotaUnits != researchRunQuotaUnitsV3) {
		return researchRunIntegrityError()
	}
	if expectedProvider != nil {
		if !expectedProvider.Attempted {
			if toolCallID != nil || actualQuotaUnits != 0 {
				return researchRunConflictError()
			}
			return nil
		}
		if toolCallID == nil || traceID == nil || provider == nil ||
			durationMS == nil || usageQuantity == nil || callPricingStatus == nil ||
			expectedProvider.QuotaUnits != actualQuotaUnits ||
			*traceID != expectedProvider.TraceID ||
			*provider != expectedProvider.Provider ||
			*usageQuantity != expectedProvider.UsageQuantity ||
			!equalResearchOptionalIntV3(httpStatus, expectedProvider.HTTPStatus) ||
			*durationMS != expectedProvider.DurationMS ||
			currency != "USD" {
			return researchRunConflictError()
		}
		if expectedProvider.CostKnown {
			if providerCostMicroUSD == nil || callCurrency == nil ||
				*providerCostMicroUSD != expectedProvider.CostMicroUSD ||
				*callPricingStatus != expectedProvider.PricingStatus ||
				*callCurrency != expectedProvider.CostCurrency ||
				pricingStatus != expectedProvider.PricingStatus {
				return researchRunConflictError()
			}
		} else if providerCostMicroUSD != nil || callCurrency != nil ||
			*callPricingStatus != "unpriced" || pricingStatus != "estimated" {
			return researchRunConflictError()
		}
	}
	return nil
}

func equalResearchOptionalIntV3(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (c ResearchProviderCallV3) validateForCompleted(costMicroUSD int64) error {
	providerCostValid := (c.Provider == "exa" &&
		(c.PricingStatus == "provider_reported" || c.PricingStatus == "calculated")) ||
		(c.Provider == "kimi" && c.PricingStatus == "calculated" &&
			c.CostMicroUSD == 0 && costMicroUSD == 0)
	if c.TraceID == "" || len(c.TraceID) > 255 || !providerCostValid ||
		!finiteResearchSpendV3(c.UsageQuantity) ||
		c.QuotaUnits != researchRunQuotaUnitsV3 || !c.Attempted || !c.CostKnown ||
		c.CostMicroUSD != costMicroUSD || c.CostMicroUSD < 0 ||
		c.HTTPStatus == nil || *c.HTTPStatus < 200 || *c.HTTPStatus >= 300 ||
		c.DurationMS < 0 || c.DurationMS > 86_400_000 ||
		c.CostCurrency != "USD" {
		return researchRunValidationError("research provider accounting receipt is invalid")
	}
	return nil
}

func (c ResearchProviderCallV3) validateForTerminal(
	phase ResearchRunStepPhaseV3, costMicroUSD, reservedCostMicroUSD int64,
) error {
	if phase == ResearchRunStepCompletedV3 {
		return c.validateForCompleted(costMicroUSD)
	}
	if phase != ResearchRunStepFailedV3 && phase != ResearchRunStepIndeterminateV3 {
		return researchRunValidationError("research provider terminal receipt is invalid")
	}
	if !c.Attempted {
		if phase != ResearchRunStepFailedV3 || costMicroUSD != 0 ||
			c != (ResearchProviderCallV3{}) {
			return researchRunValidationError("unattempted provider receipt is invalid")
		}
		return nil
	}
	if c.TraceID == "" || len(c.TraceID) > 255 ||
		(c.Provider != "exa" && c.Provider != "kimi") ||
		c.DurationMS < 0 || c.DurationMS > 86_400_000 ||
		c.QuotaUnits != researchRunQuotaUnitsV3 ||
		c.UsageQuantity < 0 || math.IsNaN(c.UsageQuantity) ||
		math.IsInf(c.UsageQuantity, 0) ||
		(c.HTTPStatus != nil && (*c.HTTPStatus < 100 || *c.HTTPStatus > 599)) {
		return researchRunValidationError("attempted provider receipt is invalid")
	}
	if c.Provider == "kimi" {
		if !c.CostKnown || c.CostMicroUSD != 0 || costMicroUSD != 0 ||
			c.PricingStatus != "calculated" || c.CostCurrency != "USD" {
			return researchRunValidationError("official provider spend must be known zero cost")
		}
	} else if c.CostKnown {
		if c.CostMicroUSD < 0 || c.CostMicroUSD != costMicroUSD ||
			(c.PricingStatus != "provider_reported" &&
				c.PricingStatus != "calculated") || c.CostCurrency != "USD" {
			return researchRunValidationError("known provider spend is invalid")
		}
	} else if c.CostMicroUSD != 0 || costMicroUSD != reservedCostMicroUSD ||
		c.PricingStatus != "unpriced" || c.CostCurrency != "" {
		return researchRunValidationError("unknown provider spend must keep its reservation")
	}
	return nil
}

func loadResearchRunToolGrantV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, snapshotID int64,
	planRef types.ResearchRunPlanRefV3, toolName string,
) (runtimepolicy.ResearchToolDefinitionV3, types.PlannerBudget, error) {
	row, found, err := loadResearchRunSnapshotRowV3(ctx, tx,
		CreateOrGetTaskRunSnapshotParams{
			TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
			TemporalWorkflowID: identity.TemporalWorkflowID,
			TemporalRunID:      identity.TemporalRunID,
		})
	if err != nil {
		return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{}, err
	}
	if !found || row.ID != snapshotID {
		return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{}, researchRunIntegrityError()
	}
	storedRef, err := validateStoredResearchRunSnapshotV3(identity, row)
	if err != nil || storedRef.SnapshotID != snapshotID ||
		storedRef.DefinitionDigest != planRef.DefinitionDigest ||
		storedRef.CapabilityCatalogDigest != planRef.CapabilityCatalogDigest ||
		storedRef.ToolPolicyDigest != planRef.ToolPolicyDigest {
		return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{}, researchRunIntegrityError()
	}
	seal, err := runcontext.DecodeResearchSnapshotPayloadV3(row.Payload)
	if err != nil || seal.PayloadDigest != row.PayloadDigest ||
		seal.ResearchToolPolicyDigest != planRef.ToolPolicyDigest {
		return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{}, researchRunIntegrityError()
	}
	for _, grant := range seal.ResearchTools.AllowedTools {
		if grant.Name == toolName {
			validGrant := (grant.BudgetBucket == string(QuotaExaCalls) &&
				grant.Provider == "exa") ||
				(grant.BudgetBucket == string(QuotaOfficialCalls) &&
					grant.Provider == "kimi" && grant.MaxCostMicroUSD == 1)
			if !validGrant ||
				grant.MaxCostMicroUSD <= 0 || grant.MaxCostMicroUSD > 1_000_000 {
				return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{}, researchRunIntegrityError()
			}
			return grant, storedRef.PlannerBudget, nil
		}
	}
	return runtimepolicy.ResearchToolDefinitionV3{}, types.PlannerBudget{},
		researchRunValidationError("research step Tool grant is unavailable")
}

func lockResearchRunSpendBudgetV3(
	ctx context.Context, tx pgx.Tx, temporalRunID string,
) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
		"research-spend/v3:"+temporalRunID+":budget"); err != nil {
		return researchRunDatabaseError("lock research run spend budget", err)
	}
	return nil
}

func admitResearchRunSpendBudgetV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	runSnapshotID, maxRunCost, newReservation int64,
) error {
	if maxRunCost <= 0 || newReservation <= 0 || newReservation > maxRunCost {
		return types.NewAppError(types.CodeQuotaExceeded,
			"research run cost budget is exhausted", ErrQuotaExceeded)
	}
	var recorded int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(sum(CASE WHEN settlement.id IS NULL
		                         THEN reservation.reserved_cost_micro_usd
		                         ELSE settlement.actual_cost_micro_usd END),0)::bigint
		   FROM research_run_step_spend_reservations reservation
		   LEFT JOIN research_run_step_spend_settlements settlement
		     ON settlement.reservation_id=reservation.id
		  WHERE reservation.tenant_id=$1 AND reservation.user_id=$2
		    AND reservation.task_id=$3 AND reservation.run_snapshot_id=$4
		    AND reservation.temporal_run_id=$5`,
		identity.TenantID, identity.UserID, identity.TaskID, runSnapshotID,
		identity.TemporalRunID).Scan(&recorded); err != nil {
		return researchRunDatabaseError("calculate research run spend budget", err)
	}
	if recorded < 0 || recorded > maxRunCost || newReservation > maxRunCost-recorded {
		return types.NewAppError(types.CodeQuotaExceeded,
			"research run cost budget is exhausted", ErrQuotaExceeded)
	}
	return nil
}

func consumeResearchRunQuotaV3(
	ctx context.Context, tx pgx.Tx, tenantID int64, bucket QuotaBucket, units float64,
) error {
	if tenantID <= 0 ||
		(bucket != QuotaExaCalls && bucket != QuotaOfficialCalls) ||
		units != researchRunQuotaUnitsV3 {
		return researchRunValidationError("research run quota reservation is invalid")
	}
	var admitted bool
	err := tx.QueryRow(ctx,
		`SELECT reserve_research_run_quota_v3($1,$2,$3)`,
		tenantID, string(bucket), units).Scan(&admitted)
	if err != nil {
		return classifyQuotaErr(err,
			fmt.Sprintf("reserve research quota (tenant=%d bucket=%s)", tenantID, bucket))
	}
	if !admitted {
		return types.NewAppError(types.CodeQuotaExceeded,
			"research provider quota is exhausted", ErrQuotaExceeded)
	}
	return nil
}

func finiteResearchSpendV3(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func loadResearchRunSpendReservationV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity, planID int64,
	startedStepID int64, ordinal int, requestDigest string,
) (ResearchRunStepSpendReservationV3, error) {
	var row ResearchRunStepSpendReservationV3
	var bucket string
	err := tx.QueryRow(ctx,
		`SELECT id,started_step_id,run_snapshot_id,plan_id,plan_digest,step_ordinal,
		        invocation_id,tool_name,request_digest,quota_bucket,
		        reserved_quota_units,reserved_cost_micro_usd
		   FROM research_run_step_spend_reservations
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND plan_id=$4
		    AND started_step_id=$5 AND temporal_run_id=$6 AND step_ordinal=$7
		    AND request_digest=$8`,
		identity.TenantID, identity.UserID, identity.TaskID, planID,
		startedStepID, identity.TemporalRunID, ordinal, requestDigest,
	).Scan(&row.ID, &row.StartedStepID, &row.RunSnapshotID, &row.PlanID,
		&row.PlanDigest, &row.Ordinal, &row.InvocationID, &row.ToolName, &row.RequestDigest,
		&bucket, &row.ReservedQuotaUnits, &row.ReservedCostMicroUSD)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResearchRunStepSpendReservationV3{}, researchRunIntegrityError()
	}
	if err != nil {
		return ResearchRunStepSpendReservationV3{}, researchRunDatabaseError("load research spend reservation", err)
	}
	row.QuotaBucket = QuotaBucket(bucket)
	if row.ID <= 0 || row.ReservedQuotaUnits != researchRunQuotaUnitsV3 ||
		(row.QuotaBucket != QuotaExaCalls && row.QuotaBucket != QuotaOfficialCalls) ||
		row.ReservedCostMicroUSD <= 0 {
		return ResearchRunStepSpendReservationV3{}, researchRunIntegrityError()
	}
	return row, nil
}

func insertResearchProviderToolCallV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	reservation ResearchRunStepSpendReservationV3, traceID string,
	arguments []byte, result []byte, originalSize int,
	errorCode string, call ResearchProviderCallV3,
) (int64, error) {
	if !call.Attempted || traceID != call.TraceID ||
		reservation.ID <= 0 || reservation.RunSnapshotID <= 0 ||
		reservation.ToolName == "" || len(arguments) < 2 || originalSize < len(result) {
		return 0, researchRunValidationError("research provider Tool call is invalid")
	}
	previewRunes := []rune(string(result))
	if len(previewRunes) > 8192 {
		previewRunes = previewRunes[:8192]
	}
	endpointPath := ""
	persistedToolName := reservation.ToolName
	toolKind := string(types.ToolCallKindStatic)
	switch reservation.ToolName {
	case "web_search":
		endpointPath = "/search"
	case "web_contents":
		endpointPath = "/contents"
	case "web_product_status":
		persistedToolName = "kimi:goods_list"
		toolKind = string(types.ToolCallKindOfficialFetch)
		endpointPath = "/apiv2/kimi.gateway.order.v1.GoodsService/ListGoods"
	default:
		return 0, researchRunIntegrityError()
	}
	var costMicroUSD any
	var costCurrency any
	pricingStatus := "unpriced"
	if call.CostKnown {
		costMicroUSD = call.CostMicroUSD
		costCurrency = call.CostCurrency
		pricingStatus = call.PricingStatus
	}
	errorType := ""
	if errorCode != "" {
		errorType = "provider_error"
	}
	var toolCallID int64
	err := tx.QueryRow(ctx,
		`INSERT INTO tool_calls (
		     trace_id,user_id,session_id,tool_name,tool_kind,endpoint_path,
		     arguments,result_preview,result_size,http_status,error_type,error,
		     duration_ms,retrieval_query,candidate_tools,cost_usd,source_id,
		     tenant_id,provider,usage_quantity,pricing_rule_id,pricing_status,
		     cost_amount,cost_currency,run_snapshot_id,
		     research_run_step_spend_reservation_id
		 ) VALUES ($1,$2,NULL,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'','{}',
		           CASE WHEN $13::bigint IS NULL THEN NULL ELSE $13::numeric/1000000 END,
		           NULL,$14,$15,$16,NULL,$17,
		           CASE WHEN $13::bigint IS NULL THEN NULL ELSE $13::numeric/1000000 END,
		           $18,$19,$20)
		 RETURNING id`,
		traceID, identity.UserID, persistedToolName, toolKind, endpointPath,
		arguments, string(previewRunes), originalSize, call.HTTPStatus,
		errorType, errorCode, call.DurationMS, costMicroUSD, identity.TenantID,
		call.Provider, call.UsageQuantity, pricingStatus, costCurrency,
		reservation.RunSnapshotID, reservation.ID,
	).Scan(&toolCallID)
	if err != nil {
		return 0, researchRunDatabaseError("seal research provider Tool call", err)
	}
	return toolCallID, nil
}

func insertResearchRunSpendSettlementV3(
	ctx context.Context, tx pgx.Tx, identity types.RunIdentity,
	reservation ResearchRunStepSpendReservationV3, terminalStepID int64,
	phase ResearchRunStepPhaseV3, toolCallID *int64, actualCostMicroUSD int64,
	actualQuotaUnits float64, pricingStatus, currency string,
) error {
	if terminalStepID <= 0 || reservation.ID <= 0 || actualCostMicroUSD < 0 ||
		(actualQuotaUnits != 0 && actualQuotaUnits != researchRunQuotaUnitsV3) ||
		(phase != ResearchRunStepCompletedV3 && phase != ResearchRunStepFailedV3 &&
			phase != ResearchRunStepIndeterminateV3) ||
		(phase == ResearchRunStepCompletedV3 && toolCallID == nil) {
		return researchRunValidationError("research spend settlement is invalid")
	}
	if pricingStatus == "" {
		pricingStatus = "unpriced"
	}
	if currency == "" {
		currency = "USD"
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO research_run_step_spend_settlements (
		     tenant_id,user_id,task_id,run_snapshot_id,plan_id,reservation_id,
		     terminal_step_id,tool_call_id,temporal_run_id,plan_digest,
		     step_ordinal,invocation_id,tool_name,request_digest,outcome,
		     actual_quota_units,actual_cost_micro_usd,pricing_status,
		     cost_currency,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
		           $16,$17,$18,$19,$20)`,
		identity.TenantID, identity.UserID, identity.TaskID,
		reservation.RunSnapshotID, reservation.PlanID, reservation.ID,
		terminalStepID, toolCallID, identity.TemporalRunID,
		reservation.PlanDigest,
		reservation.Ordinal, reservation.InvocationID, reservation.ToolName,
		reservation.RequestDigest, string(phase), actualQuotaUnits,
		actualCostMicroUSD, pricingStatus, currency,
		researchRunSpendSettlementSchemaV3,
	)
	if err != nil {
		return researchRunDatabaseError("seal research spend settlement", err)
	}
	return nil
}
