package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const (
	PriceMeterLLMTokens = "llm_tokens"
	PriceMeterRequest   = "request"

	providerPricingLedgerLock = "vane-provider-pricing-v1"
)

// ProviderPriceRule is an immutable price version. Updating a price closes the
// current open interval and inserts a new row; call receipts keep their exact
// rule_id and calculated amount forever.
type ProviderPriceRule struct {
	ID                         int64      `json:"id"`
	Provider                   string     `json:"provider"`
	Resource                   string     `json:"resource"`
	Meter                      string     `json:"meter"`
	Currency                   string     `json:"currency"`
	InputCacheHitPerMillion    *float64   `json:"input_cache_hit_per_million,omitempty"`
	InputCacheMissPerMillion   *float64   `json:"input_cache_miss_per_million,omitempty"`
	OutputPerMillion           *float64   `json:"output_per_million,omitempty"`
	RequestUnitPrice           *float64   `json:"request_unit_price,omitempty"`
	RequestIncludedQuantity    *float64   `json:"request_included_quantity,omitempty"`
	RequestAdditionalUnitPrice *float64   `json:"request_additional_unit_price,omitempty"`
	EffectiveFrom              time.Time  `json:"effective_from"`
	EffectiveTo                *time.Time `json:"effective_to,omitempty"`
	SourceURL                  string     `json:"source_url"`
	Note                       string     `json:"note"`
	CreatedBy                  *int64     `json:"created_by,omitempty"`
	CreatedAt                  time.Time  `json:"created_at"`
}

type ReplaceProviderPriceRuleInput struct {
	Provider                   string
	Resource                   string
	Meter                      string
	Currency                   string
	InputCacheHitPerMillion    *float64
	InputCacheMissPerMillion   *float64
	OutputPerMillion           *float64
	RequestUnitPrice           *float64
	RequestIncludedQuantity    *float64
	RequestAdditionalUnitPrice *float64
	EffectiveFrom              time.Time
	SourceURL                  string
	Note                       string
	CreatedBy                  int64
	ChangeID                   string
}

func normalizePriceRuleInput(in ReplaceProviderPriceRuleInput) (ReplaceProviderPriceRuleInput, error) {
	in.Provider = strings.ToLower(strings.TrimSpace(in.Provider))
	in.Resource = strings.TrimSpace(in.Resource)
	in.Meter = strings.TrimSpace(in.Meter)
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	in.SourceURL = strings.TrimSpace(in.SourceURL)
	in.Note = strings.TrimSpace(in.Note)
	in.ChangeID = strings.TrimSpace(in.ChangeID)
	if in.Provider == "" || in.Resource == "" || len(in.Provider) > 64 || len(in.Resource) > 255 {
		return in, types.NewAppError(types.CodeValidation, "供应商和计价资源不能为空或过长", types.ErrValidation)
	}
	if in.Meter != PriceMeterLLMTokens && in.Meter != PriceMeterRequest {
		return in, types.NewAppError(types.CodeValidation, "不支持的计价方式", types.ErrValidation)
	}
	if in.Currency != "USD" && in.Currency != "CNY" {
		return in, types.NewAppError(types.CodeValidation, "货币仅支持 USD 或 CNY", types.ErrValidation)
	}
	if in.CreatedBy <= 0 {
		return in, types.NewAppError(types.CodeValidation, "价格更新必须记录管理员", types.ErrValidation)
	}
	if in.ChangeID == "" || len(in.ChangeID) > 128 {
		return in, types.NewAppError(types.CodeValidation, "价格更新缺少有效幂等键", types.ErrValidation)
	}
	u, err := url.Parse(in.SourceURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || len(in.SourceURL) > 2048 {
		return in, types.NewAppError(types.CodeValidation, "官方价格来源必须是有效的 HTTP(S) 地址", types.ErrValidation)
	}
	if len(in.Note) > 500 {
		return in, types.NewAppError(types.CodeValidation, "价格备注不能超过 500 字符", types.ErrValidation)
	}
	validPrice := func(v *float64) bool {
		return v != nil && *v >= 0 && *v <= 1_000_000
	}
	switch in.Meter {
	case PriceMeterLLMTokens:
		if !validPrice(in.InputCacheHitPerMillion) ||
			!validPrice(in.InputCacheMissPerMillion) ||
			!validPrice(in.OutputPerMillion) ||
			in.RequestUnitPrice != nil ||
			in.RequestIncludedQuantity != nil ||
			in.RequestAdditionalUnitPrice != nil {
			return in, types.NewAppError(types.CodeValidation,
				"Token 计价必须填写缓存命中、缓存未命中和输出单价", types.ErrValidation)
		}
	case PriceMeterRequest:
		if !validPrice(in.RequestUnitPrice) ||
			!validPrice(in.RequestIncludedQuantity) ||
			!validPrice(in.RequestAdditionalUnitPrice) ||
			in.InputCacheHitPerMillion != nil ||
			in.InputCacheMissPerMillion != nil ||
			in.OutputPerMillion != nil {
			return in, types.NewAppError(types.CodeValidation,
				"按次计价只能填写单次价格", types.ErrValidation)
		}
	}
	return in, nil
}

func providerPriceRequestHash(in ReplaceProviderPriceRuleInput) (string, error) {
	payload, err := json.Marshal(struct {
		Provider                   string     `json:"provider"`
		Resource                   string     `json:"resource"`
		Meter                      string     `json:"meter"`
		Currency                   string     `json:"currency"`
		InputCacheHitPerMillion    *float64   `json:"input_cache_hit_per_million,omitempty"`
		InputCacheMissPerMillion   *float64   `json:"input_cache_miss_per_million,omitempty"`
		OutputPerMillion           *float64   `json:"output_per_million,omitempty"`
		RequestUnitPrice           *float64   `json:"request_unit_price,omitempty"`
		RequestIncludedQuantity    *float64   `json:"request_included_quantity,omitempty"`
		RequestAdditionalUnitPrice *float64   `json:"request_additional_unit_price,omitempty"`
		EffectiveFrom              *time.Time `json:"effective_from,omitempty"`
		SourceURL                  string     `json:"source_url"`
		Note                       string     `json:"note"`
	}{
		Provider: in.Provider, Resource: in.Resource, Meter: in.Meter,
		Currency:                 in.Currency,
		InputCacheHitPerMillion:  in.InputCacheHitPerMillion,
		InputCacheMissPerMillion: in.InputCacheMissPerMillion,
		OutputPerMillion:         in.OutputPerMillion, RequestUnitPrice: in.RequestUnitPrice,
		RequestIncludedQuantity:    in.RequestIncludedQuantity,
		RequestAdditionalUnitPrice: in.RequestAdditionalUnitPrice,
		EffectiveFrom: func() *time.Time {
			if in.EffectiveFrom.IsZero() {
				return nil
			}
			t := in.EffectiveFrom.UTC()
			return &t
		}(),
		SourceURL: in.SourceURL, Note: in.Note,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func scanProviderPriceRule(row pgx.Row, out *ProviderPriceRule) error {
	return row.Scan(
		&out.ID, &out.Provider, &out.Resource, &out.Meter, &out.Currency,
		&out.InputCacheHitPerMillion, &out.InputCacheMissPerMillion,
		&out.OutputPerMillion, &out.RequestUnitPrice,
		&out.RequestIncludedQuantity, &out.RequestAdditionalUnitPrice,
		&out.EffectiveFrom, &out.EffectiveTo, &out.SourceURL, &out.Note,
		&out.CreatedBy, &out.CreatedAt,
	)
}

const providerPriceRuleColumns = `
 id, provider, resource, meter, currency,
 input_cache_hit_per_million, input_cache_miss_per_million,
 output_per_million, request_unit_price,
 request_included_quantity, request_additional_unit_price,
 effective_from, effective_to, source_url, note, created_by, created_at`

func (s *Store) ListProviderPriceRules(ctx context.Context) ([]ProviderPriceRule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+providerPriceRuleColumns+`
		FROM provider_price_rules
		ORDER BY provider, resource, meter, effective_from DESC, id DESC`)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "读取供应商价格目录", err)
	}
	defer rows.Close()
	out := make([]ProviderPriceRule, 0)
	for rows.Next() {
		var rule ProviderPriceRule
		if err := rows.Scan(
			&rule.ID, &rule.Provider, &rule.Resource, &rule.Meter, &rule.Currency,
			&rule.InputCacheHitPerMillion, &rule.InputCacheMissPerMillion,
			&rule.OutputPerMillion, &rule.RequestUnitPrice,
			&rule.RequestIncludedQuantity, &rule.RequestAdditionalUnitPrice,
			&rule.EffectiveFrom, &rule.EffectiveTo, &rule.SourceURL, &rule.Note,
			&rule.CreatedBy, &rule.CreatedAt,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描供应商价格目录", err)
		}
		out = append(out, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历供应商价格目录", err)
	}
	return out, nil
}

// ReplaceProviderPriceRule atomically closes the current open price and creates
// a new effective-dated version. The advisory lock serializes concurrent owner
// updates for the same provider/resource/meter before the partial unique index.
func (s *Store) ReplaceProviderPriceRule(
	ctx context.Context,
	input ReplaceProviderPriceRuleInput,
) (*ProviderPriceRule, error) {
	in, err := normalizePriceRuleInput(input)
	if err != nil {
		return nil, err
	}
	requestHash, err := providerPriceRequestHash(in)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "生成价格更新摘要", err)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开始更新供应商价格", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		providerPricingLedgerLock,
	); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "锁定供应商价格账本", err)
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&dbNow); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "读取数据库时间", err)
	}
	if in.EffectiveFrom.IsZero() {
		in.EffectiveFrom = dbNow
	} else {
		in.EffectiveFrom = in.EffectiveFrom.UTC()
	}
	if in.EffectiveFrom.Before(dbNow.Add(-5 * time.Minute)) {
		return nil, types.NewAppError(types.CodeValidation,
			"新价格不能追溯修改已经发生的调用", types.ErrValidation)
	}
	lockKey := in.Provider + "\x1f" + in.Resource + "\x1f" + in.Meter
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "锁定供应商价格", err)
	}
	var replay ProviderPriceRule
	var replayHash string
	replayErr := scanProviderPriceRule(tx.QueryRow(ctx,
		`SELECT `+providerPriceRuleColumns+`
		   FROM provider_price_rules WHERE change_id=$1`,
		in.ChangeID), &replay)
	if replayErr == nil {
		if err := tx.QueryRow(ctx,
			`SELECT request_hash FROM provider_price_rules WHERE id=$1`, replay.ID,
		).Scan(&replayHash); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "读取价格更新回执", err)
		}
		if replayHash != requestHash {
			return nil, types.NewAppError(types.CodeConflict,
				"同一幂等键对应了不同价格更新", types.ErrConflict)
		}
		return &replay, nil
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeDatabase, "读取价格更新回执", replayErr)
	}

	var currentID int64
	var currentFrom time.Time
	err = tx.QueryRow(ctx,
		`SELECT id, effective_from
		   FROM provider_price_rules
		  WHERE provider=$1 AND resource=$2 AND meter=$3 AND effective_to IS NULL
		  FOR UPDATE`,
		in.Provider, in.Resource, in.Meter,
	).Scan(&currentID, &currentFrom)
	switch {
	case err == nil:
		if !in.EffectiveFrom.After(currentFrom) {
			return nil, types.NewAppError(types.CodeConflict,
				"新价格生效时间必须晚于当前版本", types.ErrConflict)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE provider_price_rules SET effective_to=$1 WHERE id=$2`,
			in.EffectiveFrom, currentID); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "关闭旧供应商价格", err)
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, types.NewAppError(types.CodeDatabase, "读取当前供应商价格", err)
	}

	var out ProviderPriceRule
	err = scanProviderPriceRule(tx.QueryRow(ctx,
		`INSERT INTO provider_price_rules (
		   provider, resource, meter, currency,
		   input_cache_hit_per_million, input_cache_miss_per_million,
		   output_per_million, request_unit_price,
		   request_included_quantity, request_additional_unit_price,
		   effective_from, source_url, note, created_by, change_id, request_hash
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		 RETURNING `+providerPriceRuleColumns,
		in.Provider, in.Resource, in.Meter, in.Currency,
		in.InputCacheHitPerMillion, in.InputCacheMissPerMillion,
		in.OutputPerMillion, in.RequestUnitPrice,
		in.RequestIncludedQuantity, in.RequestAdditionalUnitPrice,
		in.EffectiveFrom, in.SourceURL, in.Note, in.CreatedBy, in.ChangeID, requestHash,
	), &out)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("写入 %s/%s 新价格", in.Provider, in.Resource), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交供应商价格更新", err)
	}
	return &out, nil
}
