// Package policycandidate defines Vane's non-authoritative, content-addressed
// policy-candidate and offline-evaluation wire contracts.
//
// This package is deliberately dark. It has no database, model, capability,
// approval, rollout, or production-runtime dependency. A value produced here
// remains a candidate even when its offline evaluation passes; only a future,
// independently approved control plane may define later shadow/canary states.
package policycandidate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/server/types"
)

const (
	CandidateSchemaVersionV1         = "vane.policy-candidate/v1"
	DatasetSchemaVersionV1           = "vane.policy-evaluation-dataset/v1"
	OfflineEvalInputSchemaVersionV1  = "vane.policy-offline-evaluation-input/v1"
	OfflineEvalResultSchemaVersionV1 = "vane.policy-offline-evaluation-result/v1"

	LifecycleCandidateOnly = "candidate"
	PromotionAuthorityNone = "none"
	EvaluationStageReplay  = "offline_replay"

	maxContractItems = 100000
)

var ErrInvalidContract = errors.New("policycandidate: invalid contract")

// PrincipalV1 is the exact workspace/user authority captured for candidate
// generation. It is never a hint from which a tenant may be inferred.
type PrincipalV1 struct {
	TenantID                          types.TenantID       `json:"tenant_id"`
	UserID                            int64                `json:"user_id"`
	Role                              types.MembershipRole `json:"role"`
	ActorType                         types.ActorType      `json:"actor_type"`
	MembershipAuthorizationGeneration int64                `json:"membership_authorization_generation"`
}

type PolicyModuleKindV1 string

const (
	PolicyModulePromptPolicy       PolicyModuleKindV1 = "prompt_policy"
	PolicyModuleModelRoute         PolicyModuleKindV1 = "model_route"
	PolicyModuleCapabilityManifest PolicyModuleKindV1 = "capability_manifest"
)

func (k PolicyModuleKindV1) valid() bool {
	switch k {
	case PolicyModulePromptPolicy, PolicyModuleModelRoute,
		PolicyModuleCapabilityManifest:
		return true
	default:
		return false
	}
}

// PolicyModuleRefV1 names one immutable, separately versioned policy module.
// Its body is intentionally absent: evaluation logs carry identities, never
// prompts, tool schemas, user content, or credential material.
type PolicyModuleRefV1 struct {
	Kind    PolicyModuleKindV1 `json:"kind"`
	ID      string             `json:"id"`
	Version string             `json:"version"`
	Digest  string             `json:"digest"`
}

// RuntimeCompositionV1 freezes the exact aggregate Policy Manifest and its
// prompt, model-route, and capability-manifest modules.
type RuntimeCompositionV1 struct {
	PolicyManifestDigest string              `json:"policy_manifest_digest"`
	Modules              []PolicyModuleRefV1 `json:"modules"`
}

type CandidateTargetKindV1 string

const (
	CandidateTargetInteractiveAgent CandidateTargetKindV1 = "interactive_agent"
	CandidateTargetTaskRuntime      CandidateTargetKindV1 = "task_runtime"
)

type CandidateTargetV1 struct {
	Kind CandidateTargetKindV1 `json:"kind"`
	ID   string                `json:"id"`
}

// GeneratorRefV1 content-addresses the code/model policy that generated a
// candidate. It grants no approval or runtime authority.
type GeneratorRefV1 struct {
	ID             string `json:"id"`
	Version        string `json:"version"`
	ArtifactDigest string `json:"artifact_digest"`
}

type CandidateV1 struct {
	SchemaVersion       string               `json:"schema_version"`
	Principal           PrincipalV1          `json:"principal"`
	Target              CandidateTargetV1    `json:"target"`
	Baseline            RuntimeCompositionV1 `json:"baseline"`
	Proposed            RuntimeCompositionV1 `json:"proposed"`
	Generator           GeneratorRefV1       `json:"generator"`
	SourceDatasetDigest string               `json:"source_dataset_digest"`
	Lifecycle           string               `json:"lifecycle"`
	PromotionAuthority  string               `json:"promotion_authority"`
	CandidateDigest     string               `json:"candidate_digest"`
}

type CandidateInputV1 struct {
	Principal           PrincipalV1
	Target              CandidateTargetV1
	Baseline            RuntimeCompositionV1
	Proposed            RuntimeCompositionV1
	Generator           GeneratorRefV1
	SourceDatasetDigest string
}

func NewCandidateV1(input CandidateInputV1) (CandidateV1, error) {
	baseline, err := normalizeRuntime(input.Baseline)
	if err != nil {
		return CandidateV1{}, err
	}
	proposed, err := normalizeRuntime(input.Proposed)
	if err != nil {
		return CandidateV1{}, err
	}
	candidate := CandidateV1{
		SchemaVersion: CandidateSchemaVersionV1,
		Principal:     input.Principal, Target: input.Target,
		Baseline: baseline, Proposed: proposed, Generator: input.Generator,
		SourceDatasetDigest: input.SourceDatasetDigest,
		Lifecycle:           LifecycleCandidateOnly, PromotionAuthority: PromotionAuthorityNone,
	}
	if err := candidate.validateFields(); err != nil {
		return CandidateV1{}, err
	}
	candidate.CandidateDigest, err = digestJSON(candidate.withoutDigest())
	if err != nil {
		return CandidateV1{}, invalid("candidate cannot be encoded")
	}
	return candidate, nil
}

func (c CandidateV1) Validate() error {
	if err := c.validateFields(); err != nil {
		return err
	}
	if !validDigest(c.CandidateDigest) {
		return invalid("candidate digest is invalid")
	}
	baseline, err := normalizeRuntime(c.Baseline)
	if err != nil || !slices.Equal(baseline.Modules, c.Baseline.Modules) {
		return invalid("baseline is not canonical")
	}
	proposed, err := normalizeRuntime(c.Proposed)
	if err != nil || !slices.Equal(proposed.Modules, c.Proposed.Modules) {
		return invalid("proposed composition is not canonical")
	}
	digest, err := digestJSON(c.withoutDigest())
	if err != nil || digest != c.CandidateDigest {
		return invalid("candidate digest differs")
	}
	return nil
}

func (c CandidateV1) validateFields() error {
	if c.SchemaVersion != CandidateSchemaVersionV1 ||
		c.Lifecycle != LifecycleCandidateOnly ||
		c.PromotionAuthority != PromotionAuthorityNone {
		return invalid("candidate header is invalid")
	}
	if err := c.Principal.validate(); err != nil {
		return err
	}
	if err := c.Target.validate(); err != nil {
		return err
	}
	if err := c.Baseline.validate(); err != nil {
		return err
	}
	if err := c.Proposed.validate(); err != nil {
		return err
	}
	if slices.Equal(c.Baseline.Modules, c.Proposed.Modules) &&
		c.Baseline.PolicyManifestDigest == c.Proposed.PolicyManifestDigest {
		return invalid("candidate is a no-op")
	}
	if !validIdentifier(c.Generator.ID) || !validVersion(c.Generator.Version) ||
		!validDigest(c.Generator.ArtifactDigest) ||
		!validDigest(c.SourceDatasetDigest) {
		return invalid("candidate identity is invalid")
	}
	return nil
}

func (c CandidateV1) withoutDigest() CandidateV1 {
	c.CandidateDigest = ""
	return c
}

type FailureTypeV1 string

const (
	FailureNone           FailureTypeV1 = ""
	FailureProvider       FailureTypeV1 = "provider_error"
	FailureTimeout        FailureTypeV1 = "timeout"
	FailureInvalidOutput  FailureTypeV1 = "invalid_output"
	FailurePolicyDenied   FailureTypeV1 = "policy_denied"
	FailureBudgetExceeded FailureTypeV1 = "budget_exceeded"
	FailureCapability     FailureTypeV1 = "capability_error"
	FailureInternal       FailureTypeV1 = "internal"
)

func (f FailureTypeV1) valid() bool {
	switch f {
	case FailureNone, FailureProvider, FailureTimeout, FailureInvalidOutput,
		FailurePolicyDenied, FailureBudgetExceeded, FailureCapability, FailureInternal:
		return true
	default:
		return false
	}
}

// FeedbackSignalV1 carries only stable categorical feedback. Detail text is
// intentionally not part of the offline-evaluation contract.
type FeedbackSignalV1 struct {
	Action types.FeedbackAction `json:"action"`
	Reason types.FeedbackReason `json:"reason"`
}

type ModelRefV1 struct {
	Provider    string `json:"provider"`
	Model       string `json:"model"`
	RouteDigest string `json:"route_digest"`
}

// ObservationV1 is a credential-free projection of one immutable production
// run. ReplayFixtureDigest points at a separately controlled fixture; raw
// prompts, completions, tool arguments/results, feedback detail, URLs, and
// credential references never enter this wire.
type ObservationV1 struct {
	Principal                  PrincipalV1          `json:"principal"`
	TaskID                     string               `json:"task_id"`
	RunSnapshotID              int64                `json:"run_snapshot_id"`
	RunSnapshotReferenceDigest string               `json:"run_snapshot_reference_digest"`
	TraceID                    string               `json:"trace_id"`
	Runtime                    RuntimeCompositionV1 `json:"runtime"`
	Model                      ModelRefV1           `json:"model"`
	ReplayFixtureDigest        string               `json:"replay_fixture_digest"`
	CostMicroUSD               int64                `json:"cost_micro_usd"`
	FailureType                FailureTypeV1        `json:"failure_type"`
	Feedback                   FeedbackSignalV1     `json:"feedback"`
}

type EvaluationDatasetV1 struct {
	SchemaVersion string          `json:"schema_version"`
	Principal     PrincipalV1     `json:"principal"`
	Observations  []ObservationV1 `json:"observations"`
	DatasetDigest string          `json:"dataset_digest"`
}

func NewEvaluationDatasetV1(
	principal PrincipalV1,
	observations []ObservationV1,
) (EvaluationDatasetV1, error) {
	dataset := EvaluationDatasetV1{
		SchemaVersion: DatasetSchemaVersionV1,
		Principal:     principal,
		Observations:  slices.Clone(observations),
	}
	for index := range dataset.Observations {
		normalized, err := normalizeRuntime(dataset.Observations[index].Runtime)
		if err != nil {
			return EvaluationDatasetV1{}, err
		}
		dataset.Observations[index].Runtime = normalized
	}
	slices.SortFunc(dataset.Observations, compareObservations)
	if err := dataset.validateFields(); err != nil {
		return EvaluationDatasetV1{}, err
	}
	var err error
	dataset.DatasetDigest, err = digestJSON(dataset.withoutDigest())
	if err != nil {
		return EvaluationDatasetV1{}, invalid("dataset cannot be encoded")
	}
	return dataset, nil
}

func (d EvaluationDatasetV1) Validate() error {
	if err := d.validateFields(); err != nil {
		return err
	}
	if !validDigest(d.DatasetDigest) {
		return invalid("dataset digest is invalid")
	}
	canonical := slices.Clone(d.Observations)
	slices.SortFunc(canonical, compareObservations)
	if !slices.EqualFunc(canonical, d.Observations, func(a, b ObservationV1) bool {
		return reflect.DeepEqual(a, b)
	}) {
		return invalid("dataset observations are not canonical")
	}
	digest, err := digestJSON(d.withoutDigest())
	if err != nil || digest != d.DatasetDigest {
		return invalid("dataset digest differs")
	}
	return nil
}

func (d EvaluationDatasetV1) validateFields() error {
	if d.SchemaVersion != DatasetSchemaVersionV1 ||
		len(d.Observations) == 0 || len(d.Observations) > maxContractItems {
		return invalid("dataset header is invalid")
	}
	if err := d.Principal.validate(); err != nil {
		return err
	}
	for index := range d.Observations {
		observation := d.Observations[index]
		if observation.Principal != d.Principal {
			return invalid("observation principal differs")
		}
		if err := observation.validate(); err != nil {
			return err
		}
		if index > 0 && compareObservations(d.Observations[index-1], observation) == 0 {
			return invalid("duplicate observation")
		}
	}
	return nil
}

func (d EvaluationDatasetV1) withoutDigest() EvaluationDatasetV1 {
	d.DatasetDigest = ""
	return d
}

type GraderKindV1 string

const (
	GraderCode  GraderKindV1 = "code"
	GraderModel GraderKindV1 = "model"
	GraderHuman GraderKindV1 = "human_rubric"
)

func (k GraderKindV1) valid() bool {
	return k == GraderCode || k == GraderModel || k == GraderHuman
}

type GraderRefV1 struct {
	Kind    GraderKindV1 `json:"kind"`
	ID      string       `json:"id"`
	Version string       `json:"version"`
	Digest  string       `json:"digest"`
}

type OfflineEvalInputV1 struct {
	SchemaVersion      string              `json:"schema_version"`
	Principal          PrincipalV1         `json:"principal"`
	CandidateDigest    string              `json:"candidate_digest"`
	Dataset            EvaluationDatasetV1 `json:"dataset"`
	Graders            []GraderRefV1       `json:"graders"`
	Stage              string              `json:"stage"`
	PromotionAuthority string              `json:"promotion_authority"`
	InputDigest        string              `json:"input_digest"`
}

func NewOfflineEvalInputV1(
	candidate CandidateV1,
	dataset EvaluationDatasetV1,
	graders []GraderRefV1,
) (OfflineEvalInputV1, error) {
	if err := candidate.Validate(); err != nil {
		return OfflineEvalInputV1{}, err
	}
	if err := dataset.Validate(); err != nil {
		return OfflineEvalInputV1{}, err
	}
	input := OfflineEvalInputV1{
		SchemaVersion: OfflineEvalInputSchemaVersionV1,
		Principal:     candidate.Principal, CandidateDigest: candidate.CandidateDigest,
		Dataset: dataset, Graders: slices.Clone(graders),
		Stage: EvaluationStageReplay, PromotionAuthority: PromotionAuthorityNone,
	}
	slices.SortFunc(input.Graders, compareGraders)
	if err := input.validateFor(candidate); err != nil {
		return OfflineEvalInputV1{}, err
	}
	var err error
	input.InputDigest, err = digestJSON(input.withoutDigest())
	if err != nil {
		return OfflineEvalInputV1{}, invalid("offline input cannot be encoded")
	}
	return input, nil
}

func (i OfflineEvalInputV1) ValidateFor(candidate CandidateV1) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if err := i.validateFor(candidate); err != nil {
		return err
	}
	if !validDigest(i.InputDigest) {
		return invalid("offline input digest is invalid")
	}
	canonical := slices.Clone(i.Graders)
	slices.SortFunc(canonical, compareGraders)
	if !slices.Equal(canonical, i.Graders) {
		return invalid("grader references are not canonical")
	}
	digest, err := digestJSON(i.withoutDigest())
	if err != nil || digest != i.InputDigest {
		return invalid("offline input digest differs")
	}
	return nil
}

func (i OfflineEvalInputV1) validateFor(candidate CandidateV1) error {
	if i.SchemaVersion != OfflineEvalInputSchemaVersionV1 ||
		i.Stage != EvaluationStageReplay ||
		i.PromotionAuthority != PromotionAuthorityNone ||
		i.CandidateDigest != candidate.CandidateDigest ||
		i.Principal != candidate.Principal ||
		i.Dataset.Principal != candidate.Principal ||
		i.Dataset.DatasetDigest != candidate.SourceDatasetDigest ||
		len(i.Graders) == 0 ||
		len(i.Graders) > maxContractItems {
		return invalid("offline input header is invalid")
	}
	if err := i.Dataset.Validate(); err != nil {
		return err
	}
	for _, observation := range i.Dataset.Observations {
		if !runtimeEqual(observation.Runtime, candidate.Baseline) {
			return invalid("dataset baseline composition differs")
		}
	}
	for index, grader := range i.Graders {
		if err := grader.validate(); err != nil {
			return err
		}
		if index > 0 && compareGraders(i.Graders[index-1], grader) == 0 {
			return invalid("duplicate grader")
		}
	}
	return nil
}

func (i OfflineEvalInputV1) withoutDigest() OfflineEvalInputV1 {
	i.InputDigest = ""
	return i
}

type EvaluationMetricsV1 struct {
	SampleCount             int   `json:"sample_count"`
	SuccessfulReplays       int   `json:"successful_replays"`
	FailedReplays           int   `json:"failed_replays"`
	QualityWins             int   `json:"quality_wins"`
	QualityLosses           int   `json:"quality_losses"`
	QualityTies             int   `json:"quality_ties"`
	RegressionCount         int   `json:"regression_count"`
	BaselineFailures        int   `json:"baseline_failures"`
	CandidateFailures       int   `json:"candidate_failures"`
	BaselineCostMicroUSD    int64 `json:"baseline_cost_micro_usd"`
	CandidateCostMicroUSD   int64 `json:"candidate_cost_micro_usd"`
	QualityDeltaBasisPoints int   `json:"quality_delta_basis_points"`
}

type GraderResultV1 struct {
	Grader           GraderRefV1 `json:"grader"`
	Passed           bool        `json:"passed"`
	ScoreBasisPoints int         `json:"score_basis_points"`
	FindingCount     int         `json:"finding_count"`
	EvidenceDigest   string      `json:"evidence_digest"`
}

type EvaluationVerdictV1 string

const (
	EvaluationPassed       EvaluationVerdictV1 = "passed"
	EvaluationFailed       EvaluationVerdictV1 = "failed"
	EvaluationInconclusive EvaluationVerdictV1 = "inconclusive"
)

type CandidateRecommendationV1 string

const (
	RecommendationManualReview CandidateRecommendationV1 = "manual_review"
	RecommendationReject       CandidateRecommendationV1 = "reject_candidate"
	RecommendationRetain       CandidateRecommendationV1 = "retain_candidate"
)

type OfflineEvalResultV1 struct {
	SchemaVersion      string                    `json:"schema_version"`
	Principal          PrincipalV1               `json:"principal"`
	CandidateDigest    string                    `json:"candidate_digest"`
	InputDigest        string                    `json:"input_digest"`
	Stage              string                    `json:"stage"`
	Metrics            EvaluationMetricsV1       `json:"metrics"`
	GraderResults      []GraderResultV1          `json:"grader_results"`
	Verdict            EvaluationVerdictV1       `json:"verdict"`
	Recommendation     CandidateRecommendationV1 `json:"recommendation"`
	PromotionAuthority string                    `json:"promotion_authority"`
	ResultDigest       string                    `json:"result_digest"`
}

func NewOfflineEvalResultV1(
	input OfflineEvalInputV1,
	candidate CandidateV1,
	metrics EvaluationMetricsV1,
	graderResults []GraderResultV1,
) (OfflineEvalResultV1, error) {
	if err := input.ValidateFor(candidate); err != nil {
		return OfflineEvalResultV1{}, err
	}
	result := OfflineEvalResultV1{
		SchemaVersion: OfflineEvalResultSchemaVersionV1,
		Principal:     input.Principal, CandidateDigest: candidate.CandidateDigest,
		InputDigest: input.InputDigest, Stage: EvaluationStageReplay,
		Metrics: metrics, GraderResults: slices.Clone(graderResults),
		PromotionAuthority: PromotionAuthorityNone,
	}
	slices.SortFunc(result.GraderResults, compareGraderResults)
	result.Verdict, result.Recommendation = deriveVerdict(metrics, result.GraderResults)
	if err := result.validateFor(input, candidate); err != nil {
		return OfflineEvalResultV1{}, err
	}
	var err error
	result.ResultDigest, err = digestJSON(result.withoutDigest())
	if err != nil {
		return OfflineEvalResultV1{}, invalid("offline result cannot be encoded")
	}
	return result, nil
}

func (r OfflineEvalResultV1) ValidateFor(
	input OfflineEvalInputV1,
	candidate CandidateV1,
) error {
	if err := input.ValidateFor(candidate); err != nil {
		return err
	}
	if err := r.validateFor(input, candidate); err != nil {
		return err
	}
	if !validDigest(r.ResultDigest) {
		return invalid("offline result digest is invalid")
	}
	canonical := slices.Clone(r.GraderResults)
	slices.SortFunc(canonical, compareGraderResults)
	if !slices.Equal(canonical, r.GraderResults) {
		return invalid("grader results are not canonical")
	}
	digest, err := digestJSON(r.withoutDigest())
	if err != nil || digest != r.ResultDigest {
		return invalid("offline result digest differs")
	}
	return nil
}

func (r OfflineEvalResultV1) validateFor(
	input OfflineEvalInputV1,
	candidate CandidateV1,
) error {
	if r.SchemaVersion != OfflineEvalResultSchemaVersionV1 ||
		r.Principal != input.Principal || r.Principal != candidate.Principal ||
		r.CandidateDigest != candidate.CandidateDigest ||
		r.InputDigest != input.InputDigest || r.Stage != EvaluationStageReplay ||
		r.PromotionAuthority != PromotionAuthorityNone ||
		len(r.GraderResults) != len(input.Graders) {
		return invalid("offline result header is invalid")
	}
	if err := r.Metrics.validate(len(input.Dataset.Observations)); err != nil {
		return err
	}
	for index, graderResult := range r.GraderResults {
		if err := graderResult.validate(); err != nil {
			return err
		}
		if index > 0 && compareGraderResults(r.GraderResults[index-1], graderResult) == 0 {
			return invalid("duplicate grader result")
		}
	}
	for index := range input.Graders {
		if input.Graders[index] != r.GraderResults[index].Grader {
			return invalid("grader result set differs")
		}
	}
	verdict, recommendation := deriveVerdict(r.Metrics, r.GraderResults)
	if r.Verdict != verdict || r.Recommendation != recommendation {
		return invalid("verdict or recommendation differs")
	}
	return nil
}

func (r OfflineEvalResultV1) withoutDigest() OfflineEvalResultV1 {
	r.ResultDigest = ""
	return r
}

func (p PrincipalV1) validate() error {
	if p.TenantID <= 0 || p.UserID <= 0 || !p.Role.Valid() ||
		!p.ActorType.Valid() || p.MembershipAuthorizationGeneration <= 0 {
		return invalid("principal is invalid")
	}
	return nil
}

func (t CandidateTargetV1) validate() error {
	if (t.Kind != CandidateTargetInteractiveAgent && t.Kind != CandidateTargetTaskRuntime) ||
		!validIdentifier(t.ID) {
		return invalid("candidate target is invalid")
	}
	return nil
}

func (r RuntimeCompositionV1) validate() error {
	if !validDigest(r.PolicyManifestDigest) || len(r.Modules) != 3 {
		return invalid("runtime composition header is invalid")
	}
	seen := make(map[PolicyModuleKindV1]struct{}, len(r.Modules))
	for _, module := range r.Modules {
		if err := module.validate(); err != nil {
			return err
		}
		if _, duplicate := seen[module.Kind]; duplicate {
			return invalid("duplicate policy module kind")
		}
		seen[module.Kind] = struct{}{}
	}
	for _, required := range []PolicyModuleKindV1{
		PolicyModulePromptPolicy, PolicyModuleModelRoute, PolicyModuleCapabilityManifest,
	} {
		if _, exists := seen[required]; !exists {
			return invalid("required policy module is missing")
		}
	}
	return nil
}

func (r PolicyModuleRefV1) validate() error {
	if !r.Kind.valid() || !validIdentifier(r.ID) || !validVersion(r.Version) ||
		!validDigest(r.Digest) {
		return invalid("policy module reference is invalid")
	}
	return nil
}

func normalizeRuntime(runtime RuntimeCompositionV1) (RuntimeCompositionV1, error) {
	runtime.Modules = slices.Clone(runtime.Modules)
	slices.SortFunc(runtime.Modules, comparePolicyModules)
	if err := runtime.validate(); err != nil {
		return RuntimeCompositionV1{}, err
	}
	return runtime, nil
}

func (o ObservationV1) validate() error {
	if err := o.Principal.validate(); err != nil {
		return err
	}
	if !validIdentifier(o.TaskID) || o.RunSnapshotID <= 0 ||
		!validDigest(o.RunSnapshotReferenceDigest) || !validIdentifier(o.TraceID) ||
		!validIdentifier(o.Model.Provider) || !validIdentifier(o.Model.Model) ||
		!validDigest(o.Model.RouteDigest) || !validDigest(o.ReplayFixtureDigest) ||
		o.CostMicroUSD < 0 || !o.FailureType.valid() {
		return invalid("observation is invalid")
	}
	if err := o.Runtime.validate(); err != nil {
		return err
	}
	normalized, err := normalizeRuntime(o.Runtime)
	if err != nil || !slices.Equal(normalized.Modules, o.Runtime.Modules) {
		return invalid("observation runtime is not canonical")
	}
	modelModule, ok := moduleByKind(o.Runtime, PolicyModuleModelRoute)
	if !ok || modelModule.Digest != o.Model.RouteDigest {
		return invalid("observation model route differs")
	}
	return o.Feedback.validate()
}

func (f FeedbackSignalV1) validate() error {
	switch f.Action {
	case "":
		if f.Reason != "" {
			return invalid("feedback reason without action")
		}
	case types.FeedbackActionInterested, types.FeedbackActionNotInterested,
		types.FeedbackActionDeepDive, types.FeedbackActionQuestion:
		if f.Reason != "" {
			return invalid("feedback reason is not applicable")
		}
	case types.FeedbackActionMisjudged:
		if !f.Reason.Valid() {
			return invalid("misjudged feedback reason is invalid")
		}
	default:
		return invalid("feedback action is invalid")
	}
	return nil
}

func (g GraderRefV1) validate() error {
	if !g.Kind.valid() || !validIdentifier(g.ID) || !validVersion(g.Version) ||
		!validDigest(g.Digest) {
		return invalid("grader reference is invalid")
	}
	return nil
}

func (m EvaluationMetricsV1) validate(sampleCount int) error {
	if sampleCount <= 0 || m.SampleCount != sampleCount ||
		m.SuccessfulReplays < 0 || m.FailedReplays < 0 ||
		m.SuccessfulReplays+m.FailedReplays != sampleCount ||
		m.QualityWins < 0 || m.QualityLosses < 0 || m.QualityTies < 0 ||
		m.QualityWins+m.QualityLosses+m.QualityTies != m.SuccessfulReplays ||
		m.RegressionCount < 0 || m.RegressionCount > m.SuccessfulReplays ||
		m.BaselineFailures < 0 || m.BaselineFailures > sampleCount ||
		m.CandidateFailures < 0 || m.CandidateFailures > sampleCount ||
		m.BaselineCostMicroUSD < 0 || m.CandidateCostMicroUSD < 0 ||
		m.QualityDeltaBasisPoints < -10000 || m.QualityDeltaBasisPoints > 10000 {
		return invalid("evaluation metrics are inconsistent")
	}
	return nil
}

func (r GraderResultV1) validate() error {
	if err := r.Grader.validate(); err != nil {
		return err
	}
	if r.ScoreBasisPoints < 0 || r.ScoreBasisPoints > 10000 ||
		r.FindingCount < 0 || !validDigest(r.EvidenceDigest) {
		return invalid("grader result is invalid")
	}
	return nil
}

func deriveVerdict(
	metrics EvaluationMetricsV1,
	graders []GraderResultV1,
) (EvaluationVerdictV1, CandidateRecommendationV1) {
	if metrics.FailedReplays > 0 {
		return EvaluationInconclusive, RecommendationRetain
	}
	for _, grader := range graders {
		if !grader.Passed {
			return EvaluationFailed, RecommendationReject
		}
	}
	if metrics.RegressionCount > 0 {
		return EvaluationFailed, RecommendationReject
	}
	return EvaluationPassed, RecommendationManualReview
}

func moduleByKind(
	runtime RuntimeCompositionV1,
	kind PolicyModuleKindV1,
) (PolicyModuleRefV1, bool) {
	for _, module := range runtime.Modules {
		if module.Kind == kind {
			return module, true
		}
	}
	return PolicyModuleRefV1{}, false
}

func runtimeEqual(a, b RuntimeCompositionV1) bool {
	return a.PolicyManifestDigest == b.PolicyManifestDigest &&
		slices.Equal(a.Modules, b.Modules)
}

func comparePolicyModules(a, b PolicyModuleRefV1) int {
	return strings.Compare(
		string(a.Kind)+"\x00"+a.ID+"\x00"+a.Version+"\x00"+a.Digest,
		string(b.Kind)+"\x00"+b.ID+"\x00"+b.Version+"\x00"+b.Digest,
	)
}

func compareObservations(a, b ObservationV1) int {
	if a.TaskID != b.TaskID {
		return strings.Compare(a.TaskID, b.TaskID)
	}
	if a.RunSnapshotID < b.RunSnapshotID {
		return -1
	}
	if a.RunSnapshotID > b.RunSnapshotID {
		return 1
	}
	if a.TraceID != b.TraceID {
		return strings.Compare(a.TraceID, b.TraceID)
	}
	return strings.Compare(a.ReplayFixtureDigest, b.ReplayFixtureDigest)
}

func compareGraders(a, b GraderRefV1) int {
	return strings.Compare(
		string(a.Kind)+"\x00"+a.ID+"\x00"+a.Version+"\x00"+a.Digest,
		string(b.Kind)+"\x00"+b.ID+"\x00"+b.Version+"\x00"+b.Digest,
	)
}

func compareGraderResults(a, b GraderResultV1) int {
	return compareGraders(a.Grader, b.Grader)
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value ||
		strings.HasPrefix(strings.ToLower(value), "sk-") ||
		strings.HasPrefix(strings.ToLower(value), "ghp_") ||
		strings.HasPrefix(strings.ToLower(value), "github_pat_") ||
		strings.HasPrefix(strings.ToLower(value), "xox") {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("-_.:/", rune(char)) {
			continue
		}
		return false
	}
	return true
}

func validVersion(value string) bool {
	return len(value) >= 2 && len(value) <= 64 && value[0] == 'v' &&
		validIdentifier(value)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func digestJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func invalid(part string) error {
	return fmt.Errorf("%w: %s", ErrInvalidContract, part)
}
