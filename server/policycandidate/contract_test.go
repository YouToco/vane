package policycandidate

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestPolicyCandidateOfflineEvaluationCanonicalRoundTrip(t *testing.T) {
	dataset, candidate, input, result := testContractTrain(t)

	datasetPayload, err := EncodeEvaluationDatasetV1(dataset)
	if err != nil {
		t.Fatal(err)
	}
	decodedDataset, err := DecodeEvaluationDatasetV1(datasetPayload)
	if err != nil || !reflect.DeepEqual(decodedDataset, dataset) {
		t.Fatalf("dataset round trip=%+v err=%v", decodedDataset, err)
	}

	candidatePayload, err := EncodeCandidateV1(candidate)
	if err != nil {
		t.Fatal(err)
	}
	decodedCandidate, err := DecodeCandidateV1(candidatePayload)
	if err != nil || !reflect.DeepEqual(decodedCandidate, candidate) {
		t.Fatalf("candidate round trip=%+v err=%v", decodedCandidate, err)
	}

	inputPayload, err := EncodeOfflineEvalInputV1(input, candidate)
	if err != nil {
		t.Fatal(err)
	}
	decodedInput, err := DecodeOfflineEvalInputV1(inputPayload, candidate)
	if err != nil || !reflect.DeepEqual(decodedInput, input) {
		t.Fatalf("input round trip=%+v err=%v", decodedInput, err)
	}

	resultPayload, err := EncodeOfflineEvalResultV1(result, input, candidate)
	if err != nil {
		t.Fatal(err)
	}
	decodedResult, err := DecodeOfflineEvalResultV1(resultPayload, input, candidate)
	if err != nil || !reflect.DeepEqual(decodedResult, result) {
		t.Fatalf("result round trip=%+v err=%v", decodedResult, err)
	}

	for name, payload := range map[string][]byte{
		"dataset": datasetPayload, "candidate": candidatePayload,
		"input": inputPayload, "result": resultPayload,
	} {
		if bytes.Contains(payload, []byte("system prompt")) ||
			bytes.Contains(payload, []byte("sk-secret")) ||
			bytes.Contains(payload, []byte("credential")) ||
			bytes.Contains(payload, []byte("completion")) ||
			bytes.Contains(payload, []byte("feedback detail")) {
			t.Fatalf("%s payload contains forbidden raw material: %s", name, payload)
		}
	}
}

func TestCandidateContentAddressesEveryAuthorityField(t *testing.T) {
	_, candidate, _, _ := testContractTrain(t)
	otherDigest := strings.Repeat("f", 64)
	tests := []struct {
		name string
		edit func(*CandidateV1)
	}{
		{"tenant", func(value *CandidateV1) { value.Principal.TenantID++ }},
		{"user", func(value *CandidateV1) { value.Principal.UserID++ }},
		{"role", func(value *CandidateV1) { value.Principal.Role = types.MembershipRoleAdmin }},
		{"actor", func(value *CandidateV1) { value.Principal.ActorType = types.ActorTypeServiceAccount }},
		{"generation", func(value *CandidateV1) { value.Principal.MembershipAuthorizationGeneration++ }},
		{"target", func(value *CandidateV1) { value.Target.ID = "other-task" }},
		{"baseline manifest", func(value *CandidateV1) { value.Baseline.PolicyManifestDigest = otherDigest }},
		{"baseline module", func(value *CandidateV1) { value.Baseline.Modules[0].Digest = otherDigest }},
		{"proposed manifest", func(value *CandidateV1) { value.Proposed.PolicyManifestDigest = otherDigest }},
		{"proposed module", func(value *CandidateV1) { value.Proposed.Modules[0].Digest = otherDigest }},
		{"generator", func(value *CandidateV1) { value.Generator.ArtifactDigest = otherDigest }},
		{"dataset", func(value *CandidateV1) { value.SourceDatasetDigest = otherDigest }},
		{"lifecycle", func(value *CandidateV1) { value.Lifecycle = "approved" }},
		{"promotion", func(value *CandidateV1) { value.PromotionAuthority = "automatic" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneCandidate(candidate)
			test.edit(&changed)
			if err := changed.Validate(); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("Validate error=%v, want ErrInvalidContract", err)
			}
		})
	}
}

func TestEvaluationContractsRejectCrossPrincipalAndPolicyDrift(t *testing.T) {
	dataset, candidate, input, result := testContractTrain(t)

	wrongDataset := dataset
	wrongDataset.Principal.UserID++
	if _, err := NewCandidateV1(CandidateInputV1{
		Principal: candidate.Principal, Target: candidate.Target,
		Baseline: candidate.Baseline, Proposed: candidate.Proposed,
		Generator: candidate.Generator, SourceDatasetDigest: wrongDataset.DatasetDigest,
	}); err != nil {
		t.Fatalf("candidate builder should not infer a principal from dataset: %v", err)
	}
	if _, err := NewOfflineEvalInputV1(candidate, wrongDataset, input.Graders); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("cross-principal dataset error=%v", err)
	}

	drifted := cloneDataset(dataset)
	drifted.Observations[0].Runtime.PolicyManifestDigest = strings.Repeat("e", 64)
	drifted.DatasetDigest, _ = digestJSON(drifted.withoutDigest())
	if err := drifted.Validate(); err != nil {
		t.Fatalf("self-consistent historical policy drift should remain a new dataset: %v", err)
	}
	if _, err := NewOfflineEvalInputV1(candidate, drifted, input.Graders); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("candidate accepted different source dataset: %v", err)
	}
	driftCandidate, err := NewCandidateV1(CandidateInputV1{
		Principal: candidate.Principal, Target: candidate.Target,
		Baseline: candidate.Baseline, Proposed: candidate.Proposed,
		Generator: candidate.Generator, SourceDatasetDigest: drifted.DatasetDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewOfflineEvalInputV1(driftCandidate, drifted, input.Graders); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("offline input accepted mixed baseline composition: %v", err)
	}

	changedInput := input
	changedInput.Principal.UserID++
	if err := changedInput.ValidateFor(candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("cross-user input error=%v", err)
	}

	changedResult := result
	changedResult.Principal.TenantID++
	if err := changedResult.ValidateFor(input, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("cross-tenant result error=%v", err)
	}
}

func TestDatasetCanonicalizesOrderAndRejectsDuplicateRunIdentity(t *testing.T) {
	principal := testPrincipal()
	baseline := testRuntime("1", "2", "3", "4")
	first := testObservation(principal, baseline, "task-b", 2, "trace-b", "5")
	second := testObservation(principal, baseline, "task-a", 1, "trace-a", "6")
	dataset, err := NewEvaluationDatasetV1(principal, []ObservationV1{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Observations[0].TaskID != "task-a" ||
		dataset.Observations[1].TaskID != "task-b" ||
		!slices.IsSortedFunc(dataset.Observations, compareObservations) ||
		!slices.IsSortedFunc(dataset.Observations[0].Runtime.Modules, comparePolicyModules) {
		t.Fatalf("dataset is not canonical: %+v", dataset)
	}

	duplicate := first
	duplicate.CostMicroUSD++
	if _, err := NewEvaluationDatasetV1(principal, []ObservationV1{first, duplicate}); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("duplicate run identity error=%v", err)
	}

	crossTenant := second
	crossTenant.Principal.TenantID++
	if _, err := NewEvaluationDatasetV1(principal, []ObservationV1{first, crossTenant}); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("cross-tenant observation error=%v", err)
	}
}

func TestCandidateRejectsNoOpAndIncompleteComposition(t *testing.T) {
	principal := testPrincipal()
	runtime := testRuntime("1", "2", "3", "4")
	dataset, err := NewEvaluationDatasetV1(
		principal,
		[]ObservationV1{testObservation(principal, runtime, "task-a", 1, "trace-a", "5")},
	)
	if err != nil {
		t.Fatal(err)
	}
	base := CandidateInputV1{
		Principal: principal,
		Target:    CandidateTargetV1{Kind: CandidateTargetTaskRuntime, ID: "task-a"},
		Baseline:  runtime, Proposed: runtime,
		Generator:           GeneratorRefV1{ID: "optimizer", Version: "v1", ArtifactDigest: strings.Repeat("6", 64)},
		SourceDatasetDigest: dataset.DatasetDigest,
	}
	if _, err := NewCandidateV1(base); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("no-op candidate error=%v", err)
	}

	incomplete := runtime
	incomplete.Modules = incomplete.Modules[:2]
	base.Proposed = incomplete
	if _, err := NewCandidateV1(base); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("incomplete composition error=%v", err)
	}
}

func TestOfflineResultVerdictCannotPromoteCandidate(t *testing.T) {
	_, candidate, input, passed := testContractTrain(t)
	if passed.Verdict != EvaluationPassed ||
		passed.Recommendation != RecommendationManualReview ||
		passed.PromotionAuthority != PromotionAuthorityNone ||
		candidate.Lifecycle != LifecycleCandidateOnly {
		t.Fatalf("passed offline result widened authority: %+v candidate=%+v", passed, candidate)
	}

	failedGrader := passed.GraderResults
	failedGrader[0].Passed = false
	failedGrader[0].FindingCount = 1
	failed, err := NewOfflineEvalResultV1(input, candidate, passed.Metrics, failedGrader)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Verdict != EvaluationFailed || failed.Recommendation != RecommendationReject {
		t.Fatalf("failed result=%+v", failed)
	}

	inconclusiveMetrics := passed.Metrics
	inconclusiveMetrics.SuccessfulReplays--
	inconclusiveMetrics.FailedReplays++
	inconclusiveMetrics.QualityTies--
	inconclusive, err := NewOfflineEvalResultV1(input, candidate, inconclusiveMetrics, passed.GraderResults)
	if err != nil {
		t.Fatal(err)
	}
	if inconclusive.Verdict != EvaluationInconclusive ||
		inconclusive.Recommendation != RecommendationRetain {
		t.Fatalf("inconclusive result=%+v", inconclusive)
	}

	tampered := passed
	tampered.Recommendation = RecommendationManualReview
	tampered.PromotionAuthority = "production"
	if err := tampered.ValidateFor(input, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("promotion tamper error=%v", err)
	}
}

func TestStrictDecodeRejectsUnknownDuplicateMissingAndNonCanonicalJSON(t *testing.T) {
	_, candidate, _, _ := testContractTrain(t)
	payload, err := EncodeCandidateV1(candidate)
	if err != nil {
		t.Fatal(err)
	}

	unknown := bytes.Replace(payload, []byte(`"candidate_digest"`), []byte(`"unknown":"x","candidate_digest"`), 1)
	duplicate := bytes.Replace(payload, []byte(`"schema_version"`), []byte(`"schema_version":"x","schema_version"`), 1)
	missing := bytes.Replace(payload, []byte(`"promotion_authority":"none",`), nil, 1)
	pretty := &bytes.Buffer{}
	if err := json.Indent(pretty, payload, "", "  "); err != nil {
		t.Fatal(err)
	}
	for name, changed := range map[string][]byte{
		"unknown": unknown, "duplicate": duplicate, "missing": missing, "pretty": pretty.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeCandidateV1(changed); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("DecodeCandidateV1 error=%v", err)
			}
		})
	}
}

func TestObservationRejectsRawOrUnscopedShapes(t *testing.T) {
	principal := testPrincipal()
	runtime := testRuntime("1", "2", "3", "4")
	base := testObservation(principal, runtime, "task-a", 1, "trace-a", "5")
	tests := []struct {
		name string
		edit func(*ObservationV1)
	}{
		{"missing task", func(value *ObservationV1) { value.TaskID = "" }},
		{"missing snapshot", func(value *ObservationV1) { value.RunSnapshotID = 0 }},
		{"route drift", func(value *ObservationV1) { value.Model.RouteDigest = strings.Repeat("f", 64) }},
		{"negative cost", func(value *ObservationV1) { value.CostMicroUSD = -1 }},
		{"raw feedback", func(value *ObservationV1) { value.Feedback.Action = "feedback detail" }},
		{"reason without action", func(value *ObservationV1) { value.Feedback.Reason = types.FeedbackReasonOther }},
		{"unknown failure", func(value *ObservationV1) { value.FailureType = "raw provider failure text" }},
		{"secret-like model", func(value *ObservationV1) { value.Model.Model = "sk-secret" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.edit(&changed)
			_, err := NewEvaluationDatasetV1(principal, []ObservationV1{changed})
			if !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("dataset error=%v", err)
			}
		})
	}
}

func TestEvaluationMetricsRejectInconsistentCounts(t *testing.T) {
	_, candidate, input, result := testContractTrain(t)
	tests := []struct {
		name string
		edit func(*EvaluationMetricsV1)
	}{
		{"sample count", func(value *EvaluationMetricsV1) { value.SampleCount++ }},
		{"replay total", func(value *EvaluationMetricsV1) { value.FailedReplays++ }},
		{"quality total", func(value *EvaluationMetricsV1) { value.QualityWins++ }},
		{"regression", func(value *EvaluationMetricsV1) { value.RegressionCount = value.SampleCount + 1 }},
		{"baseline failures", func(value *EvaluationMetricsV1) { value.BaselineFailures = value.SampleCount + 1 }},
		{"candidate failures", func(value *EvaluationMetricsV1) { value.CandidateFailures = value.SampleCount + 1 }},
		{"baseline cost", func(value *EvaluationMetricsV1) { value.BaselineCostMicroUSD = -1 }},
		{"quality delta", func(value *EvaluationMetricsV1) { value.QualityDeltaBasisPoints = 10001 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := result.Metrics
			test.edit(&changed)
			if _, err := NewOfflineEvalResultV1(input, candidate, changed, result.GraderResults); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("result error=%v", err)
			}
		})
	}
}

func TestCodecRejectsInvalidValuesAtEveryBoundary(t *testing.T) {
	dataset, candidate, input, result := testContractTrain(t)

	badCandidate := cloneCandidate(candidate)
	badCandidate.CandidateDigest = strings.Repeat("0", 64)
	if _, err := EncodeCandidateV1(badCandidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("EncodeCandidateV1 error=%v", err)
	}
	badCandidatePayload, _ := json.Marshal(badCandidate)
	if _, err := DecodeCandidateV1(badCandidatePayload); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeCandidateV1 self-consistent JSON error=%v", err)
	}
	badDataset := cloneDataset(dataset)
	badDataset.DatasetDigest = strings.Repeat("0", 64)
	if _, err := EncodeEvaluationDatasetV1(badDataset); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("EncodeEvaluationDatasetV1 error=%v", err)
	}
	badDatasetPayload, _ := json.Marshal(badDataset)
	if _, err := DecodeEvaluationDatasetV1(badDatasetPayload); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeEvaluationDatasetV1 self-consistent JSON error=%v", err)
	}
	badInput := input
	badInput.InputDigest = strings.Repeat("0", 64)
	if _, err := EncodeOfflineEvalInputV1(badInput, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("EncodeOfflineEvalInputV1 error=%v", err)
	}
	badInputPayload, _ := json.Marshal(badInput)
	if _, err := DecodeOfflineEvalInputV1(badInputPayload, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeOfflineEvalInputV1 self-consistent JSON error=%v", err)
	}
	badResult := result
	badResult.ResultDigest = strings.Repeat("0", 64)
	if _, err := EncodeOfflineEvalResultV1(badResult, input, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("EncodeOfflineEvalResultV1 error=%v", err)
	}
	badResultPayload, _ := json.Marshal(badResult)
	if _, err := DecodeOfflineEvalResultV1(badResultPayload, input, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeOfflineEvalResultV1 self-consistent JSON error=%v", err)
	}

	for name, decode := range map[string]func([]byte) error{
		"candidate": func(payload []byte) error { _, err := DecodeCandidateV1(payload); return err },
		"dataset":   func(payload []byte) error { _, err := DecodeEvaluationDatasetV1(payload); return err },
		"input":     func(payload []byte) error { _, err := DecodeOfflineEvalInputV1(payload, candidate); return err },
		"result":    func(payload []byte) error { _, err := DecodeOfflineEvalResultV1(payload, input, candidate); return err },
	} {
		t.Run(name+" empty", func(t *testing.T) {
			if err := decode(nil); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("decode error=%v", err)
			}
		})
		t.Run(name+" malformed", func(t *testing.T) {
			if err := decode([]byte(`{"credential":"secret"}`)); !errors.Is(err, ErrInvalidContract) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
	if _, err := encode(func() {}); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("encode unsupported error=%v", err)
	}
}

func TestClosedEnumsAndNestedReferencesFailClosed(t *testing.T) {
	dataset, candidate, input, result := testContractTrain(t)

	for _, kind := range []PolicyModuleKindV1{
		PolicyModulePromptPolicy, PolicyModuleModelRoute, PolicyModuleCapabilityManifest,
	} {
		if !kind.valid() {
			t.Fatalf("known module kind rejected: %q", kind)
		}
	}
	if PolicyModuleKindV1("remote").valid() {
		t.Fatal("remote module kind accepted")
	}
	for _, failure := range []FailureTypeV1{
		FailureNone, FailureProvider, FailureTimeout, FailureInvalidOutput,
		FailurePolicyDenied, FailureBudgetExceeded, FailureCapability, FailureInternal,
	} {
		if !failure.valid() {
			t.Fatalf("known failure rejected: %q", failure)
		}
	}
	for _, grader := range []GraderKindV1{GraderCode, GraderModel, GraderHuman} {
		if !grader.valid() {
			t.Fatalf("known grader rejected: %q", grader)
		}
	}
	if GraderKindV1("remote_authority").valid() {
		t.Fatal("remote grader authority accepted")
	}

	badCandidate := cloneCandidate(candidate)
	badCandidate.Baseline.Modules[0].Kind = "remote"
	if err := badCandidate.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("bad nested module error=%v", err)
	}
	badCandidate = cloneCandidate(candidate)
	badCandidate.Target.Kind = "workspace_wide"
	if err := badCandidate.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("bad target error=%v", err)
	}
	badCandidate = cloneCandidate(candidate)
	slices.Reverse(badCandidate.Baseline.Modules)
	if err := badCandidate.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("noncanonical baseline error=%v", err)
	}
	badCandidate = cloneCandidate(candidate)
	slices.Reverse(badCandidate.Proposed.Modules)
	if err := badCandidate.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("noncanonical proposed error=%v", err)
	}

	badDataset := cloneDataset(dataset)
	slices.Reverse(badDataset.Observations[0].Runtime.Modules)
	badDataset.DatasetDigest, _ = digestJSON(badDataset.withoutDigest())
	if err := badDataset.Validate(); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("noncanonical observation runtime error=%v", err)
	}

	badInput := input
	badInput.Graders = slices.Clone(input.Graders)
	badInput.Graders[0].Kind = "remote_authority"
	if err := badInput.ValidateFor(candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("bad grader error=%v", err)
	}

	badResult := result
	badResult.GraderResults = slices.Clone(result.GraderResults)
	badResult.GraderResults[0].EvidenceDigest = "invalid"
	if err := badResult.ValidateFor(input, candidate); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("bad grader result error=%v", err)
	}
}

func TestFeedbackSignalsAreCategoricalOnly(t *testing.T) {
	valid := []FeedbackSignalV1{
		{},
		{Action: types.FeedbackActionInterested},
		{Action: types.FeedbackActionNotInterested},
		{Action: types.FeedbackActionDeepDive},
		{Action: types.FeedbackActionQuestion},
		{Action: types.FeedbackActionMisjudged, Reason: types.FeedbackReasonFactWrong},
	}
	for _, feedback := range valid {
		if err := feedback.validate(); err != nil {
			t.Fatalf("valid feedback %+v error=%v", feedback, err)
		}
	}
	invalidValues := []FeedbackSignalV1{
		{Reason: types.FeedbackReasonOther},
		{Action: types.FeedbackActionInterested, Reason: types.FeedbackReasonOther},
		{Action: types.FeedbackActionMisjudged},
		{Action: "raw feedback body"},
	}
	for _, feedback := range invalidValues {
		if err := feedback.validate(); !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("invalid feedback %+v error=%v", feedback, err)
		}
	}
}

func testContractTrain(
	t *testing.T,
) (EvaluationDatasetV1, CandidateV1, OfflineEvalInputV1, OfflineEvalResultV1) {
	t.Helper()
	principal := testPrincipal()
	baseline := testRuntime("1", "2", "3", "4")
	proposed := testRuntime("5", "6", "3", "4")
	observations := []ObservationV1{
		testObservation(principal, baseline, "task-b", 2, "trace-b", "7"),
		testObservation(principal, baseline, "task-a", 1, "trace-a", "8"),
	}
	observations[0].FailureType = FailureTimeout
	observations[0].Feedback = FeedbackSignalV1{
		Action: types.FeedbackActionMisjudged,
		Reason: types.FeedbackReasonPoorSource,
	}
	dataset, err := NewEvaluationDatasetV1(principal, observations)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := NewCandidateV1(CandidateInputV1{
		Principal: principal,
		Target:    CandidateTargetV1{Kind: CandidateTargetTaskRuntime, ID: "task-policy"},
		Baseline:  baseline, Proposed: proposed,
		Generator: GeneratorRefV1{
			ID: "policy-optimizer", Version: "v1", ArtifactDigest: strings.Repeat("9", 64),
		},
		SourceDatasetDigest: dataset.DatasetDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	graders := []GraderRefV1{
		{Kind: GraderModel, ID: "quality-rubric", Version: "v1", Digest: strings.Repeat("b", 64)},
		{Kind: GraderCode, ID: "safety-regression", Version: "v2", Digest: strings.Repeat("a", 64)},
	}
	input, err := NewOfflineEvalInputV1(candidate, dataset, graders)
	if err != nil {
		t.Fatal(err)
	}
	metrics := EvaluationMetricsV1{
		SampleCount: 2, SuccessfulReplays: 2, QualityWins: 1, QualityTies: 1,
		BaselineFailures: 1, CandidateFailures: 0,
		BaselineCostMicroUSD: 9000, CandidateCostMicroUSD: 8000,
		QualityDeltaBasisPoints: 250,
	}
	graderResults := []GraderResultV1{
		{Grader: graders[0], Passed: true, ScoreBasisPoints: 9000, EvidenceDigest: strings.Repeat("d", 64)},
		{Grader: graders[1], Passed: true, ScoreBasisPoints: 10000, EvidenceDigest: strings.Repeat("c", 64)},
	}
	result, err := NewOfflineEvalResultV1(input, candidate, metrics, graderResults)
	if err != nil {
		t.Fatal(err)
	}
	return dataset, candidate, input, result
}

func testPrincipal() PrincipalV1 {
	return PrincipalV1{
		TenantID: 41, UserID: 73, Role: types.MembershipRoleMember,
		ActorType: types.ActorTypeUser, MembershipAuthorizationGeneration: 9,
	}
}

func testRuntime(manifest, prompt, model, capability string) RuntimeCompositionV1 {
	return RuntimeCompositionV1{
		PolicyManifestDigest: strings.Repeat(manifest, 64),
		Modules: []PolicyModuleRefV1{
			{Kind: PolicyModuleCapabilityManifest, ID: "capabilities", Version: "v3", Digest: strings.Repeat(capability, 64)},
			{Kind: PolicyModuleModelRoute, ID: "model-route", Version: "v2", Digest: strings.Repeat(model, 64)},
			{Kind: PolicyModulePromptPolicy, ID: "prompt-bundle", Version: "v4", Digest: strings.Repeat(prompt, 64)},
		},
	}
}

func testObservation(
	principal PrincipalV1,
	runtime RuntimeCompositionV1,
	taskID string,
	runSnapshotID int64,
	traceID, fixture string,
) ObservationV1 {
	modelModule, _ := moduleByKind(runtime, PolicyModuleModelRoute)
	return ObservationV1{
		Principal: principal, TaskID: taskID, RunSnapshotID: runSnapshotID,
		RunSnapshotReferenceDigest: strings.Repeat("e", 64), TraceID: traceID,
		Runtime:             runtime,
		Model:               ModelRefV1{Provider: "deepseek", Model: "deepseek-v4-flash", RouteDigest: modelModule.Digest},
		ReplayFixtureDigest: strings.Repeat(fixture, 64), CostMicroUSD: 4500,
	}
}

func cloneCandidate(candidate CandidateV1) CandidateV1 {
	candidate.Baseline.Modules = slices.Clone(candidate.Baseline.Modules)
	candidate.Proposed.Modules = slices.Clone(candidate.Proposed.Modules)
	return candidate
}

func cloneDataset(dataset EvaluationDatasetV1) EvaluationDatasetV1 {
	dataset.Observations = slices.Clone(dataset.Observations)
	for index := range dataset.Observations {
		dataset.Observations[index].Runtime.Modules =
			slices.Clone(dataset.Observations[index].Runtime.Modules)
	}
	return dataset
}
