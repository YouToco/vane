package periodicbrief

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/server/types"
)

// This source mutation gate makes the compatibility marker and partial
// workflow outcome structural. Replacing deliveryErr with `_`, removing the
// version marker, or returning success on error must make the test red.
func TestPeriodicDeliveryFailureIsWorkflowVisibleMutationGate(t *testing.T) {
	source, err := os.ReadFile("workflow.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"deliveryErr := workflow.ExecuteActivity(",
		`"DeliverPeriodicBriefV1"`,
		`"periodic-delivery-partial/v1"`,
		"deliveryErr != nil && deliveryGate == 1",
		"return report, deliveryErr",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("workflow partial-delivery gate omitted %q", required)
		}
	}
	if strings.Contains(text, `_ = workflow.ExecuteActivity(
		ctx, "DeliverPeriodicBriefV1"`) {
		t.Fatal("workflow silently discards delivery failure")
	}
}

func TestPeriodicWorkflowFailsExplicitlyWhenDurableDeliveryIsPartial(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	report := periodicDeliveryReportFixture(t)
	environment.RegisterActivityWithOptions(
		func(context.Context, SynthesizeInputV1) (types.PeriodicBriefReportV1, error) {
			return report, nil
		}, activity.RegisterOptions{Name: "SynthesizePeriodicBriefV1"})
	sentinel := errors.New("durable Telegram effect has no adapter")
	environment.RegisterActivityWithOptions(
		func(context.Context, DeliverInputV1) error { return sentinel },
		activity.RegisterOptions{Name: "DeliverPeriodicBriefV1"})
	environment.SetTestTimeout(5 * time.Second)
	environment.ExecuteWorkflow(WorkflowV1, WorkflowInputV1{
		IntentID: 9, TenantID: report.TenantID, UserID: report.UserID,
	})
	if err := environment.GetWorkflowError(); err == nil ||
		!strings.Contains(err.Error(), sentinel.Error()) {
		t.Fatalf("partial delivery completed successfully: %v", err)
	}
}
