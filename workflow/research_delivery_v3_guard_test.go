package workflow

import (
	"os"
	"strings"
	"testing"
)

func TestDeliverResearchBriefV3HasNoRuntimeFallback(t *testing.T) {
	source, err := os.ReadFile("research_activities_v3.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(source)
	if !strings.Contains(body, "a.researchDeliveryV3 == nil") {
		t.Fatal("delivery Activity no longer fails closed without its reviewed adapter")
	}
	for _, forbidden := range []string{
		"delivery = a.researchRuntimeV3",
		"delivery := a.researchRuntimeV3",
		"a.researchRuntimeV3.Deliver(",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("delivery Activity exposes runtime fallback %q", forbidden)
		}
	}
}
