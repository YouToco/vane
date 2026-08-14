package releaseinfo

import "testing"

func TestRevisionRequiresExactCleanLinkerValues(t *testing.T) {
	originalRevision, originalClean := revision, clean
	t.Cleanup(func() { revision, clean = originalRevision, originalClean })

	revision, clean = "ac36c9d967c0815ef1a0df3c7ac722823683b646", "true"
	if value, ok := Revision(); !ok || value != revision {
		t.Fatalf("value=%q ok=%v", value, ok)
	}
	for _, mutation := range []struct {
		revision string
		clean    string
	}{
		{revision: revision, clean: "false"},
		{revision: revision[:39], clean: "true"},
		{revision: "Ac36c9d967c0815ef1a0df3c7ac722823683b646", clean: "true"},
	} {
		revision, clean = mutation.revision, mutation.clean
		if value, ok := Revision(); ok || value != "" {
			t.Fatalf("invalid linker values accepted: value=%q ok=%v", value, ok)
		}
	}
}
