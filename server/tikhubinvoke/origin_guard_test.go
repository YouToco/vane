package tikhubinvoke

import "testing"

func TestInvariant_DefaultOriginIsTikHub(t *testing.T) {
	const want = "https://api.tikhub.io"
	if defaultBaseURL != want {
		t.Fatalf("default TikHub origin = %q, want %q", defaultBaseURL, want)
	}
}
