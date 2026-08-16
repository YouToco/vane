package sandbox

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var releaseVersionPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

func validateVersionOutput(binary, expected, output string) error {
	if !releaseVersionPattern.MatchString(expected) {
		return errors.New("expected version is invalid")
	}
	actual := strings.TrimSpace(output)
	if actual != binary+" "+expected || strings.Contains(strings.ToLower(actual), "debug") {
		return fmt.Errorf("version output %q does not match approved release %q", actual, expected)
	}
	return nil
}
