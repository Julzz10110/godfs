package security

import (
	"os"
	"strings"
)

// envFileOrValue returns trimmed env value; values prefixed with @ are read from that file path.
func envFileOrValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if !strings.HasPrefix(raw, "@") {
		return raw, nil
	}
	b, err := os.ReadFile(strings.TrimPrefix(raw, "@"))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
