package security

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	jwt "github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	PrincipalCluster = "cluster"
)

// Auth holds API key map, cluster key, and optional JWT HMAC secret.
type Auth struct {
	Enabled       bool
	APIKeyToUser  map[string]string // secret -> principal
	ClusterKey    string
	JWTHS256Secret []byte
}

// LoadAuthFromEnv configures auth when any of GODFS_CLUSTER_KEY, GODFS_API_KEYS, GODFS_JWT_HMAC_SECRET is set.
func LoadAuthFromEnv() (*Auth, error) {
	cluster := strings.TrimSpace(os.Getenv("GODFS_CLUSTER_KEY"))
	apiKeysRaw := strings.TrimSpace(os.Getenv("GODFS_API_KEYS"))
	jwtSecret := strings.TrimSpace(os.Getenv("GODFS_JWT_HMAC_SECRET"))

	a := &Auth{
		APIKeyToUser: map[string]string{},
		ClusterKey:   cluster,
	}
	if apiKeysRaw != "" {
		// "alice:key1,bob:key2" or path @file
		if strings.HasPrefix(apiKeysRaw, "@") {
			b, err := os.ReadFile(strings.TrimPrefix(apiKeysRaw, "@"))
			if err != nil {
				return nil, err
			}
			apiKeysRaw = string(b)
		}
		for _, part := range strings.Split(apiKeysRaw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			i := strings.IndexByte(part, ':')
			if i <= 0 || i == len(part)-1 {
				continue
			}
			user := strings.TrimSpace(part[:i])
			secret := strings.TrimSpace(part[i+1:])
			if user != "" && secret != "" {
				a.APIKeyToUser[secret] = user
			}
		}
	}
	if jwtSecret != "" {
		a.JWTHS256Secret = []byte(jwtSecret)
	}
	a.Enabled = cluster != "" || len(a.APIKeyToUser) > 0 || len(a.JWTHS256Secret) > 0
	return a, nil
}

// PrincipalFromContext extracts Bearer token / JWT and returns principal name.
func (a *Auth) PrincipalFromContext(ctx context.Context) (string, error) {
	if !a.Enabled {
		return "", nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	var tok string
	for _, k := range []string{"authorization", "Authorization"} {
		if v := md.Get(k); len(v) > 0 {
			tok = strings.TrimSpace(v[0])
			break
		}
	}
	if tok == "" {
		return "", status.Error(codes.Unauthenticated, "missing authorization")
	}
	if strings.HasPrefix(strings.ToLower(tok), "bearer ") {
		tok = strings.TrimSpace(tok[7:])
	}
	if tok == "" {
		return "", status.Error(codes.Unauthenticated, "empty token")
	}
	if a.ClusterKey != "" && tok == a.ClusterKey {
		return PrincipalCluster, nil
	}
	if p, ok := a.APIKeyToUser[tok]; ok {
		return p, nil
	}
	if len(a.JWTHS256Secret) > 0 && strings.Count(tok, ".") == 2 {
		p, err := parseJWT(tok, a.JWTHS256Secret)
		if err != nil {
			return "", status.Errorf(codes.Unauthenticated, "jwt: %v", err)
		}
		if p != "" {
			return p, nil
		}
	}
	return "", status.Error(codes.Unauthenticated, "invalid credentials")
}

func parseJWT(token string, secret []byte) (string, error) {
	t, err := jwt.ParseWithClaims(token, &jwt.RegisteredClaims{}, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, jwt.ErrSignatureInvalid
		}
		return secret, nil
	})
	if err != nil {
		return "", err
	}
	c, ok := t.Claims.(*jwt.RegisteredClaims)
	if !ok || !t.Valid || c.Subject == "" {
		return "", jwt.ErrTokenInvalidClaims
	}
	return c.Subject, nil
}

// RBACRulesJSON loads optional GODFS_RBAC_JSON or file @path.
func RBACRulesJSON() string {
	raw := strings.TrimSpace(os.Getenv("GODFS_RBAC_JSON"))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "@") {
		b, err := os.ReadFile(strings.TrimPrefix(raw, "@"))
		if err != nil {
			return ""
		}
		return string(b)
	}
	return raw
}

// ParseRBACRules parses JSON array: [{"principal":"u","path_prefix":"/","permissions":["read","write"]}]
func ParseRBACRules(raw string) ([]RBACRule, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var rules []RBACRule
	if err := json.Unmarshal([]byte(raw), &rules); err != nil {
		return nil, err
	}
	return rules, nil
}
