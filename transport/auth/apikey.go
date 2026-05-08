package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
)

const (
	// KeyLength is the total number of random bytes in the secret key
	KeyLength = 32

	base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// GeneratedKey contains the components of a newly generated API key
type GeneratedKey struct {
	KeyHash string // SHA256 hash of the full key (for storage)
	Prefix  string // qtr-[tag]- plus first 8 entropy chars (for display)
	Key     string // Full API key to return to user (shown once)
}

// base62Encode encodes random bytes as a Base62 string using the big-integer approach.
func base62Encode(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(62)
	mod := new(big.Int)
	var digits []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		digits = append(digits, base62Alphabet[mod.Int64()])
	}
	// reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}

// GenerateAPIKey generates a new API key in qtr-[tag]-[entropy] format.
// tag must already be resolved (non-empty); caller is responsible for cascade.
// If tag is empty, it defaults to "live" as a safety net.
func GenerateAPIKey(tag string) (GeneratedKey, error) {
	if tag == "" {
		tag = "live"
	}

	randomBytes := make([]byte, KeyLength)
	if _, err := rand.Read(randomBytes); err != nil {
		return GeneratedKey{}, fmt.Errorf("generating random bytes: %w", err)
	}

	entropy := base62Encode(randomBytes)
	fullKey := fmt.Sprintf("qtr-%s-%s", tag, entropy)
	prefix := fmt.Sprintf("qtr-%s-%s", tag, entropy[:8])

	return GeneratedKey{
		Prefix:  prefix,
		KeyHash: HashAPIKey(fullKey),
		Key:     fullKey,
	}, nil
}

// HashAPIKey creates a SHA256 hash of an API key for storage
func HashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

// ValidateAPIKeyFormat checks if an API key has the correct qtr-[tag]-[entropy] format
func ValidateAPIKeyFormat(key string) error {
	if key == "" {
		return fmt.Errorf("api key cannot be empty")
	}

	parts := strings.SplitN(key, "-", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid api key format; expected qtr-[tag]-[entropy]")
	}

	if parts[0] != "qtr" {
		return fmt.Errorf("invalid api key format; must begin with 'qtr'")
	}

	tag := parts[1]
	if len(tag) == 0 {
		return fmt.Errorf("invalid api key format; tag cannot be empty")
	}
	if len(tag) > 16 {
		return fmt.Errorf("invalid api key format; tag must be at most 16 characters")
	}
	for _, c := range tag {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
			return fmt.Errorf("invalid api key format; tag must be lowercase alphanumeric only")
		}
	}

	entropy := parts[2]
	if len(entropy) < 43 {
		return fmt.Errorf("invalid api key format; entropy must be at least 43 characters")
	}

	return nil
}
