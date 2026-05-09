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
	// DefaultKeyTag is the fallback tag used when no tag is supplied at any cascade level
	DefaultKeyTag = "live"

	base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// GeneratedKey contains the components of a newly generated API key
type GeneratedKey struct {
	KeyHash string // SHA256 hash of the full key (for storage)
	Prefix  string // qtr-[tag]- plus first 8 entropy chars (for display)
	Key     string // Full API key to return to user (shown once)
}

func base62Encode(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(62)
	mod := new(big.Int)
	digits := make([]byte, 0, 43)
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		digits = append(digits, base62Alphabet[mod.Int64()])
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	for len(digits) < 43 {
		digits = append([]byte{base62Alphabet[0]}, digits...)
	}
	return string(digits)
}

func GenerateAPIKey(tag string) (GeneratedKey, error) {
	if tag == "" {
		tag = DefaultKeyTag
	}

	if err := validateKeyTag(tag); err != nil {
		return GeneratedKey{}, err
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

// validateKeyTag checks that a tag is lowercase alphanumeric and at most 16 characters
func validateKeyTag(tag string) error {
	if len(tag) == 0 {
		return fmt.Errorf("invalid api key format; tag cannot be empty")
	}
	if len(tag) > 16 {
		return fmt.Errorf("invalid api key format; tag must be at most 16 characters")
	}
	for _, c := range tag {
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return fmt.Errorf("invalid api key format; tag must be lowercase alphanumeric only")
		}
	}
	return nil
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

	if err := validateKeyTag(parts[1]); err != nil {
		return err
	}

	entropy := parts[2]
	if len(entropy) < 43 {
		return fmt.Errorf("invalid api key format; entropy must be at least 43 characters")
	}
	for _, c := range entropy {
		if !strings.ContainsRune(base62Alphabet, c) {
			return fmt.Errorf("invalid api key format; entropy must contain only base62 characters (0-9, A-Z, a-z)")
		}
	}

	return nil
}
