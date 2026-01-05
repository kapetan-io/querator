package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	// PrefixLength is the number of characters in the key prefix (for identification)
	PrefixLength = 8
	// KeyLength is the total number of random bytes in the secret key
	KeyLength = 32
	// DefaultEnvTag is the default environment tag for API keys
	DefaultEnvTag = "qtr"
)

// GeneratedKey contains the components of a newly generated API key
type GeneratedKey struct {
	KeyHash string // SHA256 hash of the full key (for storage)
	Prefix  string // First 8 chars of the key (for identification)
	Key     string // Full API key to return to user (shown once)
}

// GenerateAPIKey generates a new API key with the format: env_prefix_random
// Returns the full key, its prefix, and its hash.
func GenerateAPIKey(envTag string) (GeneratedKey, error) {
	if envTag == "" {
		envTag = DefaultEnvTag
	}

	randomBytes := make([]byte, KeyLength)
	if _, err := rand.Read(randomBytes); err != nil {
		return GeneratedKey{}, fmt.Errorf("generating random bytes: %w", err)
	}

	secret := base64.RawURLEncoding.EncodeToString(randomBytes)
	prefix := secret[:PrefixLength]
	fullKey := fmt.Sprintf("%s_%s_%s", envTag, prefix, secret)

	hash := HashAPIKey(fullKey)

	return GeneratedKey{
		Prefix:  prefix,
		KeyHash: hash,
		Key:     fullKey,
	}, nil
}

// HashAPIKey creates a SHA256 hash of an API key for storage
func HashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

// ParseAPIKey extracts the environment tag and prefix from an API key
func ParseAPIKey(key string) (envTag, prefix string, err error) {
	parts := strings.SplitN(key, "_", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid api key format")
	}
	return parts[0], parts[1], nil
}

// ValidateAPIKeyFormat checks if an API key has the correct format
func ValidateAPIKeyFormat(key string) error {
	if key == "" {
		return fmt.Errorf("api key cannot be empty")
	}

	parts := strings.SplitN(key, "_", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid api key format; expected env_prefix_secret")
	}

	if len(parts[0]) == 0 {
		return fmt.Errorf("invalid api key format; env tag cannot be empty")
	}

	if len(parts[1]) != PrefixLength {
		return fmt.Errorf("invalid api key format; prefix must be %d characters", PrefixLength)
	}

	if len(parts[2]) == 0 {
		return fmt.Errorf("invalid api key format; secret cannot be empty")
	}

	return nil
}
