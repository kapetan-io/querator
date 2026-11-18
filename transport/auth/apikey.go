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
	KeyLength       = 32
	DefaultKeyTag   = "live"
	MaxKeyTagLength = 16

	entropyLength = 43

	base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var base62Lookup [128]bool

func init() {
	for _, c := range base62Alphabet {
		base62Lookup[c] = true
	}
}

type GeneratedKey struct {
	KeyHash string
	Prefix  string
	Key     string
}

func encodeEntropy(b []byte) string {
	n := new(big.Int).SetBytes(b)
	base := big.NewInt(62)
	mod := new(big.Int)
	digits := make([]byte, entropyLength)
	for i := len(digits) - 1; i >= 0; i-- {
		n.DivMod(n, base, mod)
		digits[i] = base62Alphabet[mod.Int64()]
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

	entropy := encodeEntropy(randomBytes)
	fullKey := fmt.Sprintf("qtr-%s-%s", tag, entropy)
	prefix := fmt.Sprintf("qtr-%s-%s", tag, entropy[:8])

	return GeneratedKey{
		Prefix:  prefix,
		KeyHash: HashAPIKey(fullKey),
		Key:     fullKey,
	}, nil
}

func HashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}

func validateKeyTag(tag string) error {
	if len(tag) == 0 {
		return fmt.Errorf("invalid api key format; tag cannot be empty")
	}
	if len(tag) > MaxKeyTagLength {
		return fmt.Errorf("invalid api key format; tag must be at most %d characters", MaxKeyTagLength)
	}
	for _, c := range tag {
		if !isLowerAlphanumeric(c) {
			return fmt.Errorf("invalid api key format; tag must be lowercase alphanumeric only")
		}
	}
	return nil
}

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
	if len(entropy) < entropyLength {
		return fmt.Errorf("invalid api key format; entropy must be at least %d characters", entropyLength)
	}
	for _, c := range entropy {
		if c >= 128 || !base62Lookup[c] {
			return fmt.Errorf("invalid api key format; entropy must contain only base62 characters (0-9, A-Z, a-z)")
		}
	}

	return nil
}

func isLowerAlphanumeric(c rune) bool {
	if c >= '0' && c <= '9' {
		return true
	}
	return c >= 'a' && c <= 'z'
}
