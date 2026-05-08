package auth_test

import (
	"regexp"
	"testing"

	"github.com/kapetan-io/querator/transport/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateAPIKey(t *testing.T) {
	keyPattern := regexp.MustCompile(`^qtr-[a-z0-9]{1,16}-[A-Za-z0-9]{43,}$`)

	t.Run("KeyMatchesExpectedFormat", func(t *testing.T) {
		result, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)
		assert.Regexp(t, keyPattern, result.Key)
	})

	t.Run("PrefixStartsWithTag", func(t *testing.T) {
		result, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)
		assert.True(t, len(result.Prefix) > 0)
		assert.Equal(t, "qtr-live-", result.Prefix[:9])
		// Prefix contains 8 entropy characters after qtr-live-
		assert.Len(t, result.Prefix, len("qtr-live-")+8)
	})

	t.Run("KeyHashMatchesHashAPIKey", func(t *testing.T) {
		result, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)
		assert.NotEmpty(t, result.KeyHash)
		assert.Equal(t, auth.HashAPIKey(result.Key), result.KeyHash)
	})

	t.Run("EmptyTagDefaultsToLive", func(t *testing.T) {
		result, err := auth.GenerateAPIKey("")
		require.NoError(t, err)
		assert.Regexp(t, regexp.MustCompile(`^qtr-live-`), result.Key)
	})

	t.Run("TwoCallsProduceDifferentKeys", func(t *testing.T) {
		result1, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)
		result2, err := auth.GenerateAPIKey("live")
		require.NoError(t, err)
		assert.NotEqual(t, result1.Key, result2.Key)
	})

	t.Run("CustomTagEmbeddedInKey", func(t *testing.T) {
		result, err := auth.GenerateAPIKey("staging")
		require.NoError(t, err)
		assert.Regexp(t, regexp.MustCompile(`^qtr-staging-`), result.Key)
		assert.Regexp(t, keyPattern, result.Key)
	})
}

func TestValidateAPIKeyFormat(t *testing.T) {
	validKey, err := auth.GenerateAPIKey("live")
	require.NoError(t, err)

	for _, test := range []struct {
		name    string
		key     string
		wantErr string
	}{
		{
			name: "ValidGeneratedKey",
			key:  validKey.Key,
		},
		{
			name:    "EmptyString",
			key:     "",
			wantErr: "api key cannot be empty",
		},
		{
			name:    "OldUnderscoreFormat",
			key:     "qtr_invalid_invalidkey",
			wantErr: "expected qtr-[tag]-[entropy]",
		},
		{
			name:    "MissingQtrPrefix",
			key:     "bad-live-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: "must begin with 'qtr'",
		},
		{
			name:    "TagWithUppercaseLetters",
			key:     "qtr-LIVE-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: "tag must be lowercase alphanumeric only",
		},
		{
			name:    "TagLongerThan16Chars",
			key:     "qtr-thistagiswaytoolong-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: "tag must be at most 16 characters",
		},
		{
			name:    "EntropyTooShort",
			key:     "qtr-live-tooshort",
			wantErr: "entropy must be at least 43 characters",
		},
		{
			name:    "OnlyTwoSegments",
			key:     "qtr-live",
			wantErr: "expected qtr-[tag]-[entropy]",
		},
		{
			name:    "EmptyTag",
			key:     "qtr--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: "tag cannot be empty",
		},
		{
			name:    "TagWithSymbols",
			key:     "qtr-live!-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr: "tag must be lowercase alphanumeric only",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := auth.ValidateAPIKeyFormat(test.key)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}
