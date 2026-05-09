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
		Name    string
		Key     string
		WantErr string
	}{
		{
			Name: "ValidGeneratedKey",
			Key:  validKey.Key,
		},
		{
			Name:    "EmptyString",
			Key:     "",
			WantErr: "api key cannot be empty",
		},
		{
			Name:    "OldUnderscoreFormat",
			Key:     "qtr_invalid_invalidkey",
			WantErr: "expected qtr-[tag]-[entropy]",
		},
		{
			Name:    "MissingQtrPrefix",
			Key:     "bad-live-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WantErr: "must begin with 'qtr'",
		},
		{
			Name:    "TagWithUppercaseLetters",
			Key:     "qtr-LIVE-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WantErr: "tag must be lowercase alphanumeric only",
		},
		{
			Name:    "TagLongerThan16Chars",
			Key:     "qtr-thistagiswaytoolong-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WantErr: "tag must be at most 16 characters",
		},
		{
			Name:    "EntropyTooShort",
			Key:     "qtr-live-tooshort",
			WantErr: "entropy must be at least 43 characters",
		},
		{
			Name:    "OnlyTwoSegments",
			Key:     "qtr-live",
			WantErr: "expected qtr-[tag]-[entropy]",
		},
		{
			Name:    "EmptyTag",
			Key:     "qtr--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WantErr: "tag cannot be empty",
		},
		{
			Name:    "TagWithSymbols",
			Key:     "qtr-live!-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WantErr: "tag must be lowercase alphanumeric only",
		},
		{
			Name:    "EntropyWithExclamationMarks",
			Key:     "qtr-live-!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
			WantErr: "entropy must contain only base62 characters",
		},
		{
			Name:    "EntropyWithSpaces",
			Key:     "qtr-live-" + "                                           ",
			WantErr: "entropy must contain only base62 characters",
		},
		{
			Name:    "EntropyWithEmbeddedDashes",
			Key:     "qtr-live-aaaaaaaaaaaaaaaaaaaaaaa-aaaaaaaaaaaaaaaaaaa",
			WantErr: "entropy must contain only base62 characters",
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			err := auth.ValidateAPIKeyFormat(test.Key)
			if test.WantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.WantErr)
			}
		})
	}
}

func TestGenerateAPIKeyTagValidation(t *testing.T) {
	for _, test := range []struct {
		Name    string
		Tag     string
		WantErr string
	}{
		{
			Name:    "UppercaseTag",
			Tag:     "PROD",
			WantErr: "tag must be lowercase alphanumeric only",
		},
		{
			Name:    "TagLongerThan16Chars",
			Tag:     "thistagiswaytoolong",
			WantErr: "tag must be at most 16 characters",
		},
		{
			Name:    "TagWithHyphen",
			Tag:     "my-tag",
			WantErr: "tag must be lowercase alphanumeric only",
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			_, err := auth.GenerateAPIKey(test.Tag)
			require.ErrorContains(t, err, test.WantErr)
		})
	}
}
