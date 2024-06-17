package bench_test

import (
	"testing"
)

func BenchmarkCache(b *testing.B) {

	testCases := []struct {
		Name string
	}{
		{
			Name: "BuntDB",
		},
		{
			Name: "PostgresSQL",
		},
	}

	for _, testCase := range testCases {
		b.Run(testCase.Name, func(b *testing.B) {

		})
	}
}
