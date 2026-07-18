package pipeline

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchBuildConstraint(t *testing.T) {
	constraints := []string{
		"windows && amd64",
		"linux && amd64",
		"windows || linux",
		"!linux",
	}

	for _, c := range constraints {
		ok, err := matchBuildConstraint(c)
		require.NoError(t, err)
		fmt.Printf("%-16s -> %t\n", c, ok)
	}
}
