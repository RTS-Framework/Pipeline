package pipeline

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchBuildTag(t *testing.T) {
	tags := []string{
		"",
		"windows && amd64",
		"linux && amd64",
		"windows || linux",
		"!linux",
	}

	for _, tag := range tags {
		ok, err := MatchBuildTag(tag)
		require.NoError(t, err)

		fmt.Printf("%-16s -> %t\n", tag, ok)
	}
}

func TestCheckNodeBuildTag(t *testing.T) {
	t.Run("common", func(t *testing.T) {
		node := testNewTestNode("test")

		err := CheckNodeBuildTag(node)
		require.NoError(t, err)
	})

	t.Run("invalid", func(t *testing.T) {
		node := testNewTestNode("test")
		node.buildTag = "+invalid"

		err := CheckNodeBuildTag(node)
		errStr := "invalid syntax at +"
		require.EqualError(t, err, errStr)
	})

	t.Run("not matched", func(t *testing.T) {
		node := testNewTestNode("test")
		node.buildTag = "tag && cgo"

		err := CheckNodeBuildTag(node)
		errStr := "build tag: \"tag && cgo\" is not match current environment"
		require.EqualError(t, err, errStr)
	})
}
