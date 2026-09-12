package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testTypeA = ArtifactType{
		Name:        "TestTypeA",
		Description: "type A for test",
	}

	testTypeB = ArtifactType{
		Name:        "TestTypeB",
		Description: "type B for test",
	}

	testTypeC = ArtifactType{
		Name:        "TestTypeC",
		Description: "type C for test",
	}
)

func init() {
	err := RegisterArtifactType(testTypeA, int8(0))
	if err != nil {
		panic(err)
	}
	err = RegisterArtifactType(testTypeB, int16(0))
	if err != nil {
		panic(err)
	}
	err = RegisterArtifactType(testTypeC, int32(0))
	if err != nil {
		panic(err)
	}
}

func TestRegisterArtifactType(t *testing.T) {
	t.Run("already exists", func(t *testing.T) {
		err := RegisterArtifactType(testTypeA, int8(0))
		require.EqualError(t, err, "artifact type TestTypeA already exists")
	})

	t.Run("empty type name", func(t *testing.T) {
		empty := ArtifactType{}

		err := RegisterArtifactType(empty, int8(0))
		require.EqualError(t, err, "artifact type name can not be empty")
	})

	t.Run("nil reference", func(t *testing.T) {
		err := RegisterArtifactType(testTypeA, nil)
		require.EqualError(t, err, "artifact reference can not be nil")
	})
}

func TestLookupArtifactType(t *testing.T) {
	t.Run("exist", func(t *testing.T) {
		ok := LookupArtifactType(testTypeA)
		require.True(t, ok)
	})

	t.Run("not exist", func(t *testing.T) {
		unknown := ArtifactType{
			Name: "unknown",
		}

		ok := LookupArtifactType(unknown)
		require.False(t, ok)
	})

	t.Run("case-insensitive", func(t *testing.T) {
		lower := ArtifactType{
			Name: "testtypea",
		}

		ok := LookupArtifactType(lower)
		require.True(t, ok)
	})
}

func TestCheckArtifactData(t *testing.T) {
	t.Run("expected", func(t *testing.T) {
		err := CheckArtifactData(testTypeA, int8(123))
		require.NoError(t, err)
	})

	t.Run("unexpected", func(t *testing.T) {
		err := CheckArtifactData(testTypeA, int16(456))
		require.EqualError(t, err, "unexpected artifact data type")
	})

	t.Run("unknown type", func(t *testing.T) {
		unknown := ArtifactType{
			Name: "unknown",
		}

		err := CheckArtifactData(unknown, 789)
		require.EqualError(t, err, "artifact type unknown does not exist")
	})
}
