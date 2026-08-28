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
}

func TestCheckArtifactData(t *testing.T) {
	t.Run("expected", func(t *testing.T) {
		err := CheckArtifactData(testTypeA.Name, int8(123))
		require.NoError(t, err)
	})

	t.Run("unexpected", func(t *testing.T) {
		err := CheckArtifactData(testTypeA.Name, int16(456))
		require.EqualError(t, err, "unexpected artifact data type")
	})

	t.Run("unknown type", func(t *testing.T) {
		err := CheckArtifactData("unknown", 789)
		require.EqualError(t, err, "artifact type unknown does not exist")
	})
}
