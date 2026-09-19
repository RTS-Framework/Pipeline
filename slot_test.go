package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSlotTypeMatched(t *testing.T) {
	t.Run("matched", func(t *testing.T) {
		accepted := []ArtifactType{testTypeA}

		ok := isSlotTypeMatched(accepted, testTypeA)
		require.True(t, ok)
	})

	t.Run("matched with different case", func(t *testing.T) {
		accepted := []ArtifactType{testTypeA}
		typ := ArtifactType{Name: "testtypea"}

		ok := isSlotTypeMatched(accepted, typ)
		require.True(t, ok)
	})

	t.Run("matched without description", func(t *testing.T) {
		accepted := []ArtifactType{{Name: testTypeA.Name}}
		typ := testTypeA

		ok := isSlotTypeMatched(accepted, typ)
		require.True(t, ok)
	})

	t.Run("matched among many", func(t *testing.T) {
		accepted := []ArtifactType{testTypeA, testTypeB, testTypeC}
		typ := testTypeB

		ok := isSlotTypeMatched(accepted, typ)
		require.True(t, ok)
	})

	t.Run("not matched", func(t *testing.T) {
		accepted := []ArtifactType{testTypeA}
		typ := testTypeB

		ok := isSlotTypeMatched(accepted, typ)
		require.False(t, ok)
	})

	t.Run("empty accepted", func(t *testing.T) {
		ok := isSlotTypeMatched(nil, testTypeA)
		require.False(t, ok)
	})
}

func TestCheckNodeSlots(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		node := testNewTestNode("empty")

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique inputs", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "a", Accepted: []ArtifactType{testTypeA}},
			{Name: "b", Accepted: []ArtifactType{testTypeB}},
			{Name: "c", Accepted: []ArtifactType{testTypeC}},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique outputs", func(t *testing.T) {
		node := testNewTestNode("test")
		node.outputs = []*OutputSlot{
			{Name: "out1", Type: testTypeA},
			{Name: "out2", Type: testTypeB},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique inputs and outputs", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "in1", Accepted: []ArtifactType{testTypeA}},
			{Name: "in2", Accepted: []ArtifactType{testTypeB}},
		}
		node.outputs = []*OutputSlot{
			{Name: "out1", Type: testTypeA},
			{Name: "out2", Type: testTypeB},
			{Name: "out3", Type: testTypeC},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("inputs only", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "a", Accepted: []ArtifactType{testTypeA}},
			{Name: "b", Accepted: []ArtifactType{testTypeB}},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("outputs only", func(t *testing.T) {
		node := testNewTestNode("test")
		node.outputs = []*OutputSlot{
			{Name: "a", Type: testTypeA},
			{Name: "b", Type: testTypeB},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("input with many accepted types", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "a", Accepted: []ArtifactType{testTypeA, testTypeB, testTypeC}},
			{Name: "b", Accepted: []ArtifactType{testTypeB, testTypeC}},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("duplicate input slot name", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "data", Accepted: []ArtifactType{testTypeA}},
			{Name: "data", Accepted: []ArtifactType{testTypeA}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
		require.ErrorContains(t, err, "data")
	})

	t.Run("duplicate output slot name", func(t *testing.T) {
		node := testNewTestNode("test")
		node.outputs = []*OutputSlot{
			{Name: "result", Type: testTypeA},
			{Name: "result", Type: testTypeA},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate output slot name")
		require.ErrorContains(t, err, "result")
	})

	t.Run("duplicate input among many", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "a", Accepted: []ArtifactType{testTypeA}},
			{Name: "b", Accepted: []ArtifactType{testTypeB}},
			{Name: "a", Accepted: []ArtifactType{testTypeA}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
	})

	t.Run("duplicate output among many", func(t *testing.T) {
		node := testNewTestNode("test")
		node.outputs = []*OutputSlot{
			{Name: "a", Type: testTypeA},
			{Name: "b", Type: testTypeB},
			{Name: "a", Type: testTypeA},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate output slot name")
	})

	t.Run("duplicate inputs and outputs", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "dup", Accepted: []ArtifactType{testTypeA}},
			{Name: "dup", Accepted: []ArtifactType{testTypeA}},
		}
		node.outputs = []*OutputSlot{
			{Name: "dup", Type: testTypeA},
			{Name: "dup", Type: testTypeA},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
	})

	t.Run("triple duplicate", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "same", Accepted: []ArtifactType{testTypeA}},
			{Name: "same", Accepted: []ArtifactType{testTypeA}},
			{Name: "same", Accepted: []ArtifactType{testTypeA}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
		require.ErrorContains(t, err, "same")
	})

	t.Run("empty accepted in input", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "empty", Accepted: []ArtifactType{}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "empty accepted artifact type")
	})

	t.Run("unregistered artifact type in input", func(t *testing.T) {
		node := testNewTestNode("test")
		node.inputs = []*InputSlot{
			{Name: "unknown", Accepted: []ArtifactType{{Name: "unknown"}}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "is not registered")
	})

	t.Run("unregistered artifact type in output", func(t *testing.T) {
		node := testNewTestNode("test")
		node.outputs = []*OutputSlot{
			{Name: "unknown", Type: ArtifactType{Name: "unknown"}},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "is not registered")
	})
}
