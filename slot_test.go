package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckNodeSlots(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		node := testNewTestNode("empty")

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("single input", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{{Name: "only"}}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("single output", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{{Name: "only"}}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique inputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique outputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "out1"},
			{Name: "out2"},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("unique inputs and outputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "in1"},
			{Name: "in2"},
		}
		node.outputs = []*OutputSlot{
			{Name: "out1"},
			{Name: "out2"},
			{Name: "out3"},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("inputs only", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("outputs only", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		err := CheckNodeSlots(node)
		require.NoError(t, err)
	})

	t.Run("duplicate input slot name", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "data"},
			{Name: "data"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
		require.ErrorContains(t, err, "data")
	})

	t.Run("duplicate output slot name", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "result"},
			{Name: "result"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate output slot name")
		require.ErrorContains(t, err, "result")
	})

	t.Run("duplicate input among many", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
			{Name: "a"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
	})

	t.Run("duplicate output among many", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "a"},
			{Name: "b"},
			{Name: "a"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate output slot name")
	})

	t.Run("duplicate inputs and outputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "dup"},
			{Name: "dup"},
		}
		node.outputs = []*OutputSlot{
			{Name: "dup"},
			{Name: "dup"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
	})

	t.Run("triple duplicate", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "same"},
			{Name: "same"},
			{Name: "same"},
		}

		err := CheckNodeSlots(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
		require.ErrorContains(t, err, "same")
	})
}
