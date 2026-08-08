package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testNode struct {
	name string

	inputs  []*InputSlot
	outputs []*OutputSlot

	exec func(ctx *Context) error
}

func testNewTestNode(name string) *testNode {
	return &testNode{name: name}
}

func (n *testNode) Name() string {
	return n.name
}

func (n *testNode) Type() string {
	return "test"
}

func (n *testNode) Description() string {
	return "test node"
}

func (n *testNode) BuildTag() string {
	return ""
}

func (n *testNode) Inputs() []*InputSlot {
	return n.inputs
}

func (n *testNode) Outputs() []*OutputSlot {
	return n.outputs
}

func (n *testNode) Initialize() error {
	return nil
}

func (n *testNode) Execute(ctx *Context) error {
	return n.exec(ctx)
}

func (n *testNode) Close() error {
	return nil
}

func TestCheckNode(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		node := testNewTestNode("empty")

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("single input", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{{Name: "only"}}

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("single output", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{{Name: "only"}}

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("unique inputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		}

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("unique outputs", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "out1"},
			{Name: "out2"},
		}

		err := CheckNode(node)
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

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("inputs only", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("outputs only", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		err := CheckNode(node)
		require.NoError(t, err)
	})

	t.Run("duplicate input slot name", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "data"},
			{Name: "data"},
		}

		err := CheckNode(node)
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

		err := CheckNode(node)
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

		err := CheckNode(node)
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

		err := CheckNode(node)
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

		err := CheckNode(node)
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

		err := CheckNode(node)
		require.Error(t, err)
		require.ErrorContains(t, err, "duplicate input slot name")
		require.ErrorContains(t, err, "same")
	})
}

func TestGetNodeInputSlot(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		slot, err := getNodeInputSlot(node, "a")
		require.NoError(t, err)
		require.Equal(t, "a", slot.Name)
	})

	t.Run("not found", func(t *testing.T) {
		node := testNewTestNode("n")
		node.inputs = []*InputSlot{
			{Name: "a"},
		}

		slot, err := getNodeInputSlot(node, "miss")
		require.ErrorContains(t, err, "not found")
		require.Nil(t, slot)
	})
}

func TestGetNodeOutputSlot(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "a"},
			{Name: "b"},
		}

		slot, err := getNodeOutputSlot(node, "a")
		require.NoError(t, err)
		require.Equal(t, "a", slot.Name)
	})

	t.Run("not found", func(t *testing.T) {
		node := testNewTestNode("n")
		node.outputs = []*OutputSlot{
			{Name: "a"},
		}

		slot, err := getNodeOutputSlot(node, "miss")
		require.ErrorContains(t, err, "not found")
		require.Nil(t, slot)
	})
}
