package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testNode struct {
	name string

	buildTag string

	inputs  []*InputSlot
	outputs []*OutputSlot

	exec func(ctx Context) error
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
	return n.buildTag
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

func (n *testNode) Execute(ctx Context) error {
	return n.exec(ctx)
}

func (n *testNode) Close() error {
	return nil
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
