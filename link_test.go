package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLink(t *testing.T) {
	t.Run("common", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}

		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.NoError(t, err)

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("unknown src node", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.ErrorContains(t, err, "not found")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("unknown dst node", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.ErrorContains(t, err, "not found")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("unknown src slot", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}
		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "miss", "B", "in")
		require.ErrorContains(t, err, "not found")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("unknown dst slot", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}
		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "miss")
		require.ErrorContains(t, err, "not found")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("mismatched slot type", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}
		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeB}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.ErrorContains(t, err, "mismatched slot type")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("duplicate link", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}
		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.NoError(t, err)
		err = pipeline.Link("A", "out", "B", "in")
		require.ErrorContains(t, err, "already linked")

		err = pipeline.Close()
		require.NoError(t, err)
	})

	t.Run("one output to many inputs", func(t *testing.T) {
		pipeline := NewPipeline()

		nodeA := testNewTestNode("A")
		nodeA.outputs = []*OutputSlot{
			{Name: "out", Type: testTypeA},
		}
		nodeB := testNewTestNode("B")
		nodeB.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}
		nodeC := testNewTestNode("C")
		nodeC.inputs = []*InputSlot{
			{Name: "in", Accepted: []ArtifactType{testTypeA}},
		}

		err := pipeline.AddNode(nodeA)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeB)
		require.NoError(t, err)
		err = pipeline.AddNode(nodeC)
		require.NoError(t, err)

		err = pipeline.Link("A", "out", "B", "in")
		require.NoError(t, err)
		err = pipeline.Link("A", "out", "C", "in")
		require.NoError(t, err)

		err = pipeline.Close()
		require.NoError(t, err)
	})
}
