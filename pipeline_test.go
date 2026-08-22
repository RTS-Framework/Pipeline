package pipeline

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPipeline(t *testing.T) {
	p := NewPipeline()
	require.NotNil(t, p)
	assert.Empty(t, p.Nodes())
	assert.Empty(t, p.Links())
}

func TestPipeline_AddNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("n1")
		require.NoError(t, p.AddNode(n))
		assert.Len(t, p.Nodes(), 1)
	})

	t.Run("duplicate name", func(t *testing.T) {
		p := NewPipeline()
		require.NoError(t, p.AddNode(testNewTestNode("dup")))
		err := p.AddNode(testNewTestNode("dup"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("invalid node — duplicate input slots", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("bad")
		n.inputs = []*InputSlot{
			{Name: "x"},
			{Name: "x"},
		}
		err := p.AddNode(n)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate input slot")
	})

	t.Run("invalid node — duplicate output slots", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("bad")
		n.outputs = []*OutputSlot{
			{Name: "y"},
			{Name: "y"},
		}
		err := p.AddNode(n)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate output slot")
	})

	t.Run("initialize error", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("fail")
		n.InitializeFunc = func() error { return errors.New("init boom") }
		err := p.AddNode(n)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to initialize node")
	})
}

func TestPipeline_RemoveNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("n")
		require.NoError(t, p.AddNode(n))
		require.NoError(t, p.RemoveNode("n"))
		assert.Empty(t, p.Nodes())
	})

	t.Run("not found", func(t *testing.T) {
		p := NewPipeline()
		err := p.RemoveNode("ghost")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("close error", func(t *testing.T) {
		p := NewPipeline()
		n := testNewTestNode("n")
		n.CloseFunc = func() error { return errors.New("close boom") }
		require.NoError(t, p.AddNode(n))
		err := p.RemoveNode("n")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "close boom")
	})
}

func TestPipeline_Link(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p, _, _ := newSimplePipeline(t)
		assert.Len(t, p.Links(), 1)
	})

	t.Run("source node not found", func(t *testing.T) {
		p := NewPipeline()
		b := testNewTestNode("B")
		b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}
		require.NoError(t, p.AddNode(b))
		err := p.Link("A", "out", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("destination node not found", func(t *testing.T) {
		p := NewPipeline()
		a := testNewTestNode("A")
		a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}
		require.NoError(t, p.AddNode(a))
		err := p.Link("A", "out", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("source slot not found", func(t *testing.T) {
		p := NewPipeline()
		a := testNewTestNode("A")
		a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}
		b := testNewTestNode("B")
		b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}
		require.NoError(t, p.AddNode(a))
		require.NoError(t, p.AddNode(b))
		err := p.Link("A", "miss", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("destination slot not found", func(t *testing.T) {
		p := NewPipeline()
		a := testNewTestNode("A")
		a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}
		b := testNewTestNode("B")
		b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}
		require.NoError(t, p.AddNode(a))
		require.NoError(t, p.AddNode(b))
		err := p.Link("A", "out", "B", "miss")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("type mismatch", func(t *testing.T) {
		p := NewPipeline()
		a := testNewTestNode("A")
		a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}
		b := testNewTestNode("B")
		b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeB}}}
		require.NoError(t, p.AddNode(a))
		require.NoError(t, p.AddNode(b))
		err := p.Link("A", "out", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mismatched slot type")
	})

	t.Run("duplicate link", func(t *testing.T) {
		p, _, _ := newSimplePipeline(t)
		err := p.Link("A", "out", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already linked")
	})

	t.Run("fan-out: one output to many inputs", func(t *testing.T) {
		p := NewPipeline()
		a := testNewTestNode("A")
		a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}

		b := testNewTestNode("B")
		b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}

		c := testNewTestNode("C")
		c.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}

		require.NoError(t, p.AddNode(a))
		require.NoError(t, p.AddNode(b))
		require.NoError(t, p.AddNode(c))
		require.NoError(t, p.Link("A", "out", "B", "in"))
		require.NoError(t, p.Link("A", "out", "C", "in"))
		assert.Len(t, p.Links(), 2)
	})
}

func TestPipeline_Unlink(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		p, _, _ := newSimplePipeline(t)
		require.NoError(t, p.Unlink("A", "out", "B", "in"))
		assert.Empty(t, p.Links())
	})

	t.Run("link not found", func(t *testing.T) {
		p := NewPipeline()
		err := p.Unlink("A", "out", "B", "in")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func newSimplePipeline(t *testing.T) (*Pipeline, *testNode, *testNode) {
	t.Helper()
	p := NewPipeline()

	a := testNewTestNode("A")
	a.outputs = []*OutputSlot{{Name: "out", Type: testTypeA}}

	b := testNewTestNode("B")
	b.inputs = []*InputSlot{{Name: "in", Accepted: []ArtifactType{testTypeA}}}

	require.NoError(t, p.AddNode(a))
	require.NoError(t, p.AddNode(b))
	require.NoError(t, p.Link("A", "out", "B", "in"))
	return p, a, b
}

// wireExec sets exec on node so it reads inSlot, passes data through,
// and writes to outSlot.
func wireExec(node *testNode, inSlot, outSlot string, transform func(any) any) {
	node.exec = func(ctx *Context) error {
		art, err := ctx.ReadInput(node, inSlot)
		if err != nil {
			return err
		}
		if art == nil {
			return nil
		}
		if transform != nil {
			art.Data = transform(art.Data)
		}
		return ctx.WriteOutput(node, outSlot, art)
	}
}
