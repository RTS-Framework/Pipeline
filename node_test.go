package pipeline

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
