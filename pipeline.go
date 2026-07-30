package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Options contains the option for Pipeline.Execute.
type Options struct {
	Logger Logger

	BeforeNodeInitialize func(node Node) error
	AfterNodeInitialize  func(node Node) error
	BeforeNodeExecute    func(node Node) error
	AfterNodeExecute     func(node Node) error
}

// Pipeline is a parallel directed acyclic graph (DAG) of processing nodes.
// It manages node registration, linking, validation, and execution.
type Pipeline struct {
	nodes map[string]Node
	links map[string]*Link
	rwm   sync.RWMutex
}

// NewPipeline is used to create a new empty Pipeline instance.
func NewPipeline() *Pipeline {
	pipeline := Pipeline{
		nodes: make(map[string]Node),
		links: make(map[string]*Link),
	}
	return &pipeline
}

// AddNode is used to register a new node to the pipeline.
// The node must pass validation (see CheckNode) and have a unique name.
// Upon successful addition, the node's Initialize method is called once.
func (p *Pipeline) AddNode(node Node) error {
	err := CheckNode(node)
	if err != nil {
		return err
	}
	p.rwm.Lock()
	defer p.rwm.Unlock()
	name := node.Name()
	_, err = p.getNode(name)
	if err == nil {
		return fmt.Errorf("node \"%s\" already exists", name)
	}
	err = node.Initialize()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}
	p.nodes[name] = node
	return nil
}

// RemoveNode is used to remove a node from the pipeline by its name.
// The node's Close method is called to release held resources.
//
// Note: This does not automatically remove links connected to the node.
// Callers (e.g., UI layer) should unlink all connected edges first,
// then call RemoveNode. If links remain, Validate/Execute will report errors.
func (p *Pipeline) RemoveNode(name string) error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	node, err := p.getNode(name)
	if err != nil {
		return err
	}
	delete(p.nodes, name)
	err = node.Close()
	if err != nil {
		return fmt.Errorf("failed to clean node: %s", err)
	}
	return nil
}

func (p *Pipeline) getNode(name string) (Node, error) {
	if node, ok := p.nodes[name]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("node \"%s\" is not found", name)
}

// Link is used to link a source node output slot to the destination
// node input slot, one output slot can link to multi input slot,
// but one input slot can only be linked with one output slot.
func (p *Pipeline) Link(srcNode, srcSlot, dstNode, dstSlot string) error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	path := buildLinkPath(srcNode, srcSlot, dstNode, dstSlot)
	_, err := p.getLink(path)
	if err == nil {
		return fmt.Errorf("\"%s\" is already linked", path)
	}
	sNode, err := p.getNode(srcNode)
	if err != nil {
		return err
	}
	sSlot, err := getNodeOutputSlot(sNode, srcSlot)
	if err != nil {
		return err
	}
	dNode, err := p.getNode(dstNode)
	if err != nil {
		return err
	}
	dSlot, err := getNodeInputSlot(dNode, dstSlot)
	if err != nil {
		return err
	}
	if !isSlotTypeMatched(dSlot.Accepted, sSlot.Type) {
		return errors.New("mismatched slot type")
	}
	p.links[path] = &Link{
		path:    path,
		srcNode: sNode,
		srcSlot: sSlot,
		dstNode: dNode,
		dstSlot: dSlot,
	}
	return nil
}

// Unlink is used to unlink output slot -> input slot.
func (p *Pipeline) Unlink(srcNode, srcSlot, dstNode, dstSlot string) error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	path := buildLinkPath(srcNode, srcSlot, dstNode, dstSlot)
	_, err := p.getLink(path)
	if err != nil {
		return err
	}
	delete(p.links, path)
	return nil
}

func (p *Pipeline) getLink(path string) (*Link, error) {
	if link, ok := p.links[path]; ok {
		return link, nil
	}
	return nil, fmt.Errorf("link \"%s\" is not found", path)
}

// Validate is used to check the pipeline's integrity before execution.
func (p *Pipeline) Validate() error {
	p.rwm.RLock()
	defer p.rwm.RUnlock()
	if len(p.nodes) == 0 {
		return nil
	}
	// check the required input slots are all linked
	err := p.checkInputSlots()
	if err != nil {
		return err
	}
	// check the output slots are all linked
	err = p.checkOutputSlots()
	if err != nil {
		return err
	}
	// check the links are valid

	// check has the ring with Kahn

	return nil
}

func (p *Pipeline) checkInputSlots() error {
	linked := make(map[string]struct{})
	for _, link := range p.links {
		key := link.dstNode.Name() + "." + link.dstSlot.Name
		linked[key] = struct{}{}
	}
	for _, node := range p.nodes {
		for _, slot := range node.Inputs() {
			if !slot.Required {
				continue
			}
			nodeName := node.Name()
			slotName := slot.Name
			key := nodeName + "." + slotName
			if _, ok := linked[key]; !ok {
				format := "required input slot not linked: %s.%s"
				return fmt.Errorf(format, nodeName, slotName)
			}
		}
	}
	return nil
}

func (p *Pipeline) checkOutputSlots() error {
	linked := make(map[string]struct{})
	for _, link := range p.links {
		key := link.srcNode.Name() + "." + link.srcSlot.Name
		linked[key] = struct{}{}
	}
	for _, node := range p.nodes {
		for _, slot := range node.Outputs() {
			nodeName := node.Name()
			slotName := slot.Name
			key := nodeName + "." + slotName
			if _, ok := linked[key]; !ok {
				format := "output slot not linked: %s.%s"
				return fmt.Errorf(format, nodeName, slotName)
			}
		}
	}

	return nil
}

// Execute is used to run the pipeline with the given options.
// The pipeline must pass validation before execution begins.
// Nodes are executed concurrently in topological order; a node starts
// when all its input channels have data available.
func (p *Pipeline) Execute(ctx context.Context, opts *Options) (*Context, error) {
	err := p.Validate()
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &Options{}
	}
	p.rwm.RLock()
	defer p.rwm.RUnlock()
	pCtx, err := p.newContext(ctx, opts)
	if err != nil {
		return nil, err
	}

	return pCtx, nil
}

// Close is used to release all resources held by the pipeline.
// It calls Close on every registered node and clears the internal state.
// After Close, the pipeline should not be reused.
func (p *Pipeline) Close() error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	for _, node := range p.nodes {
		err := node.Close()
		if err != nil {
			return err
		}
	}
	p.nodes = make(map[string]Node)
	p.links = make(map[string]*Link)
	return nil
}
