package pipeline

import (
	"errors"
	"fmt"
	"sync"
)

type Options struct {
	LogDirectory string

	BeforeNodeInitialize func(node Node) error
	AfterNodeInitialize  func(node Node) error
	BeforeNodeExecute    func(node Node) error
	AfterNodeExecute     func(node Node) error
}

type Pipeline struct {
	nodes map[string]Node
	links map[string]*Link

	// logger counter for create context
	counter int64

	rwm sync.RWMutex
}

type Link struct {
	path string

	srcNode Node
	srcSlot *OutputSlot

	dstNode Node
	dstSlot *InputSlot
}

func (l *Link) String() string {
	return l.path
}

func NewPipeline() *Pipeline {
	pipeline := Pipeline{
		nodes: make(map[string]Node),
		links: make(map[string]*Link),
	}
	return &pipeline
}

func (p *Pipeline) AddNode(node Node) error {
	err := checkNode(node)
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

func (p *Pipeline) Validate() error {
	return nil
}

func (p *Pipeline) Execute(opts *Options) error {
	if opts == nil {
		opts = &Options{}
	}
	return nil
}

func (p *Pipeline) Interrupt() error {
	return nil
}

func (p *Pipeline) Close() error {
	return nil
}

func buildLinkPath(srcNode, srcSlot, dstNode, dstSlot string) string {
	return fmt.Sprintf("[%s.%s] -> [%s.%s]", srcNode, srcSlot, dstNode, dstSlot)
}
