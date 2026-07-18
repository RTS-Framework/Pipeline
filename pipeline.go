package pipeline

import (
	"errors"
	"fmt"
	"sync"
)

type Pipeline struct {
	logger *logger

	nodes map[string]*pNode
	links map[string]*pLink

	rwm sync.RWMutex
}

type pNode struct {
	node Node
	done chan struct{}
	err  error
}

type pLink struct {
	path string

	srcNode Node
	srcSlot *OutputSlot

	dstNode Node
	dstSlot *InputSlot

	channel chan *Artifact
}

func NewPipeline() *Pipeline {
	p := Pipeline{
		nodes: make(map[string]*pNode),
		links: make(map[string]*pLink),
	}
	return &p
}

func (p *Pipeline) AddNode(node Node) error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	name := node.Name()
	_, err := p.getNode(name)
	if err == nil {
		return fmt.Errorf("node \"%s\" already exists", name)
	}
	err = node.Initialize()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}
	p.nodes[name] = &pNode{
		node: node,
	}
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
	err = node.node.Close()
	if err != nil {
		return fmt.Errorf("failed to clean node: %s", err)
	}
	return nil
}

func (p *Pipeline) getNode(name string) (*pNode, error) {
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
	_, err := p.getLink(srcNode, srcSlot, dstNode, dstSlot)
	if err == nil {
		return errors.New("this path is already linked")
	}
	sNode, err := p.getNode(srcNode)
	if err != nil {
		return err
	}
	sSlot, err := getNodeOutputSlot(sNode.node, srcSlot)
	if err != nil {
		return err
	}
	dNode, err := p.getNode(dstNode)
	if err != nil {
		return err
	}
	dSlot, err := getNodeInputSlot(dNode.node, dstSlot)
	if err != nil {
		return err
	}
	if !isSlotTypeMatched(dSlot.Accepted, sSlot.Type) {
		return errors.New("mismatched slot type")
	}
	path := buildLinkPath(srcNode, srcSlot, dstNode, dstSlot)
	p.links[path] = &pLink{
		path:    path,
		srcNode: sNode.node,
		srcSlot: sSlot,
		dstNode: dNode.node,
		dstSlot: dSlot,
	}
	return nil
}

// Unlink is used to unlink output slot -> input slot.
func (p *Pipeline) Unlink(srcNode, srcSlot, dstNode, dstSlot string) error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	_, err := p.getLink(srcNode, srcSlot, dstNode, dstSlot)
	if err != nil {
		return err
	}
	path := buildLinkPath(srcNode, srcSlot, dstNode, dstSlot)
	delete(p.links, path)
	return nil
}

func (p *Pipeline) getLink(srcNode, srcSlot, dstNode, dstSlot string) (*pLink, error) {
	path := buildLinkPath(srcNode, srcSlot, dstNode, dstSlot)
	if link, ok := p.links[path]; ok {
		return link, nil
	}
	return nil, fmt.Errorf("link \"%s\" is not found", path)
}

func (p *Pipeline) Validate() error {
	return nil
}

func (p *Pipeline) Execute(ctx *Context) error {
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
