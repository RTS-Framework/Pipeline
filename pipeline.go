package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Config contains the configuration for execute Pipeline.
type Config struct {
	// task id for get unique data from a list
	ID int `toml:"id" json:"id"`

	// Env is used to get environment data in context.
	Env map[string]any `toml:"env" json:"env"`

	// the logger for node that will use.
	Logger Logger `toml:"-" json:"-"`
}

// Options contains the options for create Pipeline.
type Options struct {
	BeforeNodeInitialize func(node Node) error
	AfterNodeInitialize  func(node Node) error
	BeforeNodeExecute    func(node Node) error
	AfterNodeExecute     func(node Node) error
	BeforeNodeClose      func(node Node) error
	AfterNodeClose       func(node Node) error
}

// Pipeline is a parallel directed acyclic graph (DAG) of processing nodes.
// It manages node registration, linking, validation, and execution.
type Pipeline struct {
	opts  Options
	nodes map[string]Node
	links map[string]*Link
	rwm   sync.RWMutex
}

// NewPipeline is used to create a new empty Pipeline instance.
func NewPipeline(opts *Options) *Pipeline {
	if opts == nil {
		opts = new(Options)
	}
	pipeline := Pipeline{
		opts:  *opts,
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
	if p.opts.BeforeNodeInitialize != nil {
		err = p.opts.BeforeNodeInitialize(node)
		if err != nil {
			return fmt.Errorf("[BeforeInitialize of Node %s]: %s", name, err)
		}
	}
	err = node.Initialize()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}
	if p.opts.AfterNodeInitialize != nil {
		err = p.opts.AfterNodeInitialize(node)
		if err != nil {
			return fmt.Errorf("[AfterInitialize of Node %s]: %s", name, err)
		}
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
	err = p.closeNode(node)
	if err != nil {
		return err
	}
	return nil
}

func (p *Pipeline) getNode(name string) (Node, error) {
	if node, ok := p.nodes[name]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("node \"%s\" is not found", name)
}

func (p *Pipeline) closeNode(node Node) error {
	name := node.Name()
	if p.opts.BeforeNodeClose != nil {
		err := p.opts.BeforeNodeClose(node)
		if err != nil {
			return fmt.Errorf("[BeforeClose of Node %s]: %s", name, err)
		}
	}
	err := node.Close()
	if err != nil {
		return fmt.Errorf("failed to clean node: %s", err)
	}
	if p.opts.AfterNodeClose != nil {
		err = p.opts.AfterNodeClose(node)
		if err != nil {
			return fmt.Errorf("[AfterClose of Node %s]: %s", name, err)
		}
	}
	return nil
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

// Nodes is used to get all nodes.
func (p *Pipeline) Nodes() []Node {
	p.rwm.RLock()
	defer p.rwm.RUnlock()
	nodes := make([]Node, 0, len(p.nodes))
	for _, node := range p.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// Links is used to get all links.
func (p *Pipeline) Links() []*Link {
	p.rwm.RLock()
	defer p.rwm.RUnlock()
	links := make([]*Link, 0, len(p.links))
	for _, link := range p.links {
		links = append(links, link)
	}
	return links
}

// Validate is used to check the pipeline's integrity before execution.
func (p *Pipeline) Validate() error {
	p.rwm.RLock()
	defer p.rwm.RUnlock()
	return p.validate()
}

func (p *Pipeline) validate() error {
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
	err = p.checkLinksValid()
	if err != nil {
		return err
	}
	// check has the ring with Kahn
	err = p.checkNoCycle()
	if err != nil {
		return err
	}
	return nil
}

// checkInputSlots verifies that every required input slot is linked.
// Optional input slots may stay unlinked; at runtime Read returns
// (nil, nil) for them and the Node must skip them.
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
				format := "required input slot is not linked: %s.%s"
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
				format := "output slot is not linked: %s.%s"
				return fmt.Errorf(format, nodeName, slotName)
			}
		}
	}
	return nil
}

// checkLinksValid is used to verify every link references existing nodes
// and slots, that source and destination are not the same node, and that
// no input slot is connected to more than one output slot.
func (p *Pipeline) checkLinksValid() error {
	dstSeen := make(map[string]struct{})
	for _, link := range p.links {
		srcName := link.srcNode.Name()
		dstName := link.dstNode.Name()
		// self-link is not allowed
		if srcName == dstName {
			return fmt.Errorf("self-link is not allowed: %s", link.path)
		}
		// source node must exist
		if _, ok := p.nodes[srcName]; !ok {
			return fmt.Errorf("link references unknown source node: %s", srcName)
		}
		// destination node must exist
		if _, ok := p.nodes[dstName]; !ok {
			return fmt.Errorf("link references unknown destination node: %s", dstName)
		}
		// source slot must still exist on the node
		if _, err := getNodeOutputSlot(link.srcNode, link.srcSlot.Name); err != nil {
			return fmt.Errorf("link %s: %s", link.path, err)
		}
		// destination slot must still exist on the node
		if _, err := getNodeInputSlot(link.dstNode, link.dstSlot.Name); err != nil {
			return fmt.Errorf("link %s: %s", link.path, err)
		}
		// each input slot can only be connected to one output slot
		dstKey := dstName + "." + link.dstSlot.Name
		if _, ok := dstSeen[dstKey]; ok {
			return fmt.Errorf("input slot %s is linked more than once", dstKey)
		}
		dstSeen[dstKey] = struct{}{}
	}
	return nil
}

// checkNoCycle use Kahn's algorithm to detect cycles in the pipeline graph.
// It builds an adjacency list of node names, computes in-degrees, and performs
// a topological traversal. If not all nodes can be traversed, a cycle exists.
func (p *Pipeline) checkNoCycle() error {
	if len(p.links) == 0 {
		return nil
	}
	// collect unique node names that appear in links
	inDegree := make(map[string]int)
	adj := make(map[string]map[string]struct{})
	for _, link := range p.links {
		src := link.srcNode.Name()
		dst := link.dstNode.Name()
		if _, ok := inDegree[src]; !ok {
			inDegree[src] = 0
			adj[src] = make(map[string]struct{})
		}
		if _, ok := inDegree[dst]; !ok {
			inDegree[dst] = 0
			adj[dst] = make(map[string]struct{})
		}
		if _, ok := adj[src][dst]; !ok {
			adj[src][dst] = struct{}{}
			inDegree[dst]++
		}
	}
	// seed queue with nodes having in-degree 0
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	// BFS traversal
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for neighbor := range adj[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}
	if visited != len(inDegree) {
		return errors.New("cycle detected in the pipeline")
	}
	return nil
}

// Run is the synchronous form of Execute: it starts the pipeline and
// blocks until the run finishes.
func (p *Pipeline) Run(ctx context.Context, cfg *Config) error {
	task, err := p.Execute(ctx, cfg)
	if err != nil {
		return err
	}
	return task.Wait()
}

// Execute validates the pipeline, builds a run Context and starts the
// run in the background, then returns immediately.
//
// Only one execution is allowed at a time: calling Execute again while
// a run is in progress returns an error.
//
// Use the returned Context to inspect node status (Nodes / NodeDone /
// NodeError / Errors), wait for completion (Wait), or interrupt the run
// (Interrupt).
func (p *Pipeline) Execute(ctx context.Context, cfg *Config) (Task, error) {
	p.rwm.RUnlock()
	defer p.rwm.RUnlock()
	err := p.validate()
	if err != nil {
		return nil, err
	}
	pCtx, err := p.newContext(ctx, cfg)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]Node, len(p.nodes))
	for name, node := range p.nodes {
		nodes[name] = node
	}
	go func() {
		err := p.execute(pCtx, nodes)
		pCtx.finish(err)
	}()
	return pCtx, nil
}

func (p *Pipeline) execute(ctx *pContext, nodes map[string]Node) error {
	var wg sync.WaitGroup
	for name, node := range nodes {
		wg.Add(1)
		go func(name string, node Node) {
			defer wg.Done()
			err := p.executeNode(ctx, node)
			ctx.setNodeError(name, err)
		}(name, node)
	}
	wg.Wait()
	var errs []error
	for name, err := range ctx.Errors() {
		errs = append(errs, fmt.Errorf("node %s: %s", name, err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// executeNode runs one node with surrounding hooks, panic recovery and
// output integrity checking. AfterNodeExecute always runs, even when
// the node's Execute panics.
func (p *Pipeline) executeNode(ctx *pContext, node Node) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("panic: " + fmt.Sprint(r))
		}
	}()

	name := node.Name()
	if p.opts.BeforeNodeExecute != nil {
		err = p.opts.BeforeNodeExecute(node)
		if err != nil {
			return fmt.Errorf("[BeforeExecute of Node %s]: %s", name, err)
		}
	}
	err = node.Execute(ctx)
	if err != nil {
		return err
	}
	if err == nil && ctx.Err() == nil {
		err = ctx.checkOutputsWritten(node)
		if err != nil {
			return err
		}
	}
	if p.opts.AfterNodeExecute != nil {
		err = p.opts.AfterNodeExecute(node)
		if err != nil {
			return fmt.Errorf("[AfterExecute of Node %s]: %s", name, err)
		}
	}
	return nil
}

// Close is used to release all resources held by the pipeline.
// It calls Close on every registered node and clears the internal state.
// After Close, the pipeline should not be reused.
func (p *Pipeline) Close() error {
	p.rwm.Lock()
	defer p.rwm.Unlock()
	for _, node := range p.nodes {
		err := p.closeNode(node)
		if err != nil {
			return err
		}
	}
	p.nodes = make(map[string]Node)
	p.links = make(map[string]*Link)
	return nil
}
