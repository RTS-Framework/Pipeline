package pipeline

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Context is a Pipeline execute context, it is a mirror state of Pipeline.
//
// A Context is created by Pipeline.Execute and returned immediately:
// the pipeline runs in the background, and the caller can inspect
// per-node status, wait for completion, or interrupt the run at any
// time. Pipeline itself only stores the static node/link graph; all
// runtime state lives in this Context.
//
// Lifecycle:
//   - Done() is closed when the whole run finishes (success or failure).
//   - Wait() blocks until the run finishes and returns the joined error.
//   - Interrupt() cancels the run.
type Context interface {
	context.Context

	// ID is used to return current context id.
	// usually it get unique data from a list.
	ID() int

	// Logger is used to get logger from context, if Node want
	// to write log, need call this method.
	Logger() Logger

	// Read reads one Artifact from the node's input slot.
	//
	// An optional slot that is not linked returns (nil, nil); the node
	// should treat it as "no data" and skip. Each input slot can be read at
	// most once per execution; reading it again returns an error.
	Read(node Node, slot string) (*Artifact, error)

	// Write writes one Artifact to the node's output slot and clones
	// it to every linked input slot.
	//
	// Each output slot must be written exactly once; writing it again
	// returns an error. Write returns only after every linked channel
	// has accepted the artifact, or the context is canceled.
	Write(node Node, slot string, art *Artifact) error
}

type pContext struct {
	context.Context

	// task id for get unique data from a list
	id int

	logger Logger

	// key is "nodeName.slotName"
	inputs  map[string]<-chan *Artifact
	outputs map[string][]chan<- *Artifact

	// exactly-once access tracking for slots
	// key is "nodeName.slotName"
	inputRead   map[string]struct{}
	outputWrote map[string]struct{}
	slotMu      sync.Mutex

	// node running status
	// key is the node name
	nodesDone map[string]chan struct{}
	nodesErr  map[string]error
	nodesMu   sync.Mutex

	// lifecycle status
	finished   chan struct{}
	finishOnce sync.Once
	resultErr  error
	statusMu   sync.Mutex

	cancel context.CancelFunc
}

func (p *Pipeline) newContext(ctx context.Context, id int, opts *Options) (*pContext, error) {
	// prepare the logger with discord if opts.Logger is nil
	logger := opts.Logger
	if logger == nil {
		logger = NewLogger(io.Discard)
	}
	// create channel for each link
	linkChs := make(map[string]chan *Artifact, len(p.links))
	for link := range p.links {
		linkChs[link] = make(chan *Artifact, 1)
	}
	// build link index
	inputs := make(map[string]<-chan *Artifact)
	outputs := make(map[string][]chan<- *Artifact)
	for _, link := range p.links {
		ch := linkChs[link.path]
		key := link.dstNode.Name() + "." + link.dstSlot.Name
		inputs[key] = ch
		key = link.srcNode.Name() + "." + link.srcSlot.Name
		outputs[key] = append(outputs[key], ch)
	}
	// prepare node execute status
	nodesDone := make(map[string]chan struct{}, len(p.nodes))
	for name := range p.nodes {
		nodesDone[name] = make(chan struct{})
	}
	c := &pContext{
		id:          id,
		logger:      logger,
		inputs:      inputs,
		outputs:     outputs,
		inputRead:   make(map[string]struct{}),
		outputWrote: make(map[string]struct{}),
		nodesDone:   nodesDone,
		nodesErr:    make(map[string]error),
		finished:    make(chan struct{}),
	}
	c.Context, c.cancel = context.WithCancel(ctx)
	return c, nil
}

func (ctx *pContext) ID() int {
	return ctx.id
}

func (ctx *pContext) Logger() Logger {
	return ctx.logger
}

func (ctx *pContext) Read(node Node, slot string) (*Artifact, error) {
	_, err := getNodeInputSlot(node, slot)
	if err != nil {
		return nil, err
	}
	key := node.Name() + "." + slot
	if ctx.isInputRead(key) {
		return nil, fmt.Errorf("input slot %s already read", key)
	}
	ch := ctx.inputs[key]
	if ch == nil {
		return nil, nil
	}
	select {
	case art := <-ch:
		return art, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ctx *pContext) isInputRead(key string) bool {
	ctx.slotMu.Lock()
	defer ctx.slotMu.Unlock()
	if _, ok := ctx.inputRead[key]; ok {
		return true
	}
	ctx.inputRead[key] = struct{}{}
	return false
}

func (ctx *pContext) Write(node Node, slot string, art *Artifact) error {
	_, err := getNodeOutputSlot(node, slot)
	if err != nil {
		return err
	}
	key := node.Name() + "." + slot
	if ctx.isOutputWritten(key) {
		return fmt.Errorf("output slot %s already written", key)
	}
	chs := ctx.outputs[key]
	for _, ch := range chs {
		select {
		case ch <- art.Clone():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (ctx *pContext) isOutputWritten(key string) bool {
	ctx.slotMu.Lock()
	defer ctx.slotMu.Unlock()
	if _, ok := ctx.outputWrote[key]; ok {
		return true
	}
	ctx.outputWrote[key] = struct{}{}
	return false
}

// checkOutputsWritten verifies that a node that completed successfully
// wrote exactly one Artifact to every linked output slot.
func (ctx *pContext) checkOutputsWritten(node Node) error {
	var missing []string
	for _, slot := range node.Outputs() {
		key := node.Name() + "." + slot.Name
		if !ctx.isOutputWritten(key) {
			missing = append(missing, slot.Name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	format := "node %s execute success but not write artifact to output slot(s): %s"
	return fmt.Errorf(format, node.Name(), strings.Join(missing, ", "))
}

// setNodeResult records the result of a finished node and closes its
// done channel. It is called by the execution runner.
func (ctx *pContext) setNodeResult(name string, err error) {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	ctx.nodesErr[name] = err
	close(ctx.nodesDone[name])
}

// finish records the final pipeline result and closes Done(). It is
// called by the execution runner.
func (ctx *pContext) finish(err error) {
	ctx.statusMu.Lock()
	ctx.resultErr = err
	ctx.statusMu.Unlock()
	ctx.finishOnce.Do(func() {
		close(ctx.finished)
	})
}

// Running reports whether the pipeline run is still in progress.
func (ctx *pContext) Running() bool {
	select {
	case <-ctx.finished:
		return false
	default:
		return true
	}
}

// Wait blocks until the pipeline run finishes and returns the joined
// error: nil on success, a joined node error on failure, or a
// cancellation error.
func (ctx *pContext) Wait() error {
	<-ctx.finished
	ctx.statusMu.Lock()
	defer ctx.statusMu.Unlock()
	return ctx.resultErr
}

// NodeDone returns the channel that is closed when the node finishes.
func (ctx *pContext) NodeDone(name string) (<-chan struct{}, error) {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	ch, ok := ctx.nodesDone[name]
	if !ok {
		return nil, fmt.Errorf("node %q is not found", name)
	}
	return ch, nil
}

// NodeError returns the error recorded for a node. It returns nil for
// nodes that succeeded.
func (ctx *pContext) NodeError(name string) error {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	return ctx.nodesErr[name]
}

// Errors returns a copy of the per-node error map.
func (ctx *pContext) Errors() map[string]error {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	out := make(map[string]error, len(ctx.nodesErr))
	for name, e := range ctx.nodesErr {
		out[name] = e
	}
	return out
}

func (ctx *pContext) Interrupt() {
	ctx.cancel()
}

// Done returns a channel that is closed when the whole pipeline run
// finishes (success or failure).
//
// This is different from the embedded context.Context.Done, which only
// fires on cancellation.
func (ctx *pContext) Done() <-chan struct{} {
	return ctx.finished
}

// Err returns the current result error without blocking. It returns nil
// while the run is still in progress or has succeeded.
func (ctx *pContext) Err() error {
	ctx.statusMu.Lock()
	defer ctx.statusMu.Unlock()
	return ctx.resultErr
}
