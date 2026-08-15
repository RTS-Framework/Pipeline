package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// logIDCounter is the logger counter for creating context.
var logIDCounter int64

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
type Context struct {
	context.Context

	logger Logger

	// key is "nodeName.slotName"
	inputIndex  map[string]<-chan *Artifact
	outputIndex map[string][]chan<- *Artifact

	// exactly-once access tracking for slots
	// key is "nodeName.slotName"
	inputRead   map[string]bool
	outputWrote map[string]bool
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

func (p *Pipeline) newContext(ctx context.Context, opts *Options) (*Context, error) {
	// prepare the logger if opts.Logger is nil
	logger := opts.Logger
	if logger == nil {
		logID := atomic.AddInt64(&logIDCounter, 1)
		logName := fmt.Sprintf("pipeline-%d-%04d.log", time.Now().UnixNano(), logID)
		logPath := filepath.Join("logs", logName)
		err := os.MkdirAll("logs", 0750)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600) // #nosec
		if err != nil {
			return nil, err
		}
		logger = NewLogger(file)
	}
	// create channel for each link
	linkChs := make(map[string]chan *Artifact, len(p.links))
	for path := range p.links {
		linkChs[path] = make(chan *Artifact, 1)
	}
	// build link index
	inputIndex := make(map[string]<-chan *Artifact)
	outputIndex := make(map[string][]chan<- *Artifact)
	for _, link := range p.links {
		ch := linkChs[link.path]
		inputKey := link.dstNode.Name() + "." + link.dstSlot.Name
		inputIndex[inputKey] = ch
		outputKey := link.srcNode.Name() + "." + link.srcSlot.Name
		outputIndex[outputKey] = append(outputIndex[outputKey], ch)
	}
	// prepare node execute status
	nodesDone := make(map[string]chan struct{}, len(p.nodes))
	for name := range p.nodes {
		nodesDone[name] = make(chan struct{})
	}
	c := &Context{
		logger:      logger,
		inputIndex:  inputIndex,
		outputIndex: outputIndex,
		inputRead:   make(map[string]bool),
		outputWrote: make(map[string]bool),
		nodesDone:   nodesDone,
		nodesErr:    make(map[string]error),
		finished:    make(chan struct{}),
	}
	c.Context, c.cancel = context.WithCancel(ctx)
	return c, nil
}

// ReadInput reads one Artifact from the node's input slot.
//
// An optional slot that is not linked returns (nil, nil); the node
// should treat it as "no data" and skip. Each input slot can be read at
// most once per execution; reading it again returns an error.
func (ctx *Context) ReadInput(node Node, slotName string) (*Artifact, error) {
	if _, err := getNodeInputSlot(node, slotName); err != nil {
		return nil, err
	}
	key := node.Name() + "." + slotName

	ctx.slotMu.Lock()
	if ctx.inputRead[key] {
		ctx.slotMu.Unlock()
		return nil, fmt.Errorf("input slot %s already read", key)
	}
	ctx.inputRead[key] = true
	ctx.slotMu.Unlock()

	ch := ctx.inputIndex[key]
	if ch == nil {
		// optional slot without link: no data
		return nil, nil
	}
	select {
	case art := <-ch:
		return art, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WriteOutput writes one Artifact to the node's output slot and clones
// it to every linked input slot.
//
// Each output slot must be written exactly once; writing it again
// returns an error. WriteOutput returns only after every linked channel
// has accepted the artifact, or the context is canceled.
func (ctx *Context) WriteOutput(node Node, slotName string, art *Artifact) error {
	if _, err := getNodeOutputSlot(node, slotName); err != nil {
		return err
	}
	key := node.Name() + "." + slotName

	ctx.slotMu.Lock()
	if ctx.outputWrote[key] {
		ctx.slotMu.Unlock()
		return fmt.Errorf("output slot %s already written", key)
	}
	ctx.outputWrote[key] = true
	ctx.slotMu.Unlock()

	chs := ctx.outputIndex[key]
	if chs == nil {
		return fmt.Errorf("output slot %s is not linked", key)
	}
	for _, ch := range chs {
		select {
		case ch <- art.Clone():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// checkOutputsWritten verifies that a node that completed successfully
// wrote exactly one Artifact to every linked output slot.
func (ctx *Context) checkOutputsWritten(node Node) error {
	var missing []string
	for _, slot := range node.Outputs() {
		key := node.Name() + "." + slot.Name
		if _, linked := ctx.outputIndex[key]; !linked {
			// Validate already rejects unlinked outputs; defensive only.
			continue
		}
		ctx.slotMu.Lock()
		wrote := ctx.outputWrote[key]
		ctx.slotMu.Unlock()
		if !wrote {
			missing = append(missing, slot.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("node %q returned success without writing output slot(s): %s",
			node.Name(), strings.Join(missing, ", "))
	}
	return nil
}

// setNodeResult records the result of a finished node and closes its
// done channel. It is called by the execution runner.
func (ctx *Context) setNodeResult(name string, err error) {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	ctx.nodesErr[name] = err
	close(ctx.nodesDone[name])
}

// finish records the final pipeline result and closes Done(). It is
// called by the execution runner.
func (ctx *Context) finish(err error) {
	ctx.statusMu.Lock()
	ctx.resultErr = err
	ctx.statusMu.Unlock()
	ctx.finishOnce.Do(func() {
		close(ctx.finished)
	})
}

// Done returns a channel that is closed when the whole pipeline run
// finishes (success or failure).
//
// This is different from the embedded context.Context.Done, which only
// fires on cancellation.
func (ctx *Context) Done() <-chan struct{} {
	return ctx.finished
}

// Wait blocks until the pipeline run finishes and returns the joined
// error: nil on success, a joined node error on failure, or a
// cancellation error.
func (ctx *Context) Wait() error {
	<-ctx.finished
	ctx.statusMu.Lock()
	defer ctx.statusMu.Unlock()
	return ctx.resultErr
}

// Err returns the current result error without blocking. It returns nil
// while the run is still in progress or has succeeded.
func (ctx *Context) Err() error {
	ctx.statusMu.Lock()
	defer ctx.statusMu.Unlock()
	return ctx.resultErr
}

// Running reports whether the pipeline run is still in progress.
func (ctx *Context) Running() bool {
	select {
	case <-ctx.finished:
		return false
	default:
		return true
	}
}

// NodeDone returns the channel that is closed when the node finishes.
func (ctx *Context) NodeDone(name string) (<-chan struct{}, error) {
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
func (ctx *Context) NodeError(name string) error {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	return ctx.nodesErr[name]
}

// Errors returns a copy of the per-node error map.
func (ctx *Context) Errors() map[string]error {
	ctx.nodesMu.Lock()
	defer ctx.nodesMu.Unlock()
	out := make(map[string]error, len(ctx.nodesErr))
	for name, e := range ctx.nodesErr {
		out[name] = e
	}
	return out
}

// Logger is used to get logger from context, if a Node want
// to write log, need call this method.
func (ctx *Context) Logger() Logger {
	return ctx.logger
}

// Interrupt is used to interrupt the whole execute context.
func (ctx *Context) Interrupt() {
	ctx.cancel()
}
