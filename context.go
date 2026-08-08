package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// logger counter for create context
var logIDCounter int64

// Context is a Pipeline execute context, it is a mirror state of Pipeline.
type Context struct {
	context.Context

	logger Logger

	// key is "nodeName.slotName"
	inputIndex  map[string]<-chan *Artifact
	outputIndex map[string][]chan<- *Artifact

	// key is the node name
	limitGetInput  map[string]struct{}
	limitGetOutput map[string]struct{}
	limitGetMu     sync.Mutex

	// node running status
	// key is node name
	nodesDone map[string]chan struct{}
	nodesErr  map[string]error
	nodesMu   sync.Mutex

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
		logger = NewLogger(io.MultiWriter(os.Stdout, file))
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
		logger:         logger,
		limitGetInput:  make(map[string]struct{}),
		limitGetOutput: make(map[string]struct{}),
		inputIndex:     inputIndex,
		outputIndex:    outputIndex,
		nodesDone:      nodesDone,
		nodesErr:       make(map[string]error),
	}
	c.Context, c.cancel = context.WithCancel(ctx)
	return c, nil
}

// GetInput is used to get input channels by Node.
// returned map's key is the input slot name.
// this method can only call once by each Node.
func (ctx *Context) GetInput(node Node) map[string]<-chan *Artifact {
	name := node.Name()
	if ctx.alreadyGetInput(name) {
		panic("Context.GetInput can only be called once")
	}
	slots := node.Inputs()
	channels := make(map[string]<-chan *Artifact)
	for _, slot := range slots {
		key := name + "." + slot.Name
		ch := ctx.inputIndex[key]
		if ch == nil {
			panic(fmt.Sprintf("invalid node implement: \"%s\"", name))
		}
		channels[slot.Name] = ch
	}
	return channels
}

func (ctx *Context) alreadyGetInput(name string) bool {
	ctx.limitGetMu.Lock()
	defer ctx.limitGetMu.Unlock()
	if _, ok := ctx.limitGetInput[name]; ok {
		return true
	}
	ctx.limitGetInput[name] = struct{}{}
	return false
}

// GetOutput is used to get output channels by Node.
// returned map's key is the output slot name.
// this method can only call once by each Node.
func (ctx *Context) GetOutput(node Node) map[string]chan<- *Artifact {
	name := node.Name()
	if ctx.alreadyGetOutput(name) {
		panic("Context.GetOutput can only be called once")
	}
	slots := node.Outputs()
	channels := make(map[string]chan<- *Artifact)
	for _, slot := range slots {
		key := name + "." + slot.Name
		chs := ctx.outputIndex[key]
		if chs == nil {
			panic(fmt.Sprintf("invalid node implement: \"%s\"", name))
		}
		// Node only write Artifact to channel once
		proxy := make(chan *Artifact)
		go func() {
			var art *Artifact
			select {
			case art = <-proxy:
			case <-ctx.Context.Done():
				return
			}
			for _, ch := range chs {
				select {
				case ch <- art.Clone():
				case <-ctx.Context.Done():
					return
				}
			}
		}()
		channels[slot.Name] = proxy
	}
	return channels
}

func (ctx *Context) alreadyGetOutput(name string) bool {
	ctx.limitGetMu.Lock()
	defer ctx.limitGetMu.Unlock()
	if _, ok := ctx.limitGetOutput[name]; ok {
		return true
	}
	ctx.limitGetOutput[name] = struct{}{}
	return false
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
