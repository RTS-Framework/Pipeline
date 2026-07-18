package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Context struct {
	logger *logger

	// key is the link path
	linkChs map[string]chan *Artifact

	// key is "nodeName.slotName"
	inputIndex  map[string]<-chan *Artifact
	outputIndex map[string][]chan<- *Artifact

	// node running status
	// key is node name
	nodesDone map[string]chan struct{}
	nodesErr  map[string]error
	nodesMu   sync.Mutex

	context context.Context
	cancel  context.CancelFunc
}

func (p *Pipeline) newContext(ctx context.Context, opts *Options) (*Context, error) {
	// prepare the logger
	logDir := opts.LogDirectory
	if logDir == "" {
		logDir = "logs"
	}
	logID := atomic.AddInt64(&p.counter, 1)
	logName := fmt.Sprintf("pipeline-%d-%04d.log", time.Now().UnixNano(), logID)
	logPath := filepath.Join(logDir, logName)
	logger, err := newLogger(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %s", err)
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
		linkChs:     linkChs,
		inputIndex:  inputIndex,
		outputIndex: outputIndex,
		nodesDone:   nodesDone,
		nodesErr:    make(map[string]error),
	}
	c.context, c.cancel = context.WithCancel(ctx)
	return c, nil
}

// GetInput is used to get input channels by Node.
// returned map's key is the input slot name.
func (ctx *Context) GetInput(node Node) map[string]<-chan *Artifact {
	name := node.Name()
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

// GetOutput is used to get output channels by Node.
// returned map's key is the output slot name.
func (ctx *Context) GetOutput(node Node) map[string][]chan<- *Artifact {
	name := node.Name()
	slots := node.Outputs()
	channels := make(map[string][]chan<- *Artifact)
	for _, slot := range slots {
		key := name + "." + slot.Name
		chs := ctx.outputIndex[key]
		if chs != nil {
			panic(fmt.Sprintf("invalid node implement: \"%s\"", name))
		}
		channels[slot.Name] = chs
	}
	return channels
}

func (ctx *Context) Logger() Logger {
	return ctx.logger
}

func (ctx *Context) Done() <-chan struct{} {
	return ctx.context.Done()
}

func (ctx *Context) Err() error {
	return ctx.context.Err()
}

func (ctx *Context) Interrupt() {
	ctx.cancel()
}
