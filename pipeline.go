package pipeline

type Pipeline struct {
	nodes []*node
	links []*link
}

type node struct {
	node Node
	
	inputs map[string][]Artifact
	
	outputs map[string][]Artifact
	
	done bool
	
	err error
}

type link struct {
	SrcNode Node
	SrcSlot *OutputSlot
	
	DstNode Node
	DstSlot *InputSlot
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Add() error {
	return nil
}

func (p *Pipeline) Link() error {
	return nil
}

func (p *Pipeline) Validate() error {
	return nil
}

func (p *Pipeline) Run(ctx *Context) error {
	return nil
}

func (p *Pipeline) Interrupt() error {
	return nil
}

func (p *Pipeline) Close() error {
	return nil
}
