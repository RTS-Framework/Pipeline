package pipeline

type Node interface {
	Name() string

	Description() string

	Inputs() []*InputSlot

	Outputs() []*OutputSlot

	Configure(any) error

	Init() error

	Execute(ctx *Context) error

	Close() error
}
