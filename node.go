package pipeline

// Node is the processing unit of a Pipeline. Each Node declares its input/output
// Slots and implements the execution logic that transforms input Artifacts into
// output Artifacts.
type Node interface {
	// Name returns a unique identifier for this Node within the Pipeline.
	Name() string

	// Description returns a human-readable summary of this Node's purpose.
	Description() string

	// BuildTag is used to define the Node require environment like
	// "//go:build windows && amd64" or "windows && amd64".
	// If current environment is not matched, Node can still execute,
	// but some functions will be limited or skipped.
	BuildTag() string

	// Inputs returns the list of input Slots this Node expects.
	// Called at build time for link validation; must be deterministic.
	Inputs() []*InputSlot

	// Outputs returns the list of output Slots this Node produces.
	// Called at build time for link validation; must be deterministic.
	Outputs() []*OutputSlot

	// Initialize is used to initialize the Node,
	// it will be called once when added to the pipeline.
	Initialize() error

	// Execute is used to execute the Node's processing logic,
	// it will be called when the pipeline Execute.
	Execute(ctx *Context) error

	// Close is used to release the resources held by this Node,
	// it will be called once when this Node be removed or Pipeline Close.
	Close() error
}
