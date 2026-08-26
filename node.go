package pipeline

import (
	"fmt"
)

// Node is the processing unit of a Pipeline. Each Node declares its
// input/output Slots and implements the execution logic that transforms
// input Artifacts into output Artifacts.
//
// Runtime contract (violations are reported as node errors):
//
//   - All nodes of one Execute start at the same time; never assume an
//     execution order between nodes.
//
//   - Read every input slot through ctx.ReadInput exactly once. An
//     optional slot that is not linked returns (nil, nil) and must be
//     skipped; a second read is an error.
//
//   - Write every linked output slot through ctx.WriteOutput exactly
//     once before returning nil. Missing or duplicate writes are
//     reported as node errors.
//
//   - Blocking reads must select on ctx.Done() so a failed node does
//     not leave other nodes waiting forever. ctx.ReadInput already
//     handles cancellation internally.
//
//   - Node.Execute must be safe for concurrent use by multiple
//     goroutines and must be reentrant: the same instance can be
//     executed by multiple Pipeline.Execute calls.
type Node interface {
	// Name returns a unique identifier for this Node within the Pipeline.
	Name() string

	// Type returns the Node's type name like "PE Image Loader".
	Type() string

	// Description returns a human-readable summary of this Node's purpose.
	Description() string

	// BuildTag is used to define the Node require environment like
	// "//go:build windows && amd64" or "windows && amd64".
	// If current environment is not matched, Node can still execute,
	// but some functions will be limited or skipped.
	BuildTag() string

	// Inputs returns the list of input Slots this Node expects.
	// Called at build time for link validation.
	Inputs() []*InputSlot

	// Outputs returns the list of output Slots this Node produces.
	// Called at build time for link validation.
	Outputs() []*OutputSlot

	// Initialize is used to initialize the Node,
	// it will be called once when added to the Pipeline.
	Initialize() error

	// Execute is used to execute the Node's processing logic.
	// it will be called when the Pipeline Executes.
	// it must be safe for concurrent use by multiple goroutines.
	Execute(ctx Context) error

	// Close is used to release the resources held by this Node,
	// it will be called once when this Node be removed or Pipeline Close.
	Close() error
}

// CheckNode is used to check this Node implement is valid.
func CheckNode(node Node) error {
	// check the same input/output slot name
	inputs := node.Inputs()
	iNames := make(map[string]struct{}, len(inputs))
	for _, slot := range inputs {
		if _, ok := iNames[slot.Name]; ok {
			return fmt.Errorf("duplicate input slot name: \"%s\"", slot.Name)
		}
		iNames[slot.Name] = struct{}{}
	}
	outputs := node.Outputs()
	oNames := make(map[string]struct{}, len(outputs))
	for _, slot := range outputs {
		if _, ok := oNames[slot.Name]; ok {
			return fmt.Errorf("duplicate output slot name: \"%s\"", slot.Name)
		}
		oNames[slot.Name] = struct{}{}
	}
	return nil
}

func getNodeInputSlot(node Node, name string) (*InputSlot, error) {
	slots := node.Inputs()
	for _, slot := range slots {
		if slot.Name == name {
			return slot, nil
		}
	}
	return nil, fmt.Errorf("input slot \"%s\" is not found", name)
}

func getNodeOutputSlot(node Node, name string) (*OutputSlot, error) {
	slots := node.Outputs()
	for _, slot := range slots {
		if slot.Name == name {
			return slot, nil
		}
	}
	return nil, fmt.Errorf("output slot \"%s\" is not found", name)
}
