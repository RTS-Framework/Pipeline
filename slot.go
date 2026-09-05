package pipeline

import (
	"fmt"
	"slices"
)

// InputSlot contains the input slot information.
type InputSlot struct {
	// each Name in the same node must be different.
	Name string `toml:"name" json:"name"`

	// Description contains this slot information.
	Description string `toml:"description" json:"description"`

	// this slot must be linked with another output slot.
	Required bool `toml:"required" json:"required"`

	// defines this slot accepted artifact type.
	Accepted []ArtifactType `toml:"accepted" json:"accepted"`
}

// OutputSlot contains the output slot information.
type OutputSlot struct {
	// each Name in the same node must be different.
	Name string `toml:"name" json:"name"`

	// Description contains this slot information.
	Description string `toml:"description" json:"description"`

	// defines this slot output artifact type.
	Type ArtifactType `toml:"type" json:"type"`
}

func isSlotTypeMatched(accepted []ArtifactType, typ ArtifactType) bool {
	return slices.Contains(accepted, typ)
}

// CheckNodeSlots is used to check this Node implement is valid.
func CheckNodeSlots(node Node) error {
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
