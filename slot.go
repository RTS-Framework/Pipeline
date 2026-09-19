package pipeline

import (
	"fmt"
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

	// this slot must be linked with another input slot.
	Required bool `toml:"required" json:"required"`

	// defines this slot output artifact type.
	Type ArtifactType `toml:"type" json:"type"`
}

func isSlotTypeMatched(accepted []ArtifactType, typ ArtifactType) bool {
	key := typ.key()
	for _, item := range accepted {
		if item.key() == key {
			return true
		}
	}
	return false
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
		if len(slot.Accepted) == 0 {
			return fmt.Errorf("input slot \"%s\" with empty accepted artifact type", slot.Name)
		}
		for i := 0; i < len(slot.Accepted); i++ {
			accepted := slot.Accepted[i]
			if !LookupArtifactType(accepted) {
				return fmt.Errorf("artifact type \"%s\" is not registered", accepted.Name)
			}
		}
		iNames[slot.Name] = struct{}{}
	}
	outputs := node.Outputs()
	oNames := make(map[string]struct{}, len(outputs))
	for _, slot := range outputs {
		if _, ok := oNames[slot.Name]; ok {
			return fmt.Errorf("duplicate output slot name: \"%s\"", slot.Name)
		}
		if !LookupArtifactType(slot.Type) {
			return fmt.Errorf("artifact type \"%s\" is not registered", slot.Type.Name)
		}
		oNames[slot.Name] = struct{}{}
	}
	return nil
}
