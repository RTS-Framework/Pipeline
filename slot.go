package pipeline

import (
	"slices"
)

// InputSlot contains the input slot information.
type InputSlot struct {
	// each Name in the same node must be different.
	Name string

	// Description contains this slot information.
	Description string

	// this slot must be linked with another output slot.
	Required bool

	// defines this slot accepted artifact type.
	Accepted []ArtifactType
}

// OutputSlot contains the output slot information.
type OutputSlot struct {
	// each Name in the same node must be different.
	Name string

	// Description contains this slot information.
	Description string

	// defines this slot output artifact type.
	Type ArtifactType
}

func isSlotTypeMatched(accepted []ArtifactType, typ ArtifactType) bool {
	return slices.Contains(accepted, typ)
}
