package pipeline

type InputSlot struct {
	ID string

	Name string

	Accept []ArtifactType
}

type OutputSlot struct {
	ID string

	Name string

	Type ArtifactType
}
