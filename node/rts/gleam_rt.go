package node

import (
	"github.com/RTS-Framework/Pipeline"
)

var (
	TypeGleamRTTemplate = pipeline.ArtifactType{
		Name:        "Gleam-RT template",
		Description: "Gleam-RT template",
	}
	TypeGleamRTInstance = pipeline.ArtifactType{
		Name:        "Gleam-RT instance",
		Description: "Gleam-RT instance",
	}
)

func init() {
	types := []pipeline.ArtifactType{
		TypeGleamRTTemplate,
		TypeGleamRTInstance,
	}
	err := pipeline.RegisterArtifactTypes(types...)
	if err != nil {
		panic(err)
	}
}
