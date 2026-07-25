package node

import (
	"github.com/RTS-Framework/Pipeline"
)

var (
	TypeGRTPELoaderTemplate = pipeline.ArtifactType{
		Name:        "GRT-PELoader template",
		Description: "GRT-PELoader template",
	}
)

func init() {
	types := []pipeline.ArtifactType{
		TypeGRTPELoaderTemplate,
	}
	err := pipeline.RegisterArtifactTypes(types...)
	if err != nil {
		panic(err)
	}
}
