package pe

import (
	"github.com/RTS-Framework/Pipeline"
)

var TypeDLLImage = pipeline.ArtifactType{
	Name:        "DLL-Image",
	Description: "dll image file",
}

func init() {
	err := pipeline.RegisterArtifactTypes(TypeDLLImage)
	if err != nil {
		panic(err)
	}
}
