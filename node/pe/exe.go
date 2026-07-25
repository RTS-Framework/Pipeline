package pe

import (
	"github.com/RTS-Framework/Pipeline"
)

var TypeEXEImage = pipeline.ArtifactType{
	Name:        "EXE-Image",
	Description: "exe image file",
}

func init() {
	err := pipeline.RegisterArtifactTypes(TypeEXEImage)
	if err != nil {
		panic(err)
	}
}
