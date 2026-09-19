package pe

import (
	"github.com/RTS-Framework/Pipeline"
)

var TypeDLLImage = pipeline.ArtifactType{
	Name:        "DLL-Image",
	Description: "dll image file",
}

func init() {
	err := pipeline.RegisterArtifactType(TypeDLLImage, []byte{})
	if err != nil {
		panic(err)
	}
}
