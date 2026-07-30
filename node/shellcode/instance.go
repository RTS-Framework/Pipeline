package shellcode

import (
	"github.com/RTS-Framework/Pipeline"
)

var TypeShellcodeInstance = pipeline.ArtifactType{
	Name:        "Shellcode",
	Description: "generic shellcode instance",
}

func init() {
	err := pipeline.RegisterArtifactTypes(TypeShellcodeInstance)
	if err != nil {
		panic(err)
	}
}
