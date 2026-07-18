package pipeline

import (
	"fmt"
	"strings"
)

// Artifact contains the output artifact information.
type Artifact struct {
	Name string
	Data []byte
	Type ArtifactType
}

// ArtifactType defines the artifact type information.
type ArtifactType struct {
	Name        string
	Description string
}

// define the built-in artifact type.
var (
	TypeEXEImage = ArtifactType{
		Name:        "EXE-Image",
		Description: "exe image file",
	}
	TypeDLLImage = ArtifactType{
		Name:        "DLL-Image",
		Description: "dll image file",
	}
	TypeShellcode = ArtifactType{
		Name:        "Shellcode",
		Description: "generic shellcode",
	}
	TypeRuntime = ArtifactType{
		Name:        "Runtime",
		Description: "Gleam-RT template",
	}
	TypePELoader = ArtifactType{
		Name:        "PELoader",
		Description: "PE Loader template",
	}
)

var regArtifactTypes map[string]ArtifactType

func init() {
	regArtifactTypes = make(map[string]ArtifactType, 16)
	regArtifactTypes[TypeEXEImage.Name] = TypeEXEImage
	regArtifactTypes[TypeDLLImage.Name] = TypeDLLImage
	regArtifactTypes[TypeShellcode.Name] = TypeShellcode
	regArtifactTypes[TypeRuntime.Name] = TypeRuntime
	regArtifactTypes[TypePELoader.Name] = TypePELoader
}

// RegisterArtifactType is used to register the artifact type.
func RegisterArtifactType(typ ArtifactType) error {
	for name := range regArtifactTypes {
		if strings.ToLower(name) == strings.ToLower(typ.Name) {
			return fmt.Errorf("artifact type %s already exists", typ.Name)
		}
	}
	regArtifactTypes[typ.Name] = typ
	return nil
}
