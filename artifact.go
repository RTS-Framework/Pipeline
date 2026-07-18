package pipeline

import (
	"os"
)

type Artifact interface {
	Value() any
	Type() ArtifactType
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

type MemoryArtifact struct {
	typ ArtifactType
	val any
}

func NewMemoryArtifact(typ ArtifactType, val any) *MemoryArtifact {
	return &MemoryArtifact{typ: typ, val: val}
}

func (a *MemoryArtifact) Type() ArtifactType {
	return a.typ
}

func (a *MemoryArtifact) Value() any {
	return a.val
}

type FileArtifact struct {
	typ  ArtifactType
	data []byte
}

func NewFileArtifact(typ ArtifactType, path string) (*FileArtifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	art := FileArtifact{
		typ:  typ,
		data: data,
	}
	return &art, nil
}

func (a *FileArtifact) Type() ArtifactType {
	return a.typ
}

func (a *FileArtifact) Value() any {
	return a.data
}
