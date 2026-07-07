package pipeline

import (
	"os"
)

var (
	TypeShellcode = ArtifactType{
		Name:        "Shellcode",
		Description: "generic shellcode",
	}
	TypeRuntimeTemplate = ArtifactType{
		Name:        "RuntimeTemplate",
		Description: "Gleam-RT template",
	}
)

type ArtifactType struct {
	Name        string
	Description string
}

type Artifact interface {
	Value() (any, error)
	Type() ArtifactType
}

type ObjectArtifact struct {
	artifactType ArtifactType

	value any
}

func NewObjectArtifact(t ArtifactType, value any) *ObjectArtifact {
	return &ObjectArtifact{
		artifactType: t,
		value:        value,
	}
}

func (a *ObjectArtifact) Type() ArtifactType {
	return a.artifactType
}

func (a *ObjectArtifact) Value() (any, error) {
	return a.value, nil
}

type FileArtifact struct {
	artifactType ArtifactType

	path string
}

func NewFileArtifact(t ArtifactType, path string) *FileArtifact {
	return &FileArtifact{
		artifactType: t,
		path:         path,
	}
}

func (a *FileArtifact) Type() ArtifactType {
	return a.artifactType
}

func (a *FileArtifact) Path() string {
	return a.path
}

func (a *FileArtifact) Value() (any, error) {
	return os.ReadFile(a.path)
}
