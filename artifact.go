package pipeline

import (
	"fmt"
	"strings"
	"sync"
)

// Artifact contains the output artifact information.
type Artifact struct {
	Name string
	Data any
	Type ArtifactType
}

// Clone is used to clone the Artifact structure.
func (art *Artifact) Clone() *Artifact {
	clone := *art
	return &clone
}

// ArtifactType defines the artifact type information.
type ArtifactType struct {
	Name        string
	Description string
}

var (
	artifactTypes   map[string]ArtifactType
	artifactTypesMu sync.Mutex
)

func init() {
	artifactTypes = make(map[string]ArtifactType, 64)
}

// RegisterArtifactTypes is used to register the artifact type.
func RegisterArtifactTypes(types ...ArtifactType) error {
	artifactTypesMu.Lock()
	defer artifactTypesMu.Unlock()
	for _, typ := range types {
		key := strings.ToLower(typ.Name)
		if _, ok := artifactTypes[key]; ok {
			return fmt.Errorf("artifact type %s already exists", typ.Name)
		}
		artifactTypes[key] = typ
	}
	return nil
}
