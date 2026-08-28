package pipeline

import (
	"errors"
	"fmt"
	"reflect"
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
	Name        string `toml:"name"        json:"name"`
	Description string `toml:"description" json:"description"`
}

var (
	artifactTypes    map[string]reflect.Type
	artifactTypesRWM sync.RWMutex
)

func init() {
	artifactTypes = make(map[string]reflect.Type, 64)
}

// RegisterArtifactType is used to register the artifact type.
func RegisterArtifactType(typ ArtifactType, reference any) error {
	artifactTypesRWM.Lock()
	defer artifactTypesRWM.Unlock()
	key := strings.ToLower(typ.Name)
	if _, ok := artifactTypes[key]; ok {
		return fmt.Errorf("artifact type %s already exists", typ.Name)
	}
	artifactTypes[key] = reflect.TypeOf(reference)
	return nil
}

// CheckArtifactData is used to check artifact data type is expected.
func CheckArtifactData(name string, data any) error {
	artifactTypesRWM.RLock()
	defer artifactTypesRWM.RUnlock()
	key := strings.ToLower(name)
	typ, ok := artifactTypes[key]
	if !ok {
		return fmt.Errorf("artifact type %s does not exist", name)
	}
	if reflect.TypeOf(data) != typ {
		return errors.New("unexpected artifact data type")
	}
	return nil
}
