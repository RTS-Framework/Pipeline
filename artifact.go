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
	Name string       `toml:"name" json:"name"`
	Data any          `toml:"data" json:"data"`
	Type ArtifactType `toml:"type" json:"type"`
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

func (at *ArtifactType) key() string {
	return strings.ToLower(at.Name)
}

type artifactType struct {
	Artifact ArtifactType
	GoType   reflect.Type
}

var (
	artifactTypes    map[string]*artifactType
	artifactTypesRWM sync.RWMutex
)

func init() {
	artifactTypes = make(map[string]*artifactType, 32)
}

// RegisterArtifactType is used to register the artifact type.
func RegisterArtifactType(typ ArtifactType, reference any) error {
	if typ.Name == "" {
		return errors.New("artifact type name can not be empty")
	}
	if reference == nil {
		return errors.New("artifact reference can not be nil")
	}
	artifactTypesRWM.Lock()
	defer artifactTypesRWM.Unlock()
	if _, ok := artifactTypes[typ.key()]; ok {
		return fmt.Errorf("artifact type %s already exists", typ.Name)
	}
	artifactTypes[typ.key()] = &artifactType{
		Artifact: typ,
		GoType:   reflect.TypeOf(reference),
	}
	return nil
}

// LookupArtifactType reports whether the artifact type has been registered.
// The type is matched by Name only, in a case-insensitive way.
func LookupArtifactType(typ ArtifactType) bool {
	artifactTypesRWM.RLock()
	defer artifactTypesRWM.RUnlock()
	_, ok := artifactTypes[typ.key()]
	return ok
}

// CheckArtifactData is used to check artifact data type is expected.
//
// The type is matched by Name only: Description never takes part in the
// matching, and the name is compared in a case-insensitive way, same as
// RegisterArtifactType and LookupArtifactType.
//
// The data check is an exact Go type match, not an assignability or
// interface check: a defined type such as "type Raw []byte" does not match
// []byte, and data never matches a registered interface type. A nil data is
// reported as a mismatch, while a typed nil pointer such as (*T)(nil)
// matches its type and passes.
func CheckArtifactData(typ ArtifactType, data any) error {
	artifactTypesRWM.RLock()
	defer artifactTypesRWM.RUnlock()
	at, ok := artifactTypes[typ.key()]
	if !ok {
		return fmt.Errorf("artifact type %s does not exist", typ.Name)
	}
	if reflect.TypeOf(data) != at.GoType {
		return errors.New("unexpected artifact data type")
	}
	return nil
}
