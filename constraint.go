package pipeline

import (
	"fmt"
	"go/build/constraint"
	"runtime"
	"strings"
)

// MatchBuildTag is used to check build tag is match current environment.
func MatchBuildTag(line string) (bool, error) {
	if line == "" {
		return true, nil
	}
	if !strings.HasPrefix(line, "//go:build ") {
		line = "//go:build " + line
	}
	expr, err := constraint.Parse(line)
	if err != nil {
		return false, err
	}
	return expr.Eval(matchBuildTag), nil
}

func matchBuildTag(tag string) bool {
	switch tag {
	case runtime.GOOS, runtime.GOARCH:
		return true
	}
	return false
}

// CheckNodeBuildTag is used to check build tag is match current environment.
func CheckNodeBuildTag(node Node) error {
	buildTag := node.BuildTag()
	ok, err := MatchBuildTag(buildTag)
	if err != nil {
		return err
	}
	if !ok {
		format := "build tag: \"%s\" is not match current environment"
		return fmt.Errorf(format, buildTag)
	}
	return nil
}
