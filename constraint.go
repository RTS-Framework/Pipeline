package pipeline

import (
	"go/build/constraint"
	"runtime"
	"strings"
)

func matchBuildConstraint(line string) (bool, error) {
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
