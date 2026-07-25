package pipeline

import (
	"fmt"
)

// Link is a link a Node output slot to another Node input slot.
type Link struct {
	path string

	srcNode Node
	srcSlot *OutputSlot

	dstNode Node
	dstSlot *InputSlot
}

func (l *Link) String() string {
	return l.path
}

func buildLinkPath(srcNode, srcSlot, dstNode, dstSlot string) string {
	return fmt.Sprintf("[%s.%s] -> [%s.%s]", srcNode, srcSlot, dstNode, dstSlot)
}
