package query

import (
	"bytes"
	"errors"
	"strings"
)

type ComparatorOperation string

const (
	Eq   ComparatorOperation = "="
	Neq  ComparatorOperation = "!="
	GT   ComparatorOperation = ">"
	LT   ComparatorOperation = "<"
	GTEQ ComparatorOperation = ">="
	LTEQ ComparatorOperation = "<="
)

var comparators = make([]ComparatorOperation, 0)

type Comparator struct {
	left  string
	op    ComparatorOperation
	right string
}

func (c *Comparator) Left() string {
	return c.left
}

func (c *Comparator) Right() string {
	return c.right
}

func (c *Comparator) Operation() ComparatorOperation {
	return c.op
}

func initComparators() {
	if len(comparators) == 0 {
		comparators = append(comparators, GTEQ)
		comparators = append(comparators, LTEQ)
		comparators = append(comparators, Neq)
		comparators = append(comparators, Eq)
		comparators = append(comparators, GT)
		comparators = append(comparators, LT)
	}
}

func (c *Comparator) String() string {
	buff := bytes.Buffer{}
	buff.WriteString(c.left)
	buff.WriteString(string(c.op))
	buff.WriteString(c.right)
	return buff.String()
}

func NewCompare(ws string) (*Comparator, error) {
	for _, op := range comparators {
		loc := strings.Index(ws, string(op))
		if loc != -1 {
			cmp := &Comparator{}
			cmp.left = strings.TrimSpace(strings.ToLower(ws[0:loc]))
			cmp.right = strings.TrimSpace(strings.ToLower(ws[loc+len(op):]))
			cmp.op = op
			return cmp, nil
		}
	}
	return nil, errors.New("Cannot find comparator operation in: " + ws)
}
