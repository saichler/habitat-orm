package query

import (
	"bytes"
	"errors"
	"strings"
)

type ConditionOperation string

const (
	And                 ConditionOperation = " and "
	Or                  ConditionOperation = " or "
	MAX_EXPRESSION_SIZE                    = 999999
)

type Condition struct {
	comparator *Comparator
	op         ConditionOperation
	next       *Condition
}

func (c *Condition) Comparator() *Comparator {
	return c.comparator
}

func (c *Condition) Operation() ConditionOperation {
	return c.op
}

func (c *Condition) Next() *Condition {
	return c.next
}

func (c *Condition) String() string {
	buff := &bytes.Buffer{}
	buff.WriteString("(")
	c.toString(buff)
	buff.WriteString(")")
	return buff.String()
}

func (c *Condition) toString(buff *bytes.Buffer) {
	if c.comparator != nil {
		buff.WriteString(c.comparator.String())
	}
	if c.next != nil {
		buff.WriteString(string(c.op))
		c.next.toString(buff)
	}
}

func NewCondition(ws string) (*Condition, error) {
	loc := MAX_EXPRESSION_SIZE
	var op ConditionOperation
	and := strings.Index(ws, string(And))
	if and != -1 {
		loc = and
		op = And
	}
	or := strings.Index(ws, string(Or))
	if or != -1 && or < loc {
		loc = or
		op = Or
	}

	condition := &Condition{}
	if loc == MAX_EXPRESSION_SIZE {
		cmpr, e := NewCompare(ws)
		if e != nil {
			return nil, e
		}
		condition.comparator = cmpr
		return condition, nil
	}

	cmpr, e := NewCompare(ws[0:loc])
	if e != nil {
		panic(ws)
		return nil, e
	}

	condition.comparator = cmpr
	condition.op = op

	ws = ws[loc+len(op):]
	next, e := NewCondition(ws)
	if e != nil {
		return nil, e
	}

	condition.next = next
	return condition, nil
}

func getLastConditionOp(ws string) (ConditionOperation, int, error) {
	loc := -1
	var op ConditionOperation

	and := strings.LastIndex(ws, string(And))
	if and > loc {
		op = And
		loc = and
	}

	or := strings.LastIndex(ws, string(Or))
	if or > loc {
		op = Or
		loc = or
	}

	if loc == -1 {
		return "", 0, errors.New("No last condition was found.")
	}
	return op, loc, nil
}

func getFirstConditionOp(ws string) (ConditionOperation, int, error) {
	loc := MAX_EXPRESSION_SIZE
	var op ConditionOperation
	and := strings.Index(ws, string(And))
	if and != -1 {
		loc = and
		op = And
	}
	or := strings.Index(ws, string(Or))
	if or != -1 && or < loc {
		loc = or
		op = Or
	}

	if loc == MAX_EXPRESSION_SIZE {
		return "", 0, errors.New("No first condition was found.")
	}

	return op, loc, nil
}
