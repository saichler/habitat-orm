package conditions

import (
	"bytes"
	"errors"
	. "github.com/saichler/habitat-orm/golang/query"
	. "github.com/saichler/habitat-orm/golang/registry/comparators"
	. "github.com/saichler/habitat-orm/golang/registry/schema"
	"reflect"
)

type OrmCondition struct {
	comparator *OrmComparator
	op         ConditionOperation
	next       *OrmCondition
}

func (cond *OrmCondition) String() string {
	buff := &bytes.Buffer{}
	buff.WriteString("(")
	cond.toString(buff)
	buff.WriteString(")")
	return buff.String()
}

func (cond *OrmCondition) toString(buff *bytes.Buffer) {
	if cond.comparator != nil {
		buff.WriteString(cond.comparator.String())
	}
	if cond.next != nil {
		buff.WriteString(string(cond.op))
		cond.next.toString(buff)
	}
}

func CreateCondition(schema *Schema, mainTable *TablePath, c *Condition) (*OrmCondition, error) {
	ormCond := &OrmCondition{}
	ormCond.op = c.Operation()
	comp, e := CreateComparator(schema, mainTable, c.Comparator())
	if e != nil {
		return nil, e
	}
	ormCond.comparator = comp
	if c.Next() != nil {
		next, e := CreateCondition(schema, mainTable, c.Next())
		if e != nil {
			return nil, e
		}
		ormCond.next = next
	}
	return ormCond, nil
}

func (ormCond *OrmCondition) Match(value reflect.Value) (bool, error) {
	comp, e := ormCond.comparator.Match(value)
	if e != nil {
		return false, e
	}
	next := true
	if ormCond.op == Or {
		next = false
	}
	if ormCond.next != nil {
		next, e = ormCond.next.Match(value)
		if e != nil {
			return false, e
		}
	}
	if ormCond.op == "" {
		return next && comp, nil
	}
	if ormCond.op == And {
		return comp && next, nil
	}
	if ormCond.op == Or {
		return comp || next, nil
	}
	return false, errors.New("Unsupported operation in match:" + string(ormCond.op))
}
