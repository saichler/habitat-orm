package comparators

import (
	"bytes"
	"errors"
	. "github.com/saichler/habitat-orm/golang/query"
	. "github.com/saichler/habitat-orm/golang/registry/schema"
	"reflect"
)

type OrmComparator struct {
	left            string
	leftColumnPath  *ColumnPath
	op              ComparatorOperation
	right           string
	rightColumnPath *ColumnPath
}

type Comparable interface {
	Compare([]reflect.Value, []reflect.Value) bool
}

var comparables = make(map[ComparatorOperation]Comparable)

func initComparables() {
	if len(comparables) == 0 {
		comparables[Eq] = NewEqual()
	}
}

func (comp *OrmComparator) String() string {
	buff := bytes.Buffer{}
	if comp.leftColumnPath != nil {
		buff.WriteString(comp.leftColumnPath.Key())
	} else {
		buff.WriteString(comp.left)
	}
	buff.WriteString(string(comp.op))
	if comp.rightColumnPath != nil {
		buff.WriteString(comp.rightColumnPath.Key())
	} else {
		buff.WriteString(comp.right)
	}
	return buff.String()
}

func CreateComparator(schema *Schema, mainTable *TablePath, c *Comparator) (*OrmComparator, error) {
	initComparables()
	ormComp := &OrmComparator{}
	ormComp.op = c.Operation()
	ormComp.left = c.Left()
	ormComp.right = c.Right()

	ormComp.leftColumnPath = schema.CreateColumnPath(mainTable, ormComp.left)
	ormComp.rightColumnPath = schema.CreateColumnPath(mainTable, ormComp.right)

	if ormComp.leftColumnPath == nil && ormComp.rightColumnPath == nil {
		return nil, errors.New("No Column was found for comparator:" + c.String())
	}
	return ormComp, nil
}

func (ormComp *OrmComparator) Match(value reflect.Value) (bool, error) {
	var leftValue []reflect.Value
	var rightValue []reflect.Value
	if ormComp.leftColumnPath != nil {
		leftValue = ormComp.leftColumnPath.ValueOf(value)
	} else {
		leftValue = []reflect.Value{reflect.ValueOf(ormComp.left)}
	}
	if ormComp.rightColumnPath != nil {
		rightValue = ormComp.rightColumnPath.ValueOf(value)
	} else {
		rightValue = []reflect.Value{reflect.ValueOf(ormComp.right)}
	}
	matcher := comparables[ormComp.op]
	if matcher == nil {
		panic("No Matcher for: " + ormComp.op + " operation.")
	}
	return matcher.Compare(leftValue, rightValue), nil
}
