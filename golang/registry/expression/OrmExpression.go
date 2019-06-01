package expression

import (
	"bytes"
	"errors"
	. "github.com/saichler/habitat-orm/golang/query"
	. "github.com/saichler/habitat-orm/golang/registry/conditions"
	. "github.com/saichler/habitat-orm/golang/registry/schema"
	"reflect"
)

type OrmExpression struct {
	condition *OrmCondition
	op        ConditionOperation
	next      *OrmExpression
	child     *OrmExpression
}

func (ormExpr *OrmExpression) String() string {
	buff := bytes.Buffer{}
	if ormExpr.condition != nil {
		buff.WriteString(ormExpr.condition.String())
	} else {
		buff.WriteString("(")
	}
	if ormExpr.child != nil {
		buff.WriteString(ormExpr.child.String())
	}
	if ormExpr.condition == nil {
		buff.WriteString(")")
	}
	if ormExpr.next != nil {
		buff.WriteString(string(ormExpr.op))
		buff.WriteString(ormExpr.next.String())
	}
	return buff.String()
}

func CreateExpression(schema *Schema, mainTable *TablePath, expr *Expression) (*OrmExpression, error) {
	ormExpr := &OrmExpression{}
	ormExpr.op = expr.Operation()
	if expr.Condition() != nil {
		cond, e := CreateCondition(schema, mainTable, expr.Condition())
		if e != nil {
			return nil, e
		}
		ormExpr.condition = cond
	}

	if expr.Child() != nil {
		child, e := CreateExpression(schema, mainTable, expr.Child())
		if e != nil {
			return nil, e
		}
		ormExpr.child = child
	}

	if expr.Next() != nil {
		next, e := CreateExpression(schema, mainTable, expr.Next())
		if e != nil {
			return nil, e
		}
		ormExpr.next = next
	}

	return ormExpr, nil
}

func (ormExpr *OrmExpression) Match(value reflect.Value) (bool, error) {
	cond := true
	child := true
	next := true
	var e error
	if ormExpr.op == Or {
		cond = false
		child = false
		next = false
	}
	if ormExpr.condition != nil {
		cond, e = ormExpr.condition.Match(value)
		if e != nil {
			return false, e
		}
	}
	if ormExpr.child != nil {
		child, e = ormExpr.child.Match(value)
		if e != nil {
			return false, e
		}
	}
	if ormExpr.next != nil {
		next, e = ormExpr.next.Match(value)
		if e != nil {
			return false, e
		}
	}
	if ormExpr.op == "" {
		return child && next && cond, nil
	}
	if ormExpr.op == And {
		return child && next && cond, nil
	}
	if ormExpr.op == Or {
		return child || next || cond, nil
	}

	return false, errors.New("Unsupported operation in match:" + string(ormExpr.op))
}
