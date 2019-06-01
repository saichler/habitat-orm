package schema

import (
	"bytes"
	"reflect"
)

type TablePath struct {
	columnName string
	parent     *TablePath
	structType reflect.Type
}

func (tp *TablePath) Key() string {
	if tp.parent == nil {
		return tp.columnName
	}
	buff := bytes.Buffer{}
	buff.WriteString(tp.parent.Key())
	buff.WriteString(".")
	buff.WriteString(tp.columnName)
	return buff.String()
}

func (tp *TablePath) ColumnName() string {
	return tp.columnName
}

func (tp *TablePath) Parent() *TablePath {
	return tp.parent
}

func (tp *TablePath) Type() reflect.Type {
	return tp.structType
}

func (tp *TablePath) ValueOf(value reflect.Value) reflect.Value {
	if tp.parent == nil {
		return value
	}
	parentValue := tp.parent.ValueOf(value)
	if parentValue.Kind() == reflect.Ptr {
		if parentValue.IsNil() {
			return reflect.ValueOf(nil)
		} else {
			parentValue = parentValue.Elem()
		}
	}
	return parentValue.FieldByName(tp.columnName)
}

func (tp *TablePath) NewInstance() reflect.Value {
	return reflect.New(tp.structType)
}