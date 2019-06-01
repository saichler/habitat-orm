package schema

import "reflect"

type ColumnPath struct {
	columnName string
	parent     *TablePath
}

func (cp *ColumnPath) Key() string {
	return cp.parent.Key() + "." + cp.Key()
}

func (cp *ColumnPath) ValueOf(value reflect.Value) []reflect.Value {
	myTableValue := cp.parent.ValueOf(value)
	if myTableValue.Kind() == reflect.Ptr {
		if myTableValue.IsNil() {
			return []reflect.Value{reflect.ValueOf(nil)}
		} else {
			myTableValue = myTableValue.Elem()
		}
	}
	if myTableValue.Kind() == reflect.Slice {
		result := make([]reflect.Value, myTableValue.Len())
		for i := 0; i < myTableValue.Len(); i++ {
			elem := myTableValue.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			result[i] = elem.FieldByName(cp.columnName)
		}
		return result
	} else {
		return []reflect.Value{myTableValue.FieldByName(cp.columnName)}
	}
}
