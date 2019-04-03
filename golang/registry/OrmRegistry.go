package registry

import (
	"reflect"
)

type OrmRegistry struct {
	tables      map[string]*Table
	annotations map[string]*Annotation
}

func (o *OrmRegistry) Register(any interface{}) {
	value := reflect.ValueOf(any)
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() == reflect.Slice {

	}
	o.register(value.Type())
}

func (o *OrmRegistry) register(structType reflect.Type) {
	table := o.Table(structType.Name())
	if table != nil {
		return
	}
	table = &Table{}
	table.structType = structType
	table.ormRegistry = o
	o.tables[structType.Name()] = table
	table.inspect()
}

func (o *OrmRegistry) Table(name string) *Table {
	if o.tables == nil {
		o.tables = make(map[string]*Table)
	}
	return o.tables[name]
}
