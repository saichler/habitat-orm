package registry

import (
	. "github.com/saichler/habitat-orm/golang/registry/schema"
	"reflect"
)

type OrmRegistry struct {
	tables      map[string]*Table
	annotations map[string]*Annotation
	schema      *Schema
}

func (o *OrmRegistry) Register(any interface{}) {
	if o.schema == nil {
		o.schema = NewSchema()
	}
	value := reflect.ValueOf(any)
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() == reflect.Slice {

	}
	tp := o.schema.RegiaterTablePath("", value.Type(), nil)
	o.register(value.Type(), tp)
}

func (o *OrmRegistry) register(structType reflect.Type, path *TablePath) {
	table := o.Table(structType.Name())
	if table != nil {
		return
	}
	table = &Table{}
	table.structType = structType
	table.ormRegistry = o
	o.tables[structType.Name()] = table
	table.inspect(path)
}

func (o *OrmRegistry) Table(name string) *Table {
	if o.tables == nil {
		o.tables = make(map[string]*Table)
	}
	return o.tables[name]
}

func (o *OrmRegistry) Tables() map[string]*Table {
	return o.tables
}

func (o *OrmRegistry) Schema() *Schema {
	return o.schema
}
