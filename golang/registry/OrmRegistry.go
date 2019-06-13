package registry

import (
	. "github.com/saichler/hql-schema/golang"
	"reflect"
)

type OrmRegistry struct {
	tables      map[string]*Table
	tablesList  []string
	annotations map[string]*Annotation
	schema      *Schema
}

func (o *OrmRegistry) Register(any interface{}, parent *SchemaNode) {
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
	shcemaNode := o.schema.RegisterNode("", parent, value.Type())
	o.register(value.Type(), shcemaNode)
}

func (o *OrmRegistry) register(structType reflect.Type, schemaNode *SchemaNode) {
	table := o.Table(structType.Name())
	if table != nil {
		return
	}
	table = &Table{}
	table.structType = structType
	table.ormRegistry = o
	o.tables[structType.Name()] = table
	table.inspect(schemaNode)
}

func (o *OrmRegistry) Table(name string) *Table {
	if o.tables == nil {
		o.tables = make(map[string]*Table)
	}
	return o.tables[name]
}

func (o *OrmRegistry) TablesMap() map[string]*Table {
	return o.tables
}

func (o *OrmRegistry) Tables() []string {
	if o.tablesList == nil || len(o.tablesList) != len(o.tables) {
		o.tablesList = make([]string, 0)
		for tn, _ := range o.tables {
			o.tablesList = append(o.tablesList, tn)
		}
	}
	return o.tablesList
}

func (o *OrmRegistry) Schema() *Schema {
	return o.schema
}
