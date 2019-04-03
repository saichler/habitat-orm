package registry

import "reflect"

type Table struct {
	ormRegistry *OrmRegistry
	structType  reflect.Type
	columns     map[string]*Column
	indexes     *Indexes
}

func (t *Table) inspect() {
	if t.columns == nil {
		t.columns = make(map[string]*Column)
	}
	if t.indexes == nil {
		t.indexes = &Indexes{}
	}
	for i := 0; i < t.structType.NumField(); i++ {
		field := t.structType.Field(i)
		c := t.columns[field.Name]
		if c == nil {
			c = &Column{}
			c.field = field
			c.table = t
			t.columns[field.Name] = c
			c.inspect()
			t.indexes.AddColumn(c)
		}
	}
}

func (t *Table) Columns() map[string]*Column {
	return t.columns
}

func (t *Table) Indexes() *Indexes {
	return t.indexes
}

func (t *Table) Name() string {
	return t.structType.Name()
}

func (t *Table) NewInstance() reflect.Value {
	return reflect.New(t.structType)
}

func (t *Table) OrmRegistry() *OrmRegistry {
	return t.ormRegistry
}
