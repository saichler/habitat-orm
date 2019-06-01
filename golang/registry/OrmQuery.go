package registry

import (
	"bytes"
	"errors"
	. "github.com/saichler/habitat-orm/golang/query"
	. "github.com/saichler/habitat-orm/golang/registry/expression"
	. "github.com/saichler/habitat-orm/golang/registry/schema"
	"reflect"
	"strings"
)

type OrmQuery struct {
	tables  map[string]*TablePath
	columns map[string]*ColumnPath
	where   *OrmExpression
}

func (ormQuery *OrmQuery) String() string {
	buff := bytes.Buffer{}
	buff.WriteString("Select ")
	first := true

	for _, column := range ormQuery.columns {
		if !first {
			buff.WriteString(", ")
		}
		buff.WriteString(column.Key())
		first = false
	}

	buff.WriteString(" From ")

	first = true
	for _, table := range ormQuery.tables {
		if !first {
			buff.WriteString(", ")
		}
		buff.WriteString(table.Key())
		first = false
	}

	if ormQuery.where != nil {
		buff.WriteString(" Where ")
		buff.WriteString(ormQuery.where.String())
	}
	return buff.String()
}

func (ormQuery *OrmQuery) Tables() map[string]*TablePath {
	return ormQuery.tables
}

func (OrmQuery *OrmQuery) Columns() map[string]*ColumnPath {
	return OrmQuery.columns
}

func (ormQuery *OrmQuery) OnlyTopLevel() bool {
	return true
}

func (ormQuery *OrmQuery) initTables(o *OrmRegistry, query *Query) error {
	for _, tableName := range query.Tables() {
		found := false
		for name, table := range o.tables {
			if strings.ToLower(name) == tableName {
				ormQuery.tables[tableName] = o.Schema().TablePaths()[table.Name()]
				found = true
				break
			}
		}
		if !found {
			return errors.New("Could not find Table " + tableName + " in Orm Registry.")
		}
	}
	return nil
}

func (ormQuery *OrmQuery) initColumns(o *OrmRegistry, query *Query) error {
	mainTable, e := ormQuery.MainTable()
	if e != nil {
		return e
	}
	for _, col := range query.Columns() {
		cp := o.schema.CreateColumnPath(mainTable, col)
		if cp == nil {
			return errors.New("Cannot find query field: " + col)
		}
		ormQuery.columns[col] = cp
	}
	return nil
}

func (o *OrmRegistry) NewOrmQuery(sql string) (*OrmQuery, error) {

	query, err := NewQuery(sql)
	if err != nil {
		return nil, err
	}
	ormQuery := &OrmQuery{}
	ormQuery.tables = make(map[string]*TablePath)
	ormQuery.columns = make(map[string]*ColumnPath)

	err = ormQuery.initTables(o, query)
	if err != nil {
		return nil, err
	}

	err = ormQuery.initColumns(o, query)
	if err != nil {
		return nil, err
	}

	mainTable, err := ormQuery.MainTable()
	if err != nil {
		return nil, err
	}

	if query.Where()!=nil {
		expr, err := CreateExpression(o.schema, mainTable, query.Where())
		if err != nil {
			return nil, err
		}
		ormQuery.where = expr
	}

	return ormQuery, nil
}

func (ormQuery *OrmQuery) MainTable() (*TablePath, error) {
	for _, t := range ormQuery.tables {
		return t, nil
	}
	return nil, errors.New("No tables in query")
}

func (ormQuery *OrmQuery) Match(any interface{}) (bool, error) {
	val := reflect.ValueOf(any)
	return ormQuery.match(val)
}

func (ormQuery *OrmQuery) match(value reflect.Value) (bool, error) {
	if !value.IsValid() {
		return false, nil
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return false, nil
		} else {
			value = value.Elem()
		}
	}
	tableName := strings.ToLower(value.Type().Name())
	table := ormQuery.tables[tableName]
	if table == nil {
		return false, nil
	}
	return ormQuery.where.Match(value)
}
