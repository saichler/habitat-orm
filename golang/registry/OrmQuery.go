package registry

import (
	"errors"
	. "github.com/saichler/habitat-orm/golang/common"
	"strings"
)

type OrmQuery struct {
	tables  map[string]*Table
	columns map[string]*Column
	where   *OrmExpression
}

type OrmExpression struct {
}

func (ormQuery *OrmQuery) Tables() map[string]*Table {
	return ormQuery.tables
}

func (ormQuery *OrmQuery) OnlyTopLevel() bool {
	return true
}

func (ormQuery *OrmQuery) initTables(o *OrmRegistry, query *Query) error {
	for _, tableName := range query.Tables() {
		found := false
		for name, table := range o.tables {
			if strings.ToLower(name) == tableName {
				ormQuery.tables[tableName] = table
				found = true
				break
			}
		}
		if !found {
			return errors.New("Could not find Struct " + tableName + " in Orm Registry.")
		}
	}
	return nil
}

func (ormQuery *OrmQuery) columnExistInTable(table *Table, columnName string) bool {
	for name, column := range table.columns {
		if strings.ToLower(name) == columnName {
			ormQuery.columns[columnName] = column
			return true
		} else if columnName=="*" {
			ormQuery.columns[columnName] = column
		}
	}
	if columnName=="*" {
		return true
	}
	return false
}

func (ormQuery *OrmQuery) initColumn(columnName string) error {
	index := strings.Index(columnName, ".")
	var table *Table
	if index != -1 {
		tableName := strings.TrimSpace(columnName[0:index])
		table = ormQuery.tables[tableName]
		columnName = strings.TrimSpace(columnName[index+1:])
	}

	if table != nil && !ormQuery.columnExistInTable(table, columnName) {
		return errors.New("Column " + columnName + " does not exist in table " + table.Name())
	}

	for _, table := range ormQuery.tables {
		if ormQuery.columnExistInTable(table, columnName) {
			return nil
		}
	}

	return errors.New("Cannot find column " + columnName + " in any of the query tables.")
}

func (o *OrmRegistry) NewOrmQuery(sql string) (*OrmQuery, error) {

	query := NewQuery(sql)
	ormQuery := &OrmQuery{}
	ormQuery.tables = make(map[string]*Table)
	ormQuery.columns = make(map[string]*Column)

	err := ormQuery.initTables(o, query)
	if err != nil {
		return nil, err
	}
	for _, c := range query.Columns() {
		err = ormQuery.initColumn(c)
		if err != nil {
			return nil, err
		}
	}

	return ormQuery, nil
}
