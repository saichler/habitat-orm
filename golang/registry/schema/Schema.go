package schema

import (
	"reflect"
	"strings"
)

type Schema struct {
	tablePaths map[string]*TablePath
}

func NewSchema() *Schema {
	s := &Schema{}
	s.tablePaths = make(map[string]*TablePath)
	return s
}

func (s *Schema) RegiaterTablePath(columnName string, typ reflect.Type, parent *TablePath) *TablePath {
	tp := &TablePath{}
	tp.columnName = columnName
	tp.structType = typ
	tp.parent = parent
	if tp.parent == nil {
		tp.columnName = typ.Name()
	}
	s.tablePaths[tp.Key()] = tp
	return tp
}

func (s *Schema) TablePaths() map[string]*TablePath {
	return s.tablePaths
}

func (s *Schema) CreateColumnPath(tp *TablePath, key string) *ColumnPath {
	lastIndex := strings.LastIndex(key, ".")
	if lastIndex == -1 {
		for i:=0;i<tp.structType.NumField();i++ {
			colName:=tp.structType.Field(i).Name
			if strings.ToLower(colName) == strings.ToLower(key) {
				cp:=&ColumnPath{}
				cp.columnName = colName
				cp.parent = tp
				return cp
			}
		}
		return nil
	}
	tablePath:= s.getTablePath(tp.structType.Name()+"."+key[0:lastIndex])
	if tablePath != nil {
		return s.CreateColumnPath(tablePath, key[lastIndex+1:])
	}
	return nil
}

func (s *Schema) getTablePath(path string) *TablePath {
	for name, st := range s.tablePaths {
		if strings.ToLower(name) == strings.ToLower(path) {
			return st
		}
	}
	return nil
}