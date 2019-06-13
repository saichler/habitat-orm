package persistency

import (
	"database/sql"
	"github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	"reflect"
)

func (p *Postgres) Marshal(r *OrmRegistry, tx *Transaction) error {
	allTxdata := tx.All()
	for tableName, tableData := range allTxdata {
		table := r.Table(tableName)
		hasPrimaryIndex := table.Indexes().PrimaryIndex() != nil
		if hasPrimaryIndex {
			p.marshalIndexedTable(table, tableData)
		} else {
			p.marshalUnIndexedTable(table, tableData)
		}
	}
	p.tx.Commit()
	return nil
}

func (p *Postgres) marshalIndexedTable(table *Table, tableData map[string][]*Record) error {
	index := table.Indexes().PrimaryIndex()
	for _, records := range tableData {
		for _, record := range records {
			var st *SqlST
			exist, err := rowExist(p.TableName(table), index, record, p.tx)
			if err != nil {
				return err
			}
			if !exist {
				st = CreateInsertStatement(p.TableName(table))
			} else {
				st = CreateUpdateStatement(p.TableName(table), index.CriteriaStatement())
				for _, column := range index.Columns() {
					addCriteriaColumnValue(st, column, record.Get(column.Name()))
				}
			}
			for name, value := range record.Data() {
				if !table.IgnoreColumn(name) {
					addColumnValue(st, name, table.Name(), value)
				}
			}
			_, err = st.Exec(p.tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Postgres) deleteUnIndex(key, tablename string) error {
	st := CreateDeleteStatement(tablename, "#1")
	st.AddCriteriaColumn(common.RECORD_ID, key)
	_, err := st.Exec(p.tx)
	return err
}

func (p *Postgres) marshalUnIndexedTable(table *Table, tableData map[string][]*Record) error {
	for key, records := range tableData {
		err := p.deleteUnIndex(key, p.TableName(table))
		if err != nil {
			return err
		}
		for _, record := range records {
			st := CreateInsertStatement(p.TableName(table))
			for name, value := range record.Data() {
				if !table.IgnoreColumn(name) {
					addColumnValue(st, name, table.Name(), value)
				}
			}
			_, err = st.Exec(p.tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func rowExist(tableName string, index *Index, record *Record, tx *sql.Tx) (bool, error) {
	st := CreateSelectStatement(tableName, index.CriteriaStatement())
	for _, column := range index.Columns() {
		addCriteriaColumnValue(st, column, record.Get(column.Name()))
	}
	st.AddColumn("COUNT(*) as count", "")
	rows, err := st.Query(tx)
	if err != nil {
		Error("!!!!! Error, Failed to count records.", err)
		return false, err
	}
	count := -1
	rows.Next()
	rows.Scan(&count)
	rows.Close()
	if count == 1 {
		return true, nil
	}
	return false, nil
}

func addCriteriaColumnValue(st *SqlST, column *Column, value reflect.Value) {
	if !value.IsValid() {
		panic("Criteria value is not valide for:" + column.Name() + " table:" + column.Table().Name())
	}
	if value.Kind() == reflect.String {
		st.AddCriteriaColumn(column.Name(), value.String())
	} else if value.Kind() == reflect.Int || value.Kind() == reflect.Int8 || value.Kind() == reflect.Int16 || value.Kind() == reflect.Int32 || value.Kind() == reflect.Int64 {
		st.AddCriteriaColumn(column.Name(), value.Int())
	} else if value.Kind() == reflect.Bool {
		st.AddCriteriaColumn(column.Name(), value.Bool())
	} else if value.Kind() == reflect.Float32 || value.Kind() == reflect.Float64 {
		st.AddCriteriaColumn(column.Name(), value.Float())
	} else if value.Kind() == reflect.Uint || value.Kind() == reflect.Uint8 || value.Kind() == reflect.Uint16 || value.Kind() == reflect.Uint32 || value.Kind() == reflect.Uint64 {
		st.AddCriteriaColumn(column.Name(), value.Uint())
	} else {
		panic("Not supported kind for criteria:" + value.Kind().String())
	}
}

func addColumnValue(st *SqlST, columnName, tableName string, value reflect.Value) {
	if !value.IsValid() {
		panic("Attribute value is not valide for:" + columnName + " table:" + tableName)
	}
	if value.Kind() == reflect.String {
		st.AddColumn(columnName, value.String())
	} else if value.Kind() == reflect.Int || value.Kind() == reflect.Int8 || value.Kind() == reflect.Int16 || value.Kind() == reflect.Int32 || value.Kind() == reflect.Int64 {
		st.AddColumn(columnName, value.Int())
	} else if value.Kind() == reflect.Bool {
		st.AddColumn(columnName, value.Bool())
	} else if value.Kind() == reflect.Float32 || value.Kind() == reflect.Float64 {
		st.AddColumn(columnName, value.Float())
	} else if value.Kind() == reflect.Uint || value.Kind() == reflect.Uint8 || value.Kind() == reflect.Uint16 || value.Kind() == reflect.Uint32 || value.Kind() == reflect.Uint64 {
		st.AddColumn(columnName, value.Uint())
	} else {
		panic("Not supported kind for column:" + value.Kind().String())
	}
}
