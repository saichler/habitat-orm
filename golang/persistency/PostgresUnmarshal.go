package persistency

import (
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	"reflect"
)

func (p *Postgres) Unmarshal(q *OrmQuery, r *OrmRegistry, tx *Transaction) error {
	return p.unmarshal("Node", r, tx, make(map[string]string))
}

func (p *Postgres) unmarshal(tableName string, r *OrmRegistry, tx *Transaction, loaded map[string]string) error {

	_, o := loaded[tableName]
	if o {
		return nil
	}
	loaded[tableName] = ""

	table := r.Table(tableName)

	st := CreateSelectStatement(p.TableName(table), "")

	argNames := make([]string, 1)
	st.AddColumn(RECORD_LEVEL, "")
	argNames[0] = RECORD_LEVEL
	if table.Indexes().PrimaryIndex() == nil {
		st.AddColumn(RECORD_ID, "")
		st.AddColumn(RECORD_INDEX, "")
		argNames = append(argNames, RECORD_ID)
		argNames = append(argNames, RECORD_INDEX)
	}
	for columnName, column := range table.Columns() {
		if column.MetaData().Ignore() {
			continue
		}
		st.AddColumn(columnName, "")
		argNames = append(argNames, columnName)
	}

	rows, err := st.Query(p.tx)
	if err != nil {
		Error(err)
		return err
	}

	fnc := reflect.ValueOf(rows).MethodByName("Scan")
	arguments := make([]reflect.Value, len(argNames))
	for i := 0; i < len(arguments); i++ {
		value := ""
		arguments[i] = reflect.ValueOf(&value)
	}

	subTables := make(map[string]string)

	for ; rows.Next(); {
		fnc.Call(arguments)
		record := &Record{}
		for i, columnName := range argNames {
			var colValue reflect.Value
			stringValue := arguments[i].Elem().String()
			if columnName == RECORD_LEVEL || columnName == RECORD_INDEX {
				colValue = FromString(stringValue, reflect.ValueOf(int(0)).Type())
			} else if columnName == RECORD_ID {
				colValue = arguments[i].Elem()
			} else {
				col, err := table.Column(columnName)
				if err != nil {
					panic(err)
				}
				if col.MetaData().ColumnTableName() != "" {
					subTables[col.MetaData().ColumnTableName()] = ""
					colValue = arguments[i].Elem()
				} else if col.Type().Kind() == reflect.Map || col.Type().Kind() == reflect.Slice {
					colValue = arguments[i].Elem()
				} else {
					colValue = FromString(stringValue, col.Type())
				}
			}
			record.SetValue(columnName, colValue)
		}

		recordID := ""
		if table.Indexes().PrimaryIndex() != nil {
			recordID = record.PrimaryIndex(table.Indexes().PrimaryIndex())
		} else {
			recordID = record.Get(RECORD_ID).String()
		}
		tx.AddRecord(record, table.Name(), recordID)
	}

	for tn, _ := range subTables {
		p.unmarshal(tn, r, tx, loaded)
	}

	return nil
}
