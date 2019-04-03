package marshal

import (
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	"github.com/saichler/utils/golang"
	"reflect"
)

var getters = make(map[reflect.Kind]func(*Column, *Record, *RecordID, *Transaction) reflect.Value)

func initSetters() {
	if len(getters) == 0 {
		getters[reflect.Ptr] = getPtr
		getters[reflect.String] = getDefault
		getters[reflect.Float32] = getDefault
		getters[reflect.Float64] = getDefault
		getters[reflect.Uint] = getDefault
		getters[reflect.Uint16] = getDefault
		getters[reflect.Uint32] = getDefault
		getters[reflect.Uint64] = getDefault
		getters[reflect.Int] = getDefault
		getters[reflect.Int16] = getDefault
		getters[reflect.Int32] = getDefault
		getters[reflect.Int64] = getDefault
		getters[reflect.Bool] = getDefault
		getters[reflect.Struct] = getStruct
		getters[reflect.Map] = getMap
		getters[reflect.Slice] = getSlice
	}
}

func (m *Marshaler) UnMarshal(ormQuery *Query) []interface{} {
	initSetters()
	instances := unmarshal(ormQuery, m.tx, m.ormRegistry, NewRecordID())
	result := make([]interface{}, len(instances))
	for i := 0; i < len(result); i++ {
		result[i] = instances[i].Interface()
	}
	return result
}

func unmarshal(query *Query, tx *Transaction, ormRegistry *OrmRegistry, id *RecordID) []reflect.Value {
	table := ormRegistry.Table(query.TableName())
	if table == nil {
		panic("Unknown table " + query.TableName())
	}
	result := make([]reflect.Value, 0)

	records := tx.AllRecords(query.TableName())
	for _, record := range records {
		if record.Get(RECORD_LEVEL).Int() == 0 || !query.OnlyTopLevel() {
			instance := table.NewInstance()
			result = append(result, instance)
			for _, column := range table.Columns() {
				field := instance.Elem().FieldByName(column.Name())
				key := record.PrimaryIndex(table.Indexes().PrimaryIndex())
				id.Add(table.Name(), column.Name(), key)
				fv := get(column, record, id, tx)
				instance.Elem()
				//fmt.Println(column.Name())
				if fv.IsValid() {
					field.Set(fv)
				}
				id.Del()
			}
		}
	}
	return result
}

func get(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	getter := getters[column.Type().Kind()]
	if getter == nil {
		panic("No Getter for kind:" + column.Type().String())
	}
	return getter(column, record, id, tx)
}

func getPtr(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	ptrKind := column.Type().Elem().Kind()
	if ptrKind == reflect.Struct {
		table := column.Table().OrmRegistry().Table(column.Type().Elem().Name())
		if table.Indexes().PrimaryIndex() == nil {
			subRecords := tx.Records(column.MetaData().ColumnTableName(), id.String())
			if subRecords == nil {
				return reflect.ValueOf(nil)
			}
			return getStruct(column, subRecords[0], id, tx)
		}
		key := record.Get(column.Name()).String()
		if key == "" {
			return reflect.ValueOf(nil)
		}
		subRecords := tx.Records(column.MetaData().ColumnTableName(), key)
		return getStruct(column, subRecords[0], id, tx)
	} else if ptrKind == reflect.Slice {
		newSlice := reflect.MakeSlice(reflect.SliceOf(column.Type().Elem()), 0, 0)
		//@TODO implement
		return newSlice
	} else if ptrKind == reflect.Map {
		newMap := reflect.MakeMapWithSize(column.Type(), 0)
		//@TODO implement
		return newMap
	} else {
		panic("No Ptr Handle of:" + ptrKind.String())
	}
}

func getDefault(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	return record.Get(column.Name())
}

func getStruct(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	table := column.Table().OrmRegistry().Table(column.MetaData().ColumnTableName())
	if table == nil {
		panic("Cannot find table name:" + column.MetaData().ColumnTableName())
	}
	instance := table.NewInstance()

	for _, c := range table.Columns() {
		fld := instance.Elem().FieldByName(c.Name())
		var key = "0"
		if table.Indexes().PrimaryIndex() != nil {
			key = record.PrimaryIndex(table.Indexes().PrimaryIndex())
		} else {
			value := record.Get(RECORD_ID)
			if !value.IsValid() {
				panic(table.Name() + ":" + column.Name())
			}
			if key == "" {

			}
		}
		id.Add(table.Name(), c.Name(), key)
		v := get(c, record, id, tx)
		if v.IsValid() {
			fld.Set(v)
		}
		id.Del()
	}
	return instance
}

func getMap(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	//@TODO
	return reflect.ValueOf(nil)
}

func getSlice(column *Column, record *Record, id *RecordID, tx *Transaction) reflect.Value {
	value := record.Get(column.Name())
	vString := value.String()
	if value.IsValid() {
		if vString == "" {
			return reflect.ValueOf(nil)
		}
		if column.MetaData().ColumnTableName() == "" {
			return utils.FromString(vString, column.Type())
		} else {
			//Keyed ptr slice
			/*
			table:=column.Table().OrmRegistry().Table(column.MetaData().ColumnTableName())
			if table==nil {
				panic("No Table was found with name:"+column.MetaData().ColumnTableName())
			}
			//recs:=tx.Records(table.Name(),id.String())
			fmt.Println(column.Name()+":"+vString)
			fmt.Println(id.String())
			*/
		}
	} else if column.MetaData().ColumnTableName() != "" {
		table := column.Table().OrmRegistry().Table(column.MetaData().ColumnTableName())
		if table == nil {
			panic("No Table was found with name:" + column.MetaData().ColumnTableName())
		}
		recs := tx.Records(table.Name(), id.String())
		newSlice := reflect.MakeSlice(column.Type(), len(recs), len(recs))
		for i, rec := range recs {
			elem := getStruct(column, rec, id, tx)
			newSlice.Index(i).Set(elem)

		}
		return newSlice
	}
	return reflect.ValueOf(nil)
}
