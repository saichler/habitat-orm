package marshal

import (
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	"github.com/saichler/utils/golang"
	"reflect"
	"strconv"
)

type Marshaler struct {
	ormRegistry *OrmRegistry
	persistency Persistency
	tx          *Transaction
}

var marshalers = make(map[reflect.Kind]func(reflect.Value, *OrmRegistry, *Transaction, Persistency, *RecordID) (reflect.Value, error))

func initMarshalers() {
	if len(marshalers) == 0 {
		marshalers[reflect.Ptr] = ptrMarshal
		marshalers[reflect.Struct] = structMarshal
		marshalers[reflect.Map] = mapMarshal
		marshalers[reflect.Slice] = sliceMarshal
		marshalers[reflect.String] = defaultMarshal
		marshalers[reflect.Int] = defaultMarshal
		marshalers[reflect.Int32] = defaultMarshal
		marshalers[reflect.Int64] = defaultMarshal
		marshalers[reflect.Uint] = defaultMarshal
		marshalers[reflect.Uint32] = defaultMarshal
		marshalers[reflect.Uint64] = defaultMarshal
		marshalers[reflect.Float64] = defaultMarshal
		marshalers[reflect.Float32] = defaultMarshal
		marshalers[reflect.Bool] = defaultMarshal
	}
}

func NewMarshaler(ormRegistry *OrmRegistry, persistency Persistency, tx ...*Transaction) *Marshaler {
	initMarshalers()
	m := &Marshaler{}
	m.ormRegistry = ormRegistry
	m.persistency = persistency
	if tx == nil {
		m.tx = &Transaction{}
	} else {
		m.tx = tx[0]
	}
	return m
}

func (m *Marshaler) Marshal(any interface{}) error {
	initMarshalers()
	if any == nil {
		return nil
	}
	value := reflect.ValueOf(any)
	value, err := marshal(value, m.ormRegistry, m.tx, m.persistency, NewRecordID())
	return err
}

func marshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	marshalFunc := marshalers[value.Kind()]
	if marshalFunc == nil {
		panic("No Marshal Function for kind " + value.Kind().String())
	}
	return marshalFunc(value, r, tx, pr, rid)
}

func ptrMarshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	if value.IsNil() {
		return reflect.ValueOf(""), nil
	}
	v := value.Elem()
	return marshal(v, r, tx, pr, rid)
}

func structMarshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	tableName := value.Type().Name()
	//No need to do anything, nameless struct
	if tableName == "" {
		return value, nil
	}
	table := r.Table(tableName)
	if table == nil {
		panic("Table:" + tableName + " was not registered!")
	}

	rec := &Record{}
	rec.SetInterface(RECORD_LEVEL, rid.Level())
	if table.Indexes().PrimaryIndex() == nil {
		rec.SetInterface(RECORD_ID, rid.String())
		rec.SetInterface(RECORD_INDEX,rid.Index)
	}
	subTables := make([]*Column, 0)
	for fieldName, column := range table.Columns() {
		if column.MetaData().ColumnTableName() == "" {
			fieldValue := value.FieldByName(fieldName)
			marshalValue, err := marshal(fieldValue, r, tx, pr, rid)
			if err != nil {
				panic(err)
			}
			rec.SetValue(fieldName, marshalValue)
		} else {
			subTables = append(subTables, column)
		}
	}

	recordID := ""

	if table.Indexes().PrimaryIndex() != nil {
		recordID = rec.PrimaryIndex(table.Indexes().PrimaryIndex())
		tx.AddRecord(rec, tableName, recordID)
	} else {
		tx.AddRecord(rec, tableName, rid.String())
		recordID=strconv.Itoa(rid.Index)
	}

	for _, sbColumn := range subTables {
		isTable:=sbColumn.MetaData().ColumnTableName()!=""
		if isTable {
			rid.Add(table.Name(), sbColumn.Name(), recordID)
		}
		fieldValue := value.FieldByName(sbColumn.Name())
		sbValue, err := marshal(fieldValue, r, tx, pr, rid)
		if err != nil {
			return reflect.ValueOf(rec), err
		}
		rec.SetInterface(sbColumn.Name(), utils.ToString(sbValue))
		if isTable {
			rid.Del()
		}
	}
	return reflect.ValueOf(recordID), nil
}

func sliceMarshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	if value.IsNil() {
		return reflect.ValueOf(""), nil
	}
	sb := utils.NewStringBuilder("[")
	for i := 0; i < value.Len(); i++ {
		rid.Index = i
		v, e := marshal(value.Index(i), r, tx, pr, rid)
		if e != nil {
			panic("Unable To marshal! "+e.Error())
		}
		if i != 0 {
			sb.Append(",")
		}
		sb.Append(utils.ToString(v))
	}
	sb.Append("]")
	return reflect.ValueOf(sb.String()), nil
}

func mapMarshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	if value.IsNil() {
		return reflect.ValueOf(""), nil
	}
	sb := utils.NewStringBuilder("[")
	mapKeys := value.MapKeys()
	for i, key := range mapKeys {
		mv := value.MapIndex(key)
		keyString:=utils.ToString(key)
		rid.Index =i
		v, e := marshal(mv, r, tx, pr, rid)
		if e != nil {
			panic("Unable To marshal! "+e.Error())
		}
		if i > 0 {
			sb.Append(",")
		}
		sb.Append(keyString)
		sb.Append("=")
		sb.Append(utils.ToString(v))
	}
	sb.Append("]")
	return reflect.ValueOf(sb.String()), nil
}

func defaultMarshal(value reflect.Value, r *OrmRegistry, tx *Transaction, pr Persistency, rid *RecordID) (reflect.Value, error) {
	return value, nil
}
