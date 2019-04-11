package persistency

import (
	"fmt"
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	"reflect"
)

func (p *Postgres) Unmarshal(q *Query,r *OrmRegistry) (*Transaction,error) {
	tx:=&Transaction{}
	table:=r.Table(q.TableName())
	st:=CreateSelectStatement(p.TableName(table),"")
	argNames:=make([]string,1)
	st.AddColumn(RECORD_LEVEL,"")
	argNames[0]=RECORD_LEVEL
	if table.Indexes().PrimaryIndex()==nil {
		st.AddColumn(RECORD_ID,"")
		st.AddColumn(RECORD_INDEX,"")
		argNames = append(argNames,RECORD_ID)
		argNames = append(argNames,RECORD_INDEX)
	}
	for columnName,column:=range table.Columns() {
		if column.MetaData().Ignore() {
			continue
		}
		st.AddColumn(columnName,"")
		argNames = append(argNames,columnName)
	}

	rows,err:=st.Query(p.tx)
	if err!=nil {
		Error(err)
		return nil,err
	}

	fnc := reflect.ValueOf(rows).MethodByName("Scan")
	arguments:=make([]reflect.Value,len(argNames))
	for i:=0;i<len(arguments);i++ {
		value:=""
		arguments[i] = reflect.ValueOf(&value)
	}
	for ;rows.Next(); {

		fnc.Call(arguments)
		record:=&Record{}
		for i,columnName:=range argNames {
			fmt.Println(columnName+"="+arguments[i].Elem().String())
			record.SetInterface(columnName,arguments[i])
		}
		tx.AddRecord(record,table.Name(),"")
	}
	return nil,nil
}
