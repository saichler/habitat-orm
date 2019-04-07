package tests

import (
	"fmt"
	. "github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/marshal"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	. "github.com/saichler/utils/golang/tests"
	"strconv"
	"testing"
)

var size = 5;


func initMarshaler(numOfNodes int, tx *Transaction) *Marshaler {
	registry := &OrmRegistry{}
	registry.Register(Node{})
	nodes:=InitTestModel(numOfNodes)
	m:=NewMarshaler(registry,nil,tx)
	m.Marshal(nodes)
	return m
}

func initTest(numOfNodes int) *Transaction {
	tx:=&Transaction{}
	initMarshaler(numOfNodes,tx)
	return tx
}

func findNodeRecords(records []*Record, id int) *Record {
	for _,nr:=range records {
		if nr.Get("String").String()=="String-"+strconv.Itoa(id) {
			return nr
		}
	}
	return nil
}

func TestMarshalString(t *testing.T) {
	tx:=initTest(5)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
	}
}

func TestMarshalInt(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		expected:=int64(-101+i)
		val:=rec.Get("Int").Int()
		if val!= expected{
			t.Fail()
			Error("Expected "+strconv.Itoa(int(expected))+" but got:"+strconv.Itoa(int(val)))
		}
	}
}

func TestMarshalInt32(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		expected:=int64(-102+i)
		val:=rec.Get("Int32").Int()
		if val!= expected{
			t.Fail()
			Error("Expected "+strconv.Itoa(int(expected))+" but got:"+strconv.Itoa(int(val)))
		}
	}
}

func TestMarshalInt64(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		expected:=int64(-103+i)
		val:=rec.Get("Int64").Int()
		if val!= expected{
			t.Fail()
			Error("Expected "+strconv.Itoa(int(expected))+" but got:"+strconv.Itoa(int(val)))
		}
	}
}

func TestMarshalBool(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		expected:=true
		val:=rec.Get("Bool").Bool()
		if val!= expected{
			t.Fail()
			Error("Expected true but got false")
		}
	}
}

func TestMarshalPtrKey(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		expected:="OnlyChild-String-"+strconv.Itoa(i)
		val:=rec.Get("Ptr").String()
		if val!=expected {
			t.Fail()
			Error("Expected:"+expected+" got:"+val)
		}
	}
}

func TestMarshalPtrNoKey(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		val:=rec.Get("PtrNoKey").String()
		if val!=strconv.Itoa(NO_INDEX){
			t.Fail()
			Error("Expected "+strconv.Itoa(NO_INDEX)+" string but got:"+val)
		}
	}
}

func TestMarshalSlicePtrWithKey(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		strI:=strconv.Itoa(i)
		expected:="["+strI+"-Sub-Child-0,"+strI+"-Sub-Child-1,"+strI+"-Sub-Child-2,"+strI+"-Sub-Child-3]"
		val:=rec.Get("SliceOfPtr").String()
		if val!=expected {
			t.Fail()
			Error("Expected:"+expected+" got:"+val)
		}
	}
}

func TestMarshalSlicePtrWithoutKey(t *testing.T) {
	tx:=initTest(size)
	nodeRecords:=tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:"+strconv.Itoa(i))
		}
		val:=rec.Get("SlicePtrNoKey").String()
		if val!="[0,1,2]" {
			t.Fail()
			Error("Expected [+,+,+] but got:"+val)
		}
	}
}

func TestMarshalMapIntString(t *testing.T) {
	tx := initTest(size)
	nodeRecords := tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:" + strconv.Itoa(i))
		}
		s1:=strconv.Itoa(3+i)+"=3+"+strconv.Itoa(i)
		s2:=strconv.Itoa(4+i)+"=4+"+strconv.Itoa(i)
		expected1:="["+s1+","+s2+"]"
		expected2:="["+s2+","+s1+"]"
		val:=rec.Get("MapIntString").String()
		if val!=expected1 && val!=expected2 {
			t.Fail()
			Error("Did not find "+expected1)
		}
	}
}

func TestMarshalMapStringPtr(t *testing.T) {
	tx := initTest(size)
	nodeRecords := tx.AllRecords("Node")
	for i:=0;i<size;i++ {
		rec := findNodeRecords(nodeRecords, i)
		if rec == nil {
			t.Fail()
			Error("No Recrod was found with id:" + strconv.Itoa(i))
		}
		is:=strconv.Itoa(i)
		expected1:="[k1-"+is+"=0,"+"k2-"+is+"=1]"
		expected2:="[k2-"+is+"=0,"+"k1-"+is+"=1]"
		val:=rec.Get("MapStringPtrNoKey").String()
		if val!=expected1 && val!=expected2 {
			t.Fail()
			Error("Did not find "+expected1+" IN "+val)
		}
	}
}

func TestMarshalKeyPath(t *testing.T) {
	tx := initTest(size)
	for i1:=0;i1<size;i1++ {
		si1:=strconv.Itoa(i1)
		for i2:=0;i2<3;i2++ {
			si2:=strconv.Itoa(i2)
			for i3:=0;i3<3;i3++ {
				expected:="[Node.SubNode2Slice=String-"+si1+"][SubNode2.SliceInSlice="+si2+"]"
				found:=false
				id:=NewRecordID()
				id.Add("Node","SubNode2Slice","String-"+si1)
				id.Add("SubNode2","SliceInSlice",si2)
				id.Index = i2
				nodeRecords := tx.Records("SubNode3",id.String())
				if nodeRecords==nil || len(nodeRecords)!=3 {
					t.Fail()
					Error("Expected 3 records but got: "+strconv.Itoa(len(nodeRecords)))
					fmt.Println(id.String())
					continue
				}
				for _,rec:=range nodeRecords {
					val:=rec.Get(RECORD_ID).String()
					index:=int(rec.Get(RECORD_INDEX).Int())
					if val==expected && index==i3{
						found = true
						break
					}
				}
				if !found {
					t.Fail()
					Error("Did not find RecordID "+expected)
				}
			}
		}
	}
}

func TestMarshalNumberOfRecords(t *testing.T) {
	tx:=initTest(5)
	nodeRecords:=tx.AllRecords("Node")
	if len(nodeRecords)!=30 {
		t.Fail()
		Error("Node: Expected 30 but got:"+strconv.Itoa(len(nodeRecords)))
	}
	nodeRecords=tx.AllRecords("SubNode1")
	if len(nodeRecords)!=20 {
		t.Fail()
		Error("SubNode1: Expected 20 but got:"+strconv.Itoa(len(nodeRecords)))
	}
	nodeRecords=tx.AllRecords("SubNode6")
	if len(nodeRecords)!=10 {
		t.Fail()
		Error("SubNode6: Expected 10 but got:"+strconv.Itoa(len(nodeRecords)))
	}
}
