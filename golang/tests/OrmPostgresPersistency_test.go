package tests

import (
	. "github.com/saichler/habitat-orm/golang/marshal"
	. "github.com/saichler/habitat-orm/golang/persistency"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	. "github.com/saichler/utils/golang/tests"
	"strconv"
	"testing"
)

const (
	BasicQuery = "Select * from Node"
)

func checkTable(p *Postgres, n string, t *testing.T) {
	db := p.DB()
	_, e := db.Exec("select * from " + p.Schema() + "." + n + ";")
	if e != nil {
		t.Fail()
		Error("Table " + n + " was not created:" + e.Error())
	}
}

func TestOrmPostgresinit(t *testing.T) {
	p := NewPostgresPersistency1("", 0, "", "", "", "")
	tx := &Transaction{}
	mr := initMarshaler(5, tx)
	r := mr.OrmRegistry()
	p.Init(r)
	checkTable(p, "Node", t)
	checkTable(p, "SubNode1", t)
	checkTable(p, "SubNode2", t)
	checkTable(p, "SubNode3", t)
	checkTable(p, "SubNode4", t)
	checkTable(p, "SubNode5", t)
	checkTable(p, "SubNode6", t)
}

func TestOrmPostgresMarshal(t *testing.T) {
	p := NewPostgresPersistency1("", 0, "", "", "", "")
	tx := &Transaction{}
	mr := initMarshaler(5, tx)
	r := mr.OrmRegistry()
	p.Init(r)
	p.Marshal(r, tx)
}

func TestOrmPostgresUnmarshalMarshal(t *testing.T) {
	p := NewPostgresPersistency1("", 0, "", "", "", "")
	tx := &Transaction{}
	mr := initMarshaler(5, tx)
	r := mr.OrmRegistry()
	p.Init(r)
	q, e := mr.OrmRegistry().NewOrmQuery(BasicQuery)
	if e != nil {
		Error(e)
		t.Fail()
	}
	tx = &Transaction{}
	p.Unmarshal(q, r, tx)
}

func TestPosgresUnMarshalPtrNoKey(t *testing.T) {
	p := NewPostgresPersistency1("", 0, "", "", "", "")
	tx := &Transaction{}
	mr := initMarshaler(5, tx)
	r := mr.OrmRegistry()
	p.Init(r)
	q, e := mr.OrmRegistry().NewOrmQuery(BasicQuery)
	if e != nil {
		Error(e)
		t.Fail()
	}
	tx = &Transaction{}
	p.Unmarshal(q, r, tx)

	m := NewMarshaler(r, nil, tx)

	q, e = m.OrmRegistry().NewOrmQuery(BasicQuery)
	if e != nil {
		Error(e)
		t.Fail()
	}
	instances := m.UnMarshal(q)
	if len(instances) != size {
		t.Fail()
		Error("Expected:" + strconv.Itoa(size) + " but got " + strconv.Itoa(len(instances)))
	}
	for i := 0; i < size; i++ {
		for _, n := range instances {
			node := n.(*Node)
			if node.PtrNoKey == nil {
				t.Fail()
				Error("Expected ptr no key to exist")
			} else if node.PtrNoKey.String == "" {
				t.Fail()
				Error("Expected ptr no key name not to be blank ")
			}
		}
	}
}
