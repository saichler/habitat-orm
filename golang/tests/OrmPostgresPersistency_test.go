package tests

import (
	. "github.com/saichler/habitat-orm/golang/persistency"
	. "github.com/saichler/habitat-orm/golang/transaction"
	. "github.com/saichler/utils/golang"
	"testing"
)

func checkTable(p *Postgres,n string,t *testing.T) {
	db:=p.DB()
	_,e:=db.Exec("select * from "+p.Schema()+"."+n+";")
	if e!=nil {
		t.Fail()
		Error("Table "+n+" was not created:"+e.Error())
	}
}

func TestOrmPostgresinit(t *testing.T) {
	p:=NewPostgresPersistency1("",0,"","","","")
	tx:=&Transaction{}
	mr:=initMarshaler(5,tx)
	r:=mr.OrmRegistry()
	p.Init(r)
	checkTable(p,"Node",t)
	checkTable(p,"SubNode1",t)
	checkTable(p,"SubNode2",t)
	checkTable(p,"SubNode3",t)
	checkTable(p,"SubNode4",t)
	checkTable(p,"SubNode5",t)
	checkTable(p,"SubNode6",t)
}
