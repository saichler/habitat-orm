package tests

import (
	. "github.com/saichler/habitat-orm/golang/persistency"
	. "github.com/saichler/habitat-orm/golang/transaction"
	"testing"
)

func TestOrmPostgresinit(t *testing.T) {
	p:=NewPostgresPersistency1("",0,"","","","")
	tx:=&Transaction{}
	mr:=initMarshaler(5,tx)
	r:=mr.OrmRegistry()
	p.Init(r)
}
