package tests

import (
	. "github.com/saichler/utils/golang"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/utils/golang/tests"
	"strconv"
	"testing"
)

func TestOrmRegistryMainTable(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable == nil {
		t.Fail()
		Error("Failed to register node")
		return
	}
}

func TestOrmRegistryTitleTag(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable.Columns()["String"].MetaData().Title() != "Hello" {
		t.Fail()
		Error("Title tag does not work")
		return
	}
}

func TestOrmRegistrySizeTag(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable.Columns()["String"].MetaData().Size() != 5 {
		t.Fail()
		Error("Size tag does not work")
		return
	}
}

func TestOrmRegistryIgnoreTag(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if !nodeTable.Columns()["String2"].MetaData().Ignore(){
		t.Fail()
		Error("Ignore tag does not work")
		return
	}
}

func TestOrmRegistryMaskTag(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if !nodeTable.Columns()["String3"].MetaData().Mask(){
		t.Fail()
		Error("Mask tag does not work")
		return
	}
}

func TestOrmRegistryColumns(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if len(nodeTable.Columns()) == 0 {
		t.Fail()
		Error("No columns in registry")
		return
	}
}

func TestOrmRegistrySubNode1(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("SubNode1")
	if nodeTable == nil {
		t.Fail()
		Error("Did not find sub table 1")
		return
	}
}

func TestOrmRegistrySubNode4(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable:=registry.Table("SubNode4")
	if nodeTable==nil {
		t.Fail()
		Error("Did not find sub table 4")
		return
	}
}

func TestOrmRegistryPrimaryIndex(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable.Indexes().PrimaryIndex()==nil {
		t.Fail()
		Error("Primary index was not found")
		return
	}
}

func TestOrmRegistryUniqueIndexes(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable.Indexes().UniqueIndexes()==nil {
		t.Fail()
		Error("Unique indexed were not found")
		return
	}

	if nodeTable.Indexes().UniqueIndexes()["uk1"]==nil {
		t.Fail()
		Error("Unique indexed 1 not found")
		return
	}

	columns:=nodeTable.Indexes().UniqueIndexes()["uk1"].Columns()
	l:=len(columns)
	if l!=2 {
		t.Fail()
		Error("Unique indexed len is not 2 and it is:"+strconv.Itoa(l))
		return
	}

	if columns[0].Name()!="IntKey" {
		t.Fail()
		Error("First key must be IntKey")
		return
	}

	if columns[1].Name()!="String4" {
		t.Fail()
		Error("Second key must be String4")
		return
	}
}

func TestOrmRegistryNonUniqueIndexes(t *testing.T) {
	registry := OrmRegistry{}
	registry.Register(Node{})
	nodeTable := registry.Table("Node")
	if nodeTable.Indexes().NonUniqueIndexes()==nil {
		t.Fail()
		Error("NonUnique indexed were not found")
		return
	}
}