package tests

import (
	"fmt"
	. "github.com/saichler/habitat-orm/golang/updater"
	. "github.com/saichler/utils/golang/tests"
	"testing"
)

func TestUpdater(t *testing.T) {
	nodes:=InitTestModel(2)
	old:=nodes[0]
	new:=nodes[1]
	fmt.Println(old.String)
	fmt.Println(old.SliceString)
	fmt.Println(old.SliceOfPtr[0].String)
	Update(old,new)
	fmt.Println(old.String)
	fmt.Println(old.SliceString)
	fmt.Println(old.SliceOfPtr[0].String)
}
