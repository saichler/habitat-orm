package tests

import (
	"fmt"
	. "github.com/saichler/habitat-orm/golang/cloner"
	. "github.com/saichler/utils/golang/tests"
	"strconv"
	"testing"
)

func TestCloner(t *testing.T) {
	nodes:=InitTestModel(1)
	testClone :=Clone(nodes[0]).(*Node)
	/*
	if testClone.Name!="Model Test" {
		t.Fail()
		fmt.Println("Fail @1")
		return
	}
	if testClone.Node.NodeName!="node name" {
		t.Fail()
		fmt.Println("Fail @2")
		return
	}
	if testClone.Val1!=153 {
		t.Fail()
		fmt.Println("Fail @3")
		return
	}
	if testClone.Nodes[1].NodeName!="Node #1" {
		t.Fail()
		fmt.Println("Fail @4")
		return
	}
	if testClone.Nodes[1].IntSlice[1]!=1 {
		t.Fail()
		fmt.Println("Fail @5")
		return
	}
	if testClone.Nodes[1].StringSlice[1]!="str=1" {
		t.Fail()
		fmt.Println("Fail @6")
		return
	}
	if testClone.Nodes[1].MapOfPtr["B"].NodeName!="Map=1" {
		t.Fail()
		fmt.Println("Fail @7")
		return
	}
	if testClone.Nodes[1].MapIntToStr[44]!="44" {
		t.Fail()
		fmt.Println("Fail @8")
		return
	}*/
	fmt.Println(strconv.Itoa(testClone.Int))
	fmt.Println(testClone.String)
	fmt.Println(testClone.Ptr.String)
	fmt.Println(testClone.SliceOfPtr[1].String)
	fmt.Println(testClone.SliceOfPtr[1].SliceInt[1])
	fmt.Println(testClone.SliceOfPtr[1].SliceString[1])
	fmt.Println(testClone.SliceOfPtr[1].MapStringPtr["B"].String)
	fmt.Println(testClone.SliceOfPtr[1].MapIntString[44])
}
