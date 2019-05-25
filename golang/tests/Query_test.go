package tests

import (
	. "github.com/saichler/habitat-orm/golang/query"
	. "github.com/saichler/utils/golang"
	"testing"
)

func TestQuery(t *testing.T) {
	q,e := NewQuery("Select column1,column2 fRom table1 wHere 1=2 or ((3!=4 and 5<6) and 7>8) or ((9=10) and 11=12) ")
	if e!=nil {
		Error(e)
		t.Fail()
		return
	}
	testTables(q,[]string{"table1"},t)
	testExpression(q.Where(),"(1=2) or (((3!=4 and 5<6) and (7>8)) or (((9=10) and (11=12))))",t)
}

func testTables(q *Query,expected []string, t *testing.T) {
	if len(q.Tables())!=1 {
		t.Fail()
		Error("Expected one table")
		return
	}
	for _,et:=range expected {
		found:=false
		for _,qt:=range q.Tables() {
			if qt == et {
				found=true
			}
		}
		if !found {
			t.Fail()
			Error("Expected table "+et+" but did not find it")
			return
		}
	}
}

func testExpression(expr *Expression,expected string,t *testing.T) {
	if expr.String()!=expected {
		t.Fail()
		Error("Expected: "+expected)
		Error("But got : "+expr.String())
	}
}
