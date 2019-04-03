package common

import (
	"github.com/saichler/utils/golang"
	"strconv"
)

type RecordID struct {
	entries  []*RecordIDEntry
	location int
}

type RecordIDEntry struct {
	elementType string
	elementAttribute string
	elementId string
	index int
}

func (rid *RecordIDEntry) String() string {
	result:=utils.NewStringBuilder("[")
	result.Append(rid.elementType).Append(".")
	result.Append(rid.elementAttribute).Append("=")
	result.Append(rid.elementId).Append("]")
	return result.String()
}

func NewRecordID() *RecordID {
	rid:=&RecordID{}
	rid.entries = make([]*RecordIDEntry,0)
	rid.location = -1
	return rid
}

func (rid *RecordID) Add(elementType,elementAttribute,id string) {
	if rid.entries==nil {
		panic("RecordID was not created with NewRecordID method.")
	}
	ride :=&RecordIDEntry{}
	ride.elementAttribute = elementAttribute
	ride.elementId = id
	ride.elementType = elementType
	rid.entries = append(rid.entries, ride)
	rid.location++
}

func (rid *RecordID) SetIndex(index int) {
	if rid.location >-1 {
		rid.entries[rid.location].index = index
	}
}

func (rid *RecordID) Index() string {
	return strconv.Itoa(rid.entries[rid.location].index)
}

func (rid *RecordID) Set(id string) {
	rid.entries[rid.location].elementId=id
}

func (rid *RecordID) Del(){
	rid.entries = rid.entries[0:rid.location]
	rid.location--
}

func (rid *RecordID) String() string {
	sb:=utils.NewStringBuilder("")
	for _,s:=range rid.entries {
		sb.Append(s.String())
	}
	return sb.String()
}

func (rid *RecordID) Level() int {
	return len(rid.entries)
}