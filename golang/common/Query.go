package common

type Query struct {
	ormQuery string
	tableName string
	onlyToLEvel bool
}

func NewQuery(ormQuery string,onlyTopLevel bool) *Query {
	q:=&Query{}
	q.ormQuery = ormQuery
	q.onlyToLEvel = onlyTopLevel
	q.parse()
	return q
}

func (q *Query) parse(){
	q.tableName = "Node"
}

func (q *Query) TableName() string {
	return q.tableName
}

func (q *Query) OnlyTopLevel() bool {
	return q.onlyToLEvel
}