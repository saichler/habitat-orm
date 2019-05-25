package query

import "strings"

type Query struct {
	query  string
	tables []string
	column []string
	where  *Expression
}

const (
	Select = "select"
	From   = "from"
	Where  = "where"
)

func (q *Query) Where() *Expression {
	return q.where
}

func (q *Query) Tables() []string {
	return q.tables
}

func (q *Query) Columns() []string {
	return q.column
}

func NewQuery(query string) (*Query, error) {
	cwql := &Query{}
	cwql.query = query
	e := cwql.init()
	return cwql, e
}

func (q *Query) split() (string, string, string) {
	sql := strings.TrimSpace(strings.ToLower(q.query))
	a := strings.Index(sql, Select)
	if a == -1 {
		return "", "", ""
	}
	b := strings.Index(sql, From)
	if b == -1 {
		return sql, "", ""
	}
	s := strings.TrimSpace(sql[a+len(Select) : b])
	f := strings.TrimSpace(sql[b+len(From):])
	c := strings.Index(f, Where)
	if c == -1 {
		return s, f, ""
	}
	w := strings.TrimSpace(f[c+len(Where):])
	f = strings.TrimSpace(f[0:c])
	return s, f, w
}

func (q *Query) init() error {
	s, f, w := q.split()
	if s != "" {
		columns := strings.Split(s, ",")
		q.column = make([]string, 0)
		for _, col := range columns {
			q.column = append(q.column, strings.TrimSpace(col))
		}
	}
	if f != "" {
		tables := strings.Split(f, ",")
		q.tables = make([]string, 0)
		for _, tbl := range tables {
			q.tables = append(q.tables, strings.TrimSpace(tbl))
		}
	}
	if w != "" {
		where, e := parseExpression(w)
		if e != nil {
			return e
		}
		q.where = where
	}
	return nil
}
