package persistency

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/utils/golang"
	"strconv"
)

type Postgres struct {
	host string
	port int
	user string
	pass string
	dbname string
	schema string
	db *sql.DB
}

func NewPostgresPersistency1(host string, port int, user,pass,dbname,schema string) *Postgres {
	p:=&Postgres{}
	p.host = host
	p.port = port
	p.user = user
	p.pass = pass
	p.dbname = dbname
	p.schema = schema
	if p.host=="" {
		p.host="127.0.0.1"
	}
	if p.port==0 {
		p.port = 5432
	}
	if p.user=="" {
		p.user="postgres"
	}
	if p.pass=="" {
		p.pass = "orm"
	}
	if p.dbname=="" {
		p.dbname="orm"
	}
	if p.schema=="" {
		p.schema="orm"
	}
	return p
}

func (p *Postgres) Init(registry *OrmRegistry) error {
	p.connect()
	p.createSchema()
	return nil
}

func (p *Postgres) connect() {
	connInfo:=NewStringBuilder("")
	connInfo.Append("host=").Append(p.host).Append(" ")
	connInfo.Append("port=").Append(strconv.Itoa(p.port)).Append(" ")
	connInfo.Append("user=").Append(p.user).Append(" ")
	connInfo.Append("password=").Append(p.pass).Append(" ")
	connInfo.Append("dbname=").Append(p.dbname).Append(" sslmode=disable")
	fmt.Println(connInfo.String())
	db, err := sql.Open("postgres", connInfo.String())
	p.db = db
	if err!=nil {
		panic("Unable to connect to database:"+err.Error())
	}
}

func (p *Postgres) createSchema() {
	schemaCreateSql:=NewStringBuilder("CREATE SCHEMA IF NOT EXISTS ")
	schemaCreateSql.Append(p.schema).Append(";")
	_,err := p.db.Exec(schemaCreateSql.String())
	if err!=nil {
		panic("Unable to create schema "+p.schema+":"+err.Error())
	}
}
