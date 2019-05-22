package persistency

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/saichler/habitat-orm/golang/common"
	. "github.com/saichler/habitat-orm/golang/registry"
	. "github.com/saichler/utils/golang"
	"reflect"
	"strconv"
)

type Postgres struct {
	host   string
	port   int
	user   string
	pass   string
	dbname string
	schema string
	db     *sql.DB
	tx     *sql.Tx
}

func NewPostgresPersistency2(db *sql.DB) *Postgres {
	p := &Postgres{}
	p.db = db
	return p
}

func NewPostgresPersistency1(host string, port int, user, pass, dbname, schema string) *Postgres {
	p := &Postgres{}
	p.host = host
	p.port = port
	p.user = user
	p.pass = pass
	p.dbname = dbname
	p.schema = schema
	if p.host == "" {
		p.host = "127.0.0.1"
	}
	if p.port == 0 {
		p.port = 5432
	}
	if p.user == "" {
		p.user = "postgres"
	}
	if p.pass == "" {
		p.pass = "orm"
	}
	if p.dbname == "" {
		p.dbname = "orm"
	}
	if p.schema == "" {
		p.schema = "orm"
	}
	return p
}

func (p *Postgres) Init(registry *OrmRegistry) error {
	if p.db == nil {
		p.connect()
	}
	p.createSchema(registry)
	return nil
}

func (p *Postgres) connect() {
	connInfo := NewStringBuilder("")
	connInfo.Append("host=").Append(p.host).Append(" ")
	connInfo.Append("port=").Append(strconv.Itoa(p.port)).Append(" ")
	connInfo.Append("user=").Append(p.user).Append(" ")
	connInfo.Append("password=").Append(p.pass).Append(" ")
	connInfo.Append("dbname=").Append(p.dbname).Append(" sslmode=disable")
	db, err := sql.Open("postgres", connInfo.String())
	p.db = db
	if err != nil {
		panic("Unable to connect to database:" + err.Error())
	}
}

func (p *Postgres) createSchema(r *OrmRegistry) {
	schemaCreateSql := NewStringBuilder("CREATE SCHEMA IF NOT EXISTS ")
	schemaCreateSql.Append(p.schema).Append(";")
	_, err := p.db.Exec(schemaCreateSql.String())
	if err != nil {
		panic("Unable to create schema " + p.schema + ":" + err.Error())
	}

	for tableName, table := range r.Tables() {
		createSql := NewStringBuilder("CREATE TABLE IF NOT EXISTS ")
		createSql.Append(p.schema).Append(".").Append(tableName).Append(" (\n")
		createSql.Append("    ").Append(common.RECORD_LEVEL).Append("    ").Append("integer DEFAULT 0,\n")
		if table.Indexes().PrimaryIndex() == nil {
			createSql.Append("    ").Append(common.RECORD_ID).Append("    ").Append("VARCHAR(256),\n")
			createSql.Append("    ").Append(common.RECORD_INDEX).Append("    ").Append("integer DEFAULT 0,\n")
		}
		for _, column := range table.Columns() {
			if column.MetaData().Ignore() {
				continue
			}
			columnName := column.Name()
			//User is a keywork in postgres, hence need to change it
			if columnName == "User" {
				columnName = "_" + columnName
			}
			createSql.Append("    ").Append(columnName).Append("    ")
			kind := column.Type().Kind()
			size := strconv.Itoa(column.MetaData().Size())
			if kind == reflect.Ptr {
				createSql.Append("VARCHAR(256),\n")
			} else if kind == reflect.Slice {
				createSql.Append("VARCHAR(256),\n")
			} else if kind == reflect.String {
				createSql.Append("VARCHAR(").Append(size).Append("),\n")
			} else if kind == reflect.Int32 || kind == reflect.Uint32 || kind == reflect.Int || kind == reflect.Uint {
				createSql.Append("integer DEFAULT 0,\n")
			} else if kind == reflect.Int64 || kind == reflect.Uint64 {
				createSql.Append("bigint DEFAULT 0,\n")
			} else if kind == reflect.Float64 || kind == reflect.Float32 {
				createSql.Append("decimal DEFAULT 0,\n")
			} else if kind == reflect.Bool {
				createSql.Append("boolean DEFAULT FALSE,\n")
			} else if kind == reflect.Map {
				createSql.Append("VARCHAR(256),\n")
			} else {
				panic("Unsupported field type:" + kind.String())
			}
		}

		if table.Indexes().PrimaryIndex() == nil {
			tmpsql := createSql.String()
			tmpsql = tmpsql[0:len(tmpsql)-2] + "\n);"
			createSql = NewStringBuilder(tmpsql)
		} else {
			primaryKey := NewStringBuilder("PRIMARY KEY (")
			first := true
			for _, column := range table.Indexes().PrimaryIndex().Columns() {
				if first {
					primaryKey.Append(column.Name())
					first = false
				} else {
					primaryKey.Append(", ").Append(column.Name())
				}
			}
			primaryKey.Append(")")
			createSql.Append(primaryKey.String()).Append("\n);")
		}
		_, err := p.db.Exec(createSql.String())
		if err != nil {
			panic("Failed to execute sql:" + createSql.String() + " error=" + err.Error())
		}
	}
	p.tx, err = p.db.Begin()
}

func (p *Postgres) DB() *sql.DB {
	return p.db
}

func (p *Postgres) Schema() string {
	return p.schema
}

func (p *Postgres) TableName(table *Table) string {
	return p.schema + "." + table.Name()
}
