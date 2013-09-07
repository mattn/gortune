package gortune

import (
	"database/sql"
	"fmt"
	"labix.org/v2/mgo"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

type Config struct {
	Driver     string
	DataSource string
	Database   string
}

const (
	SQLite3    = "sqlite3"
	PostgreSQL = "postgres"
	MySQL      = "mysql"
	MongoDB    = "mongodb"
)

type dataType int

const (
	String dataType = iota
	Integer
	Float
	DateTime
)

type Gortune struct {
	mux    *http.ServeMux
	db     *sql.DB
	mongo  *mgo.Database
	driver string
}

func Atoi64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

type Schema interface{}

func NewGortune(config Config) (*Gortune, error) {
	g := &Gortune{
		mux:    http.NewServeMux(),
		driver: config.Driver,
	}
	var err error
	if config.Driver == MongoDB {
		var session *mgo.Session
		session, err = mgo.Dial(config.DataSource)
		if err != nil {
			return nil, err
		}
		g.mongo = session.DB(config.Database)
	} else {
		g.db, err = sql.Open(config.Driver, config.DataSource)
	}
	if err != nil {
		return nil, err
	}
	return g, nil
}

func MustNewGortune(config Config) *Gortune {
	g, err := NewGortune(config)
	if err != nil {
		panic(err.Error())
	}
	return g
}

func (g *Gortune) placeHolder(n int) string {
	if g.driver == PostgreSQL {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func (g *Gortune) putResource(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	if g.driver == MongoDB {
		g.putMongo(name, id, schema, w, r)
	} else {
		g.putDatabase(name, id, schema, w, r)
	}
}

func (g *Gortune) postResource(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	if g.driver == MongoDB {
		g.postMongo(name, schema, w, r)
	} else {
		g.postDatabase(name, schema, w, r)
	}
}

func (g *Gortune) deleteResource(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	if g.driver == MongoDB {
		g.deleteMongo(name, id, schema, w, r)
	} else {
		g.deleteDatabase(name, id, schema, w, r)
	}
}

func (g *Gortune) listResource(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	if g.driver == MongoDB {
		g.listMongo(name, schema, w, r)
	} else {
		g.listDatabase(name, schema, w, r)
	}
}

func (g *Gortune) createTable(name string, schema Schema) error {
	if g.driver == MongoDB {
		return nil
	}
	sql := "select count(*) from " + name
	_, err := g.db.Exec(sql)
	if err == nil {
		return nil
	}

	rt := reflect.TypeOf(schema).Elem()
	l := rt.NumField()

	sql = "create table " + name + "(id integer primary key"
	for i := 0; i < l; i++ {
		k := rt.Field(i).Tag.Get("json")
		if k == "" {
			k = strings.ToLower(rt.Field(i).Name)
		}
		sql += "," + k + " "

		typ := "text"
		switch rt.Field(i).Type.Kind() {
		case reflect.Bool:
			typ = "boolean"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			typ = "integer"
		case reflect.Int64:
			typ = "bigint"
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			typ = "unsigned integer"
		case reflect.Uint64:
			typ = "unsigned bigint"
		case reflect.Float32:
			typ = "float"
		case reflect.Float64:
			typ = "double"
		case reflect.String:
			typ = "text"
		default:
			typ = "text"
		}
		sql += typ
	}
	sql += ")"
	println(sql)
	_, err = g.db.Exec(sql)
	return err
}

func (g *Gortune) getResource(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	if g.driver == MongoDB {
		g.getMongo(name, id, schema, w, r)
	} else {
		g.getDatabase(name, id, schema, w, r)
	}
}

func (g *Gortune) Resource(name string, schema Schema) *Gortune {
	g.mux.HandleFunc("/"+name+"", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			g.listResource(name, schema, w, r)
		case "POST":
			g.postResource(name, schema, w, r)
		default:
			http.NotFound(w, r)
		}
	})
	g.mux.HandleFunc("/"+name+"/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path[len(name)+2:]
		var id int64
		var err error
		if p != "" {
			id, err = Atoi64(p)
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}
		switch r.Method {
		case "GET":
			if p == "" {
				g.listResource(name, schema, w, r)
			} else {
				g.getResource(name, id, schema, w, r)
			}
		case "PUT", "POST":
			g.putResource(name, id, schema, w, r)
		case "DELETE":
			g.deleteResource(name, id, schema, w, r)
		default:
			http.NotFound(w, r)
		}
	})
	g.createTable(name, schema)
	return g
}

func (g *Gortune) Serve(l net.Listener, handler http.Handler) error {
	return http.Serve(l, g.mux)
}

func (g *Gortune) ListenAndServe(addr string, handler http.Handler) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return http.Serve(l, g.mux)
}
