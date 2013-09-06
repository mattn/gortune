package gortune

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

type Config struct {
	Driver     string
	DataSource string
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
	driver string
}

func Atoi64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

type Schema interface{}

func NewGortune(config Config) (*Gortune, error) {
	db, err := sql.Open(config.Driver, config.DataSource)
	if err != nil {
		return nil, err
	}
	return &Gortune{
		mux:    http.NewServeMux(),
		db:     db,
		driver: config.Driver,
	}, nil
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
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rt := reflect.TypeOf(schema).Elem()
	nv := reflect.New(rt)
	vv := nv.Interface()

	err = json.Unmarshal(b, &vv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fields := ""
	var args []interface{}
	l := rt.NumField()
	for i := 0; i < l; i++ {
		k := strings.ToLower(rt.Field(i).Name)
		v := nv.Elem().Field(i).Interface()
		if i == 0 {
			fields += k + "=" + g.placeHolder(i+1)
		} else {
			fields += "," + k + "=" + g.placeHolder(i+1)
		}
		args = append(args, v)
	}
	if len(args) > 0 {
		args = append(args, id)
		sql := "update " + name + " set " + fields + " where id = " + g.placeHolder(len(args))
		_, err := g.db.Exec(sql, args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var jv map[string]interface{}
		err = json.Unmarshal(b, &jv)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		jv["id"] = id
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gortune) postResource(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rt := reflect.TypeOf(schema).Elem()
	nv := reflect.New(rt)
	vv := nv.Interface()

	err = json.Unmarshal(b, &vv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fs, vs := "", ""
	var args []interface{}
	l := rt.NumField()
	for i := 0; i < l; i++ {
		k := strings.ToLower(rt.Field(i).Name)
		v := nv.Elem().Field(i).Interface()
		if i == 0 {
			fs += k
			vs += g.placeHolder(i + 1)
		} else {
			fs += "," + k
			vs += "," + g.placeHolder(i+1)
		}
		args = append(args, v)
	}
	sql := "insert into " + name + "(" + fs + ") values(" + vs + ")"
	res, err := g.db.Exec(sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		/*
		TODO: PostgreSQL doesn't work
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
		*/
		w.WriteHeader(http.StatusCreated)
	} else {
		var jv map[string]interface{}
		err = json.Unmarshal(b, &jv)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		jv["id"] = id
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(jv)
	}
}

func (g *Gortune) deleteResource(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	res, err := g.db.Exec("delete from "+name+" where id = "+g.placeHolder(1), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if n == 0 {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gortune) listResource(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	rows, err := g.db.Query("select * from " + name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rt := reflect.TypeOf(schema).Elem()

	values := make([]interface{}, 0)
	for rows.Next() {
		var fields []interface{}
		nv := reflect.New(rt)
		for _ = range cols {
			fields = append(fields, new(interface{}))
		}
		rows.Scan(fields...)

		var iid interface{}
		for i, col := range cols {
			if col == "id" {
				iid = fields[i]
				continue
			}
			for f := 0; f < nv.Elem().NumField(); f++ {
				fn := rt.Field(f).Name
				if strings.ToLower(fn) == col {
					nv.Elem().Field(f).Set(reflect.ValueOf(fields[i]).Elem().Elem().Convert(rt.Field(f).Type))
					break
				}
			}
		}
		b, err := json.Marshal(nv.Interface())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var jv map[string]interface{}
		err = json.Unmarshal(b, &jv)
		jv["id"] = iid
		values = append(values, jv)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(values)
}

func (g *Gortune) getResource(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	rows, err := g.db.Query("select * from "+name+" where id = "+g.placeHolder(1), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !rows.Next() {
		http.NotFound(w, r)
		return
	}

	rt := reflect.TypeOf(schema).Elem()
	nv := reflect.New(rt)

	var fields []interface{}
	for _ = range cols {
		fields = append(fields, new(interface{}))
	}
	err = rows.Scan(fields...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var iid interface{}
	for i, col := range cols {
			if col == "id" {
				iid = fields[i]
				continue
			}
			for f := 0; f < nv.Elem().NumField(); f++ {
				fn := rt.Field(f).Name
				if strings.ToLower(fn) == col {
					nv.Elem().Field(f).Set(reflect.ValueOf(fields[i]).Elem().Elem().Convert(rt.Field(f).Type))
					break
				}
			}
	}
	b, err := json.Marshal(nv.Interface())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var jv map[string]interface{}
	err = json.Unmarshal(b, &jv)
	jv["id"] = iid
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jv)
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
