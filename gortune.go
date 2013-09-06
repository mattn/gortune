package gortune

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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

type schemaType int

const (
	String schemaType = iota
	Number
	DateTime
)

type Gortune struct {
	mux    *http.ServeMux
	db     *sql.DB
	driver string
}

type Schema map[string]interface{}

func NewGortune(config Config) (*Gortune, error) {
	db, err := sql.Open(config.Driver, config.DataSource)
	if err != nil {
		return nil, err
	}
	return &Gortune{
		mux: http.NewServeMux(),
		db:  db,
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

func (g *Gortune) putResource(name string, id string, schema Schema, w http.ResponseWriter, r *http.Request) {
	var values map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var args []interface{}
	n := 1
	fields := ""
	for k, v := range values {
		if n == 1 {
			fields += k + "=" + g.placeHolder(n)
		} else {
			fields += "," + k + "=" + g.placeHolder(n)
		}
		args = append(args, v)
		n++
	}
	if len(args) > 0 {
		args = append(args, id)
		sql := "update " + name + "set " + fields + " where id = " + g.placeHolder(n)
		_, err = g.db.Exec(sql, args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gortune) postResource(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	var values map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fs, vs := "", ""
	var args []interface{}
	n := 1
	for k, v := range values {
		if n == 1 {
			fs += k
			vs += g.placeHolder(n)
		} else {
			fs += "," + k
			vs += "," + g.placeHolder(n)
		}
		args = append(args, v)
		n++
	}
	sql := "insert into " + name + "(" + fs + ") values(" + vs + ")"
	fmt.Println(sql)
	_, err = g.db.Exec(sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (g *Gortune) deleteResource(name string, id string, schema Schema, w http.ResponseWriter, r *http.Request) {
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
	var values []map[string]interface{}
	for rows.Next() {
		var fields []interface{}
		item := make(map[string]interface{})
		for _ = range cols {
			fields = append(fields, new(interface{}))
		}
		rows.Scan(fields...)
		for i, col := range cols {
			item[col] = fields[i]
		}
		values = append(values, item)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(values)
}

func (g *Gortune) getResource(name string, id string, schema Schema, w http.ResponseWriter, r *http.Request) {
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
	var fields []interface{}
	item := make(map[string]interface{})
	for _ = range cols {
		fields = append(fields, new(interface{}))
	}
	err = rows.Scan(fields...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for i, col := range cols {
		item[col] = fields[i]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(item)
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
		id := r.URL.Path[len(name)+2:]
		switch r.Method {
		case "GET":
			if id == "" {
				g.listResource(name, schema, w, r)
			} else {
				g.getResource(name, id, schema, w, r)
			}
		case "PUT":
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
