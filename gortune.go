package gortune

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
)

type Config struct {
	Driver     string
	DataSource string
}

type schemaType int

const (
	String schemaType = iota
	Number
)

type Gortune struct {
	Mux *http.ServeMux
	DB  *sql.DB
}

type Schema map[string]interface{}

func NewGortune(config Config) (*Gortune, error) {
	db, err := sql.Open(config.Driver, config.DataSource)
	if err != nil {
		return nil, err
	}
	return &Gortune{
		Mux: http.NewServeMux(),
		DB:  db,
	}, nil
}

func MustNewGortune(config Config) *Gortune {
	g, err := NewGortune(config)
	if err != nil {
		panic(err.Error())
	}
	return g
}

func (g *Gortune) putResource(name string, id string, values map[string]interface{}, w http.ResponseWriter, r *http.Request) {
	err := json.NewDecoder(r.Body).Decode(&values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	first := true
	fields := ""
	var args []interface{}
	for k, v := range values {
		if first {
			first = false
			fields += k + "=?"
		} else {
			fields += "," + k + "=?"
		}
		args = append(args, v)
	}
	args = append(args, id)
	sql := "update " + name + "set " + fields + " where id = ?"
	_, err = g.DB.Exec(sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gortune) postResource(name string, values map[string]interface{}, w http.ResponseWriter, r *http.Request) {
	err := json.NewDecoder(r.Body).Decode(&values)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	first := true
	fs, vs := "", ""
	var args []interface{}
	for k, v := range values {
		if first {
			first = false
			fs += k
			vs += "?"
		} else {
			fs += "," + k
			vs += ",?"
		}
		args = append(args, v)
	}
	sql := "insert into " + name + "(" + fs + ") vs(" + vs + ")"
	_, err = g.DB.Exec(sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (g *Gortune) deleteResource(name string, id string, w http.ResponseWriter, r *http.Request) {
	res, err := g.DB.Exec("delete from "+name+" where id = ?", id)
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

func (g *Gortune) listResource(name string, w http.ResponseWriter, r *http.Request) {
	rows, err := g.DB.Query("select * from " + name)
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

func (g *Gortune) getResource(name string, id string, w http.ResponseWriter, r *http.Request) {
	rows, err := g.DB.Query("select * from "+name+" where id = ?", id)
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
	g.Mux.HandleFunc("/"+name+"", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			g.listResource(name, w, r)
		case "POST":
			g.postResource(name, schema, w, r)
		default:
			http.NotFound(w, r)
		}
	})
	g.Mux.HandleFunc("/"+name+"/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len(name)+2:]
		switch r.Method {
		case "GET":
			if id == "" {
				g.listResource(name, w, r)
			} else {
				g.getResource(name, id, w, r)
			}
		case "PUT":
			g.putResource(name, id, schema, w, r)
		case "DELETE":
			g.deleteResource(name, id, w, r)
		default:
			http.NotFound(w, r)
		}
	})
	return g
}

func (g *Gortune) ListenAndServe(addr string, handler http.Handler) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return http.Serve(l, g.Mux)
}
