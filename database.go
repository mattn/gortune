package gortune

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
)

func (g *Gortune) putDatabase(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if schema == nil {
		schema = make(map[string]interface{})
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
		k := rt.Field(i).Tag.Get("json")
		if k == "" {
			k = strings.ToLower(rt.Field(i).Name)
		}
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
func (g *Gortune) postDatabase(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if schema == nil {
		schema = make(map[string]interface{})
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
		k := rt.Field(i).Tag.Get("json")
		if k == "" {
			k = strings.ToLower(rt.Field(i).Name)
		}
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

func (g *Gortune) getDatabase(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
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

	if schema == nil {
		schema = make(map[string]interface{})
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
			fn := strings.ToLower(rt.Field(i).Name)
			if fn == "" {
				fn = rt.Field(f).Name
			}
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

func (g *Gortune) deleteDatabase(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
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

func (g *Gortune) listDatabase(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
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

	if schema == nil {
		schema = make(map[string]interface{})
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
				fn := strings.ToLower(rt.Field(i).Name)
				if fn == "" {
					fn = rt.Field(f).Name
				}
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
