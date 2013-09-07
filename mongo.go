package gortune

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
)

func (g *Gortune) putMongo(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
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

	err = g.mongo.C(name).UpdateId(id, vv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(vv)
	}
}

func (g *Gortune) postMongo(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
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

	err = g.mongo.C(name).Insert(vv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(vv)
	}
}

func (g *Gortune) deleteMongo(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	err := g.mongo.C(name).RemoveId(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gortune) getMongo(name string, id int64, schema Schema, w http.ResponseWriter, r *http.Request) {
	rt := reflect.TypeOf(schema).Elem()
	nv := reflect.New(rt)
	vv := nv.Interface()

	g.mongo.C(name).FindId(id).One(&vv)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(vv)
}

func (g *Gortune) listMongo(name string, schema Schema, w http.ResponseWriter, r *http.Request) {
	values := make([]interface{}, 0)
	g.mongo.C(name).Find(values)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(values)
}
