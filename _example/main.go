package main

import (
	gt "github.com/mattn/gortune"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	gt.MustNewGortune(
		gt.Config{
			Driver: gt.SQLite3,
			DataSource: "foo.db",
		}).
		Resource("person", new(struct {
			Age int `json:"age"`
			Name string `json:"name"`})).
		ListenAndServe(":8889", nil)
}
