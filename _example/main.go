package main

import (
	_ "github.com/mattn/go-sqlite3"
	gt "github.com/mattn/gortune"
)

func main() {
	gt.MustNewGortune(
		gt.Config{
			Driver:     gt.SQLite3,
			DataSource: "foo.db",
		}).
		Resource("person", new(
		struct {
			Age  int    `json:"age"`
			Name string `json:"name"`
		})).
		ListenAndServe(":8889", nil)
}
