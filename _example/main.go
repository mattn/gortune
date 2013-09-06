package main

import (
	"github.com/mattn/gortune"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	gortune.MustNewGortune(gortune.Config{Driver: "sqlite3", DataSource: "foo.db"}).
		Resource("person", nil).
		ListenAndServe(":8889", nil)
}
