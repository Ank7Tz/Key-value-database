package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	fmt.Println("key value server!")
	db, err := leveldb.OpenFile("./db", nil)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer db.Close()

	err = db.Put([]byte("k1"), []byte("value1"), nil)
	if err != nil {
		log.Fatalln(err.Error())
	}

	data, err := db.Get([]byte("k1"), nil)
	if err != nil {
		log.Fatalln(err.Error())
	}

	fmt.Println(string(data))
}
