package main

import (
	"./gosqs"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
)

var db *gosqs.DB

var dbPath string

var port = flag.String("port", "8080", "http service port")

func init() {
	flag.StringVar(&dbPath, "dbpath", "/usr/local/work/leveldb/gosqs", "leveldb path")
	d, err := gosqs.NewDb(dbPath)
	if err != nil {
		log.Println("db error")
		log.Fatal(err)
	}
	db = d
}

func ConnectDB(dbpath string) (*gosqs.DB, error) {
	return gosqs.NewDb(dbpath)
}

func Get(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")

	if len(key) > 80 {
		w.Write([]byte("{error:\"1\"}"))
		return
	}
	value, err := db.Get(key)
	if err != nil {
		log.Println(err)
		w.Write([]byte("{error:\"1\"}"))
		return
	}

	w.Write([]byte("{error:\"0\",value:" + value + "}"))

}

func Set(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	value := r.FormValue("value")

	err := db.Set(key, value)
	if err != nil {
		log.Println(err.Error())
		w.Write([]byte("{error:\"1\"}"))
		return
	}
	w.Write([]byte("{error:\"0\"}"))
}

func Status(w http.ResponseWriter, r *http.Request) {
	html := "<table><tr><td>key</td><td>value_number</td></tr>"
	channels, _ := db.GetChannel()
	for _, v := range channels {
		getpos := db.GetGetPos(v)
		setpos := db.GetSetPos(v)

		html += "<tr><td>" + v + "</td><td>" + strconv.Itoa(setpos-getpos) + "</td></tr>"
	}
	html += "</table>"
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	http.HandleFunc("/get", Get)
	http.HandleFunc("/set", Set)
	http.HandleFunc("/status", Status)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
