package gosqs

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"log"
	"strconv"
	"strings"
	"sync"
)

type Db interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
}

type DB struct {
	stor *storage.FileStorage
	db   *leveldb.DB
	opts *opt.Options
	ro   *opt.ReadOptions
	wo   *opt.WriteOptions
	lock sync.Mutex
}

func NewDb(dbpath string) (*DB, error) {
	stor, err := storage.OpenFile(dbpath)
	if err != nil {
		return nil, err
	}
	opts := &opt.Options{Flag: opt.OFCreateIfMissing}
	db, err := leveldb.Open(stor, opts)

	if err != nil {
		return nil, err
	}

	ro := &opt.ReadOptions{}
	wo := &opt.WriteOptions{}
	return &DB{stor: stor, db: db, opts: opts, ro: ro, wo: wo}, nil
}

func (db *DB) Close() {
	db.db.Close()
}

func (db *DB) Get(key string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	if ok := db.FindChannel(key); !ok {
		return "", errors.New("no this channel")
	}
	getpos := db.GetGetPos(key)
	setpos := db.GetSetPos(key)
	if getpos == setpos {
		getpos = 0
		setpos = 0
		db.SetGetPos(key, getpos)
		db.SetSetPos(key, setpos)
		return "", errors.New("Sqs is empty") //消息队列为空
	}
	channel := key + ":" + strconv.Itoa(getpos+1)

	value, err := db.db.Get([]byte(channel), db.ro)

	if err != nil {
		return "", err
	}
	err = db.Delete(key)
	if err != nil {
		return "", err
	}
	err = db.SetGetPos(key, getpos+1)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (db *DB) Set(key, value string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	if ok := db.FindChannel(key); !ok {
		err := db.SetChannel(key)
		if err != nil {
			return errors.New("set channel error")
		}
		db.SetGetPos(key, 0)
		db.SetSetPos(key, 0)
		err = db.db.Put([]byte(key+":1"), []byte(value), db.wo)
		if err == nil {
			db.SetSetPos(key, 1)
		}
		return err
	}
	setpos := db.GetSetPos(key)

	channel := key + ":" + strconv.Itoa(setpos+1)

	err := db.db.Put([]byte(channel), []byte(value), db.wo)
	if err == nil {
		err := db.SetSetPos(key, setpos+1)
		if err != nil {
			return err
		}
		return nil
	} else {
		return err
	}
}

func (db *DB) Delete(key string) error {
	return db.db.Delete([]byte(key), db.wo)
}

func (db *DB) GetGetPos(key string) int {
	getpos, err := db.db.Get([]byte(key+":getpos"), db.ro)
	spos := string(getpos)
	if err != nil || spos == "" {
		return 0
	}
	i, err := strconv.Atoi(spos)

	if err != nil {
		return 0
	}
	return i

}

func (db *DB) SetGetPos(key string, pos int) error {

	key = key + ":getpos"

	return db.db.Put([]byte(key), []byte(strconv.Itoa(pos)), db.wo)

}

func (db *DB) GetSetPos(key string) int {

	setpos, err := db.db.Get([]byte(key+":setpos"), db.ro)
	spos := string(setpos)
	if err != nil || spos == "" {
		log.Println(err.Error())
		return 0
	}
	i, err := strconv.Atoi(spos)
	if err != nil {
		return 0
	}
	return i
}

func (db *DB) SetSetPos(key string, pos int) error {

	key = key + ":setpos"

	return db.db.Put([]byte(key), []byte(strconv.Itoa(pos)), db.wo)
}

func (db *DB) GetChannel() ([]string, error) {
	channels := make([]string, 0)
	channel, err := db.db.Get([]byte("channel"), db.ro)
	if err != nil {
		return channels, err
	}

	channels = strings.Split(string(channel), ",")
	return channels, nil
}

func (db *DB) SetChannel(channel string) error {

	channels, _ := db.GetChannel()
	channels = append(channels, channel)
	return db.db.Put([]byte("channel"), []byte(strings.Join(channels, ",")), db.wo)
}

func (db *DB) DelChannel(channel string) error {

	channels, err := db.GetChannel()
	if err != nil {
		return err
	}
	var index int
	for k, v := range channels {
		if v == channel {
			index = k
			break
		}
	}
	channels = append(channels[:index], channels[index+1:]...)

	return db.db.Put([]byte("channel"), []byte(strings.Join(channels, ",")), db.wo)
}

func (db *DB) FindChannel(channel string) bool {
	channels, err := db.GetChannel()
	if err != nil {
		return false
	}

	for _, v := range channels {
		if v == channel {
			return true
		}
	}
	return false
}
