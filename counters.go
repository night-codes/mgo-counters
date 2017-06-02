package counters

import (
	"gopkg.in/mgo.v2"
	"sync"
	"sync/atomic"
	"time"
)

type obj map[string]interface{}

var (
	session      = &mgo.Session{}
	collection   = &mgo.Collection{}
	idFieldName  = "id"
	seqFieldName = "seq"
)

type (
	Counter struct {
		sync.Mutex
		session    *mgo.Session
		collection *mgo.Collection
		data       map[string]*int64
	}
)

// Create new instance of Counter
func Create(c *mgo.Collection) *Counter {
	cn := &Counter{
		collection: c,
		session:    c.Database.Session,
		data:       map[string]*int64{},
	}
	cn.Lock()
	cn.connectionCheck()
	c.FindId("one").One(&cn.data)
	cn.Unlock()
	go func() {
		for range time.Tick(5 * time.Second) {
			cn.Save()
		}
	}()

	return cn
}

func (cn *Counter) Add(name string, delta ...int64) int64 {
	if len(delta) == 0 {
		delta = append(delta, 1)
	}
	var ret int64
	cn.Lock()
	defer cn.Unlock()
	if el, ok := cn.data[name]; !ok {
		var tmp int64
		cn.data[name] = &tmp
		atomic.StoreInt64(cn.data[name], delta[0])
		ret = tmp
	} else {
		ret = atomic.AddInt64(el, delta[0])
	}
	return ret
}

func (cn *Counter) Get(name string) int64 {
	cn.Lock()
	el, ok := cn.data[name]
	cn.Unlock()
	if ok {
		return *el
	}
	return 0
}

func (cn *Counter) Save() {
	cn.connectionCheck()
	cn.Lock()
	tempSetMap := cn.data
	cn.data = map[string]*int64{}
	cn.Unlock()
	cn.collection.UpsertId("one", obj{"$set": tempSetMap})
}

func (cn *Counter) connectionCheck() {
	if err := cn.session.Ping(); err != nil {
		cn.session.Refresh()
	}
}
