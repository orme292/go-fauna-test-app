package fdata

import (
	"log"

	f "github.com/fauna/faunadb-go/v4/faunadb"
)

type Connection struct {
	Secret   string
	Endpoint string
	DbName   string
}

type Client struct {
	Connection Connection
	Admin      *f.FaunaClient
	Db         *f.FaunaClient
	SpecSecret string
}

func (c Connection) GetAdmin() (client *f.FaunaClient) {
	return f.NewFaunaClient(c.Secret, f.Endpoint(c.Endpoint))
}

func (c Client) GetDb() (dbclient *f.FaunaClient) {
	result, err := c.Admin.Query(
		f.CreateKey(f.Obj{
			"database": f.Database(c.Connection.DbName),
			"role":     "server",
		}))

	if err != nil {
		log.Println("Fail after c.Admin.Query")
		panic(err)
	}

	err = result.At(f.ObjKey("secret")).Get(&c.SpecSecret)

	if err != nil {
		panic(err)
	}

	log.Printf("Database: %s, specific key: %s\n%s", c.Connection.DbName, c.SpecSecret, result)

	dbclient = c.Admin.NewSessionClient(c.SpecSecret)

	return
}

func (c Client) CreateDb() (bool, error) {
	result, err := c.Admin.Query(
		f.If(
			f.Exists(f.Database(c.Connection.DbName)),
			true,
			f.CreateDatabase(f.Obj{"name": c.Connection.DbName}),
		),
	)

	if err != nil {
		panic(err)
	}

	if result != f.BooleanV(true) {
		log.Printf("Created Database: %s\n %s", c.Connection.DbName, result)
		return true, err
	}
	log.Printf("Database: %s, Already Exists\n %s", c.Connection.DbName, result)
	return false, err
}

type Collection struct {
	Name              string
	ContentRetainDays int32
	HistoryRetainDays int32
	Data              f.Obj
}

func (c Client) CreateCollection(collection Collection) (bool, error) {
	result, err := c.Db.Query(
		f.If(
			f.Exists(f.Collection(collection.Name)),
			true,
			f.CreateCollection(f.Obj{
				"name":         collection.Name,
				"ttl_days":     collection.ContentRetainDays,
				"history_days": collection.HistoryRetainDays,
				"data":         collection.Data,
			}),
		),
	)

	if err != nil {
		panic(err)
	}

	if result != f.BooleanV(true) {
		log.Printf("Created Collection: %s\n%s", collection.Name, result)
		return true, err
	}
	log.Printf("Collection: %s, Already Exists\n%s", collection.Name, result)
	return false, err
}
