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

func (c Client) GetAdmin() (client *f.FaunaClient) {
	return f.NewFaunaClient(c.Connection.Secret, f.Endpoint(c.Connection.Endpoint))
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

type Index struct {
	Name       string
	PrimaryKey string
	Collection Collection
}

func (c Client) CreateIndex(index Index) (bool, error) {
	result, err := c.Db.Query(
		f.If(
			f.Exists(f.Index(index.Name)),
			true,
			f.CreateIndex(f.Obj{
				"name":   index.Name,
				"source": f.Collection(index.Collection.Name),
				"unique": true,
				"terms": f.Obj{
					"field": f.Arr{"data", index.PrimaryKey},
				},
			}),
		),
	)

	if err != nil {
		panic(err)
	}

	if result != f.BooleanV(true) {
		log.Printf("Created Index: %s\n %s", index.Name, result)
		return true, err
	}
	log.Printf("Index: %s, Already Exists\n %s", index.Name, result)
	return false, err
}

type Instance struct {
	Collection Collection
	Data       f.Obj
	Ref        *f.RefV
}

func (c Client) CreateInstance(instance Instance) (bool, error) {
	ref := new(f.RefV)
	result, err := c.Db.Query(
		f.Create(f.Collection(instance.Collection.Name), f.Obj{
			"data": instance.Data,
		}),
	)
	if err != nil {
		log.Printf("Instance Existed '%s' : %s", instance.Collection.Name, result)
		return true, err
	}

	if err = result.At(f.ObjKey("ref")).Get(&ref); err == nil {
		log.Printf("created '%s'\n%v", instance.Collection.Name, result)
	} else {
		panic(err)
	}

	result, err = c.Db.Query(f.Select(f.Arr{"data", "name"}, f.Get(ref)))
	if err != nil {
		panic(err)
	}
	log.Printf("Read '%s'\n%s", instance.Collection.Name, result)
	return true, err
}
