package main

import (
	"log"
	"os"

	f "github.com/fauna/faunadb-go/v4/faunadb"
)

var (
	secret   = os.Getenv("FAUNASECRET")
	endpoint = f.Endpoint("https://db.fauna.com")

	adminClient = f.NewFaunaClient(secret, endpoint)

	dbName = "go-fauna-test-app"
)

func createDatabase() {
	res, err := adminClient.Query(
		f.If(
			f.Exists(f.Database(dbName)),
			true,
			f.CreateDatabase(f.Obj{"name": dbName}),
		),
	)

	if err != nil {
		panic(err)
	}

	if res != f.BooleanV(true) {
		log.Printf("Created Database: %s\n %s", dbName, res)
	} else {
		log.Printf("Database: %s, Already Exists\n %s", dbName, res)
	}
}

func getDbClient() (dbClient *f.FaunaClient) {
	var res f.Value
	var err error

	var dbSecret string

	res, err = adminClient.Query(
		f.CreateKey(f.Obj{
			"database": f.Database(dbName),
			"role":     "server",
		}))

	if err != nil {
		panic(err)
	}

	err = res.At(f.ObjKey("secret")).Get(&dbSecret)

	if err != nil {
		panic(err)
	}

	log.Printf("Database: %s, specific key: %s\n%s", dbName, dbSecret, res)

	dbClient = adminClient.NewSessionClient(dbSecret)

	return
}

func createCollection(collectionName string, dbClient *f.FaunaClient) {

	res, err := dbClient.Query(
		f.If(
			f.Exists(f.Collection(collectionName)),
			true,
			f.CreateCollection(f.Obj{"name": collectionName}),
		),
	)

	if err != nil {
		panic(err)
	}

	if res != f.BooleanV(true) {
		log.Printf("Created Collection: %s\n%s", collectionName, res)
	} else {
		log.Printf("Collection: %s, Already Exists\n%s", collectionName, res)
	}
}

func createInstance(dbClient *f.FaunaClient, collectionName string, id int, name string) {
	var res f.Value
	var err error

	var ref f.RefV

	res, err = dbClient.Query(
		f.Create(f.Collection(collectionName), f.Obj{
			"data": f.Obj{
				"id":   id,
				"name": name,
			}}),
	)

	if err != nil {
		log.Printf("Instance Existed '%s' : %v : %s", collectionName, id, res)
	}

	if err = res.At(f.ObjKey("ref")).Get(&ref); err == nil {
		log.Printf("created '%s': %v \n%s", collectionName, id, res)
	} else {
		panic(err)
	}

	res, err = dbClient.Query(f.Select(f.Arr{"data", "name"}, f.Get(ref)))

	if err != nil {
		panic(err)
	}
	log.Printf("Read '%s': %v \n%s", collectionName, id, res)
}

func createIndex(dbClient *f.FaunaClient, indexName string, collectionName string, primaryKey string) {
	res, err := dbClient.Query(
		f.If(
			f.Exists(f.Index(indexName)),
			true,
			f.CreateIndex(f.Obj{
				"name":   indexName,
				"source": f.Collection(collectionName),
				"unique": true,
				"terms":  f.Obj{"field": f.Arr{"data", primaryKey}},
			})))

	if err != nil {
		panic(err)
	}

	if res != f.BooleanV(true) {
		log.Printf("Created Index: %s\n %s", indexName, res)
	} else {
		log.Printf("Index: %s, Already Exists\n %s", indexName, res)
	}
}

func getInstanceByPrimaryKey(dbClient *f.FaunaClient, indexName string, primaryKey int) {

	res, err := dbClient.Query(
		f.Select(f.Arr{"data", "name"},
			f.Get(f.MatchTerm(f.Index(indexName), primaryKey))))

	if err != nil {
		panic(err)
	}
	log.Printf("Read by Primary Key %s: %v : %s", indexName, primaryKey, res)
}

func main() {
	log.Printf("Environment Variable FAUNASECRET: %s\n", secret)
	createDatabase()

	dbClient := getDbClient()

	collectionName := "Customers"
	createCollection(collectionName, dbClient)

	indexName := "customer_by_id"
	primaryKey := "id"
	createIndex(dbClient, indexName, collectionName, primaryKey)

	createInstance(dbClient, collectionName, 1, "Adam Smith")
	createInstance(dbClient, collectionName, 2, "David Ricardo")
	createInstance(dbClient, collectionName, 3, "John Maynard Keynes")
	createInstance(dbClient, collectionName, 4, "Frederick Hayek")

	getInstanceByPrimaryKey(dbClient, indexName, 1)
	getInstanceByPrimaryKey(dbClient, indexName, 2)
	getInstanceByPrimaryKey(dbClient, indexName, 3)
	getInstanceByPrimaryKey(dbClient, indexName, 4)
}
