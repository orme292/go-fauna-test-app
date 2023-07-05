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
		f.CreateCollection(f.Obj{
			"name": collectionName,
		}))

	if err != nil {
		panic(err)
	}

	log.Printf("Created Collection: %s\n%s", collectionName, res)
}
func main() {
	log.Printf("Environment Variable FAUNASECRET: %s\n", secret)
	createDatabase()
	dbClient := getDbClient()
	createCollection("SampleCollection", dbClient)
}
