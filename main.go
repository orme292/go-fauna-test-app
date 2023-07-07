package main

import (
	"log"
	"os"
	"time"

	f "github.com/fauna/faunadb-go/v4/faunadb"
	"github.com/orme292/go-fauna-test-app/fdata"
	"gopkg.in/yaml.v2"
)

/*
Messing around with FaunaDB's Go driver
*/
func main() {

	secret := getSecret("./fauna.yaml")

	connection := fdata.Connection{
		Secret:   secret,
		Endpoint: "https://db.fauna.com",
		DbName:   "testdb",
	}

	client := fdata.Client{
		Connection: connection,
		Admin:      connection.GetAdmin(),
	}

	result, err := client.CreateDb()
	if result != true && err != nil {
		log.Printf("client.CreateDb() failed with result:%s\n%s", result, err)
	}
	client.Db = client.GetDb()

	userCollection := fdata.Collection{
		Name:              "users",
		ContentRetainDays: 10,
		HistoryRetainDays: 10,
		Data: f.Obj{
			"createdBy": "go-fauna-test-app",
			"createOn":  time.Now().Format("01-02-2023 15:04"),
		},
	}

	result, err = client.CreateCollection(userCollection)
	if result != true && err != nil {
		log.Printf("client.CreateCollection failed with results: %s\n%s", result, err)
	}
}

type FaunaSecret struct {
	Secret string `yaml:"secret"`
}

func getSecret(filename string) (secret string) {
	var faunaSecret FaunaSecret

	log.Printf("Parsing YAML file [%s]..\n", filename)
	yamlFile, err := os.ReadFile(filename)
	err = yaml.Unmarshal(yamlFile, &faunaSecret)
	if err != nil {
		log.Printf("Error parsing the YAML: %s\n", err)
		return ""
	}
	return faunaSecret.Secret
}
