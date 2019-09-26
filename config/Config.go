package config

import (
	"log"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	MongoDBUri     string
	DBName         string
	MQTTBroker     string
	GinDebug       bool
	MongoDBUserSSL bool
}

type MongoDBCredential struct {
	Uri string
}

type MongoDBConfig struct {
	Name        string
	Credentials MongoDBCredential
}

var conf *Config

func init() {
	conf = &Config{}
	conf.MongoDBUri = os.Getenv("MONGODB_URI")

	if os.Getenv("MONGODB_DB_NAME") == "" {
		conf.DBName = getDBName(conf.MongoDBUri)
	} else {
		conf.DBName = os.Getenv("MONGODB_DB_NAME")
		log.Println("[CONFIG]", "Using database", conf.DBName)
	}

	conf.GinDebug = os.Getenv("GIN_DEBUG") == "true"

	conf.MQTTBroker = os.Getenv("MQTT_BROKER")
	if conf.MQTTBroker == "" {
		conf.MQTTBroker = "tcp://35.197.155.112:4443"
	}

	log.Println("[CONFIG]", "Using MQTTBroker", conf.MQTTBroker)
}

func Get() *Config {
	return conf
}

func getDBName(mongodbUri string) string {
	parsed, e := url.Parse(mongodbUri)
	if e != nil {
		panic(e)
	}
	return strings.Trim(parsed.Path, "/")
}
