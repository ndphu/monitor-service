package dao

import (
	"crypto/tls"
	"github.com/globalsign/mgo"
	"log"
	"monitor-service/config"
	"net"
)

type DAO struct {
	Session *mgo.Session
	DBName  string
}

var (
	dao *DAO = nil
)

func init()  {
	conf := config.Get()


	dialInfo, err:= mgo.ParseURL(conf.MongoDBUri)
	if err != nil {
		panic(err)
	}
	if conf.MongoDBUserSSL {
		tlsConfig := &tls.Config{}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	}

	session, err := mgo.DialWithInfo(dialInfo)

	dbName := ""

	if conf.DBName == "" {
		dbs, err := session.DatabaseNames()
		if err != nil {
			log.Println("fail to connect to database")
			panic(err)
		} else {
			if len(dbs) == 0 {
				log.Println("no database found")
			} else {
				log.Println("found databases " + dbs[0])
			}
			dbName = dbs[0]
		}
	} else {
		dbName = conf.DBName
	}

	dao = &DAO{
		Session: session,
		DBName:  dbName,
	}
}

func Collection(name string) *mgo.Collection {
	return dao.Session.DB(dao.DBName).C(name)
}

func GetSession() *mgo.Session {
	return dao.Session
}