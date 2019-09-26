package model

import "github.com/globalsign/mgo/bson"

type Rule struct {
	Id        bson.ObjectId `json:"id" bson:"_id"`
	ProjectId string        `json:"projectId" bson:"projectId"`
	DeviceId  string        `json:"deviceId" bson:"deviceId"`
	Action    Action        `json:"action" bson:"action"`
	Interval  int           `json:"interval" bson:"interval"`
	Label     string        `json:"label" bson:"label"`
}

type Action struct {
	Type string `json:"type" bson:"type"`
}
