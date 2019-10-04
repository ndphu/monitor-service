package monitor

import (
	"encoding/json"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hako/durafmt"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"log"
	"monitor-service/config"
	"monitor-service/db"
	"time"
)

var missingThresholdMinutes = 2

func monitorEvents() {
	opts := service.NewClientOpts(config.Get().MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe(model.TopicEventBroadcast, 0, func(client mqtt.Client, message mqtt.Message) {
			e := model.Event{}
			if err := json.Unmarshal(message.Payload(), &e); err != nil {
				log.Println("[MQTT]", "Fail to unmarshal message", string(message.Payload()))
				return
			}
			log.Println("[MQTT]", "Event received", string(message.Payload()))
			go handleEvent(&e)
		}).Wait()
		log.Println("[MQTT]", "Subscribed to event broad cast topic")
	}
	c := mqtt.NewClient(opts)

	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalln("[MQTT]", "Fail to connect to message broker", tok.Error())
	}

	<-shutdownChan
	log.Println("[EVENT_MONITOR]", "Exiting event monitoring...")
	defer c.Disconnect(100)
}

func handleEvent(evt *model.Event) {
	log.Println("[EVENT_MONITOR]", "Handling event", evt.Type, evt.Result, "of device", evt.DeviceId)

	isUserPresent := evt.Result == model.ResultPresent
	log.Println("[MONITOR]", "Is user present:", isUserPresent)

	var rule model.Rule
	var ruleType string
	if evt.Type == model.EventRecognizeSuccess {
		ruleType = model.RuleTypeSittingMonitoring
	}

	if err := dao.Collection("rule").Find(bson.M{
		"deskId": evt.DeskId,
		"userId": evt.UserId,
		"type":   ruleType,
	}).One(&rule); err != nil {
		log.Println("[EVENT_MONITOR]", "No RULE found for sitting monitoring.")
		return
	}

	log.Println("[EVENT_MONITOR]", "Found rule", rule.Type, "with interval minutes", rule.IntervalMinutes)

	sit := model.SitTracking{}
	err := dao.Collection("sit_tracking").Find(bson.M{"deviceId": evt.DeviceId, "userId": evt.UserId}).One(&sit)
	if err == mgo.ErrNotFound {
		sit.UserId = evt.UserId
		sit.DeviceId = evt.DeviceId
		sit.Id = bson.NewObjectId()
		if isUserPresent {
			sit.Status = model.SitStatusPresent
		} else {
			sit.Status = model.SitStatusMissing
		}
		sit.TrackingTime = time.Now()
		if err := dao.Collection("sit_tracking").Insert(sit); err != nil {
			log.Println("[EVENT_MONITOR]", "Fail to save sit tracking status", err.Error())
		}
		return
	}

	if isUserPresent {
		if sit.Status == model.SitStatusPresent {
			sittingDuration := time.Since(sit.TrackingTime)
			log.Println("[EVENT_MONITOR]", "User may sitting contiguously for", durafmt.Parse(sittingDuration))
			if sittingDuration.Minutes() >= float64(rule.IntervalMinutes) {
				log.Println("[EVENT_MONITOR]", "Sending notification to user", evt.UserId.Hex())
				var d model.Device
				if err := dao.Collection("device").Find(bson.M{"deviceId": evt.DeviceId}).One(&d); err == nil {
					sendNotification(d, sittingDuration, rule)
				}
			} else {
				log.Println("[EVENT_MONITOR]", "User sitting duration is below threshold. Do nothing.")
			}
		} else {
			log.Println("[EVENT_MONITOR]", "Update sit tracking to PRESENT")
			sit.Status = model.SitStatusPresent
			sit.TrackingTime = time.Now()
			if err := dao.Collection("sit_tracking").UpdateId(sit.Id, &sit); err != nil {
				log.Println("[EVENT_MONITOR]", "Fail to update sit tracking status to PRESENT", err.Error())
			}
		}
	} else {
		if sit.Status == model.SitStatusPresent {
			since := time.Now().Add(-time.Duration(missingThresholdMinutes) * time.Minute)
			if c, err := dao.Collection("event").Find(bson.M{
				"deviceId": evt.DeviceId,
				"owner":    evt.UserId,
				"timestamp": bson.M{
					"$gt": since,
				},
				"result": model.ResultPresent,
			}).Count(); err != nil {
				log.Println("[EVENT_MONITOR]", "Fail to query PRESENT event status", err.Error())
			} else {
				log.Println("[EVENT_MONITOR]", "Found", c, "PRESENT events since", since)
				if c == 0 {
					// no present event found in last threshold minute. Update status to missing
					log.Println("[EVENT_MONITOR", "User is missing for a while. Update sit tracking to MISSING")
					sit.Status = model.SitStatusMissing
					sit.TrackingTime = time.Now()
					if err := dao.Collection("sit_tracking").UpdateId(sit.Id, &sit); err != nil {
						log.Println("[EVENT_MONITOR]", "Fail to update sit tracking status to MISSING", err.Error())
					}
				} else {
					log.Println("[EVENT_MONITOR]", "User may still around. Do nothing for now.")
				}
			}
		} else {
			log.Println("[EVENT_MONITOR]", "User still missing. Do nothing.")
		}
	}
}

func sendNotification(device model.Device, sitDuration time.Duration, rule model.Rule) {
	nf := model.Notification{
		DeskId:      device.DeskId,
		DeviceId:    device.DeviceId,
		UserId:      device.Owner,
		Timestamp:   time.Now(),
		Type:        model.NotificationTypeSlack,
		SitDuration: sitDuration,
		Rule:        rule,
	}

	if payload, err := json.Marshal(nf); err != nil {
		log.Println("[MONITOR]", "Fail to marshal notification")
	} else {
		opts := service.NewClientOpts(config.Get().MQTTBroker)
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Println("[MONITOR]", "Fail to connect to MQTT", token.Error())
			return
		}
		if token := client.Publish(model.TopicNotificationBroadcast, 0, false, payload); token.Wait() && token.Error() != nil {
			log.Println("[MONITOR]", "Fail publish notification via MQTT", token.Error())
		} else {
			log.Println("[MONITOR]", "Notification published successfully")
		}

		defer client.Disconnect(200)
	}
}
