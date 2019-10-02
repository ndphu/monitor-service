package monitor

import (
	"encoding/json"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo/bson"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"log"
	"monitor-service/config"
	"monitor-service/db"
	"time"
)

func monitorEvents()  {
	opts := service.NewClientOpts(config.Get().MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe("/3ml/event/broadcast", 0, func(client mqtt.Client, message mqtt.Message) {
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

	<- shutdownChan
	log.Println("[EVENT_MONITOR]", "Exiting event monitoring...")
	defer c.Disconnect(100)
}

func handleEvent(evt *model.Event)  {
	log.Println("[EVENT_MONITOR]", "Handling event", evt.Type, evt.Result, "of device", evt.DeviceId)

	isUserPresent := evt.Result == model.ResultPresent
	log.Println("[MONITOR]", "Is user present:", isUserPresent)

	if isUserPresent && shouldSendNotification(evt.DeviceId) {
		//sendNotification(evt)
		var d model.Device
		if err := dao.Collection("device").Find(bson.M{"deviceId": evt.DeviceId}).One(&d); err == nil {
			sendNotification(d)
		}
	}
}

func shouldSendNotification(deviceId string) bool {
	if events, err := getRecentRecognizeResult(deviceId); err != nil {
		log.Println("[MONITOR]", "Fail to get recent events by error", err.Error())
		return false
	} else {
		log.Println("[MONITOR]", "Found", len(events), "events")
		shouldSendNotification := true

		consecutiveMissing := 0

		for _, event := range events {
			if event.Result == model.ResultPresent {
				log.Println("[MONITOR]", "Reset consecutive count")
				consecutiveMissing = 0
			} else {
				consecutiveMissing ++
				log.Println("[MONITOR]", "Current consecutive", consecutiveMissing)
				if consecutiveMissing == 3 {
					shouldSendNotification = false
					break
				}
			}
		}
		// TODO: should check last present after consecutive too

		log.Println("[MONITOR]", "Should send notification:", shouldSendNotification)
		return shouldSendNotification
	}
}

func sendNotification(device model.Device) {
	sc := model.SlackConfig{}
	err := dao.Collection("slack_config").Find(bson.M{"userId": device.Owner}).One(&sc)
	if err != nil {
		log.Println("[MONITOR]", "Fail to send notification by error", err.Error())
		return
	}

	nf := model.Notification{
		DeskId:      device.DeskId,
		DeviceId:    device.DeviceId,
		UserId:      device.Owner,
		Timestamp:   time.Now(),
		Type:        "SLACK",
		SlackUserId: sc.SlackUserId,
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
		if token := client.Publish("/3ml/notifications/broadcast", 0, false, payload); token.Wait() && token.Error() != nil {
			log.Println("[MONITOR]", "Fail publish notification via MQTT", token.Error())
		} else {
			log.Println("[MONITOR]", "Notification published successfully")
		}

		defer client.Disconnect(200)
	}
}

func getRecentRecognizeResult(deviceId string) ([]model.Event, error) {
	events := make([]model.Event, 0)
	if err := dao.Collection("event").Find(bson.M{"type": "RECOGNIZE_SUCCESS", "deviceId": deviceId}).Sort("-timestamp").Limit(10).All(&events);
		err != nil {
		return nil, err
	} else {
		return events, err
	}
}