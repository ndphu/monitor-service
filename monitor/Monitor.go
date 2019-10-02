package monitor

import (
	"encoding/json"
	"errors"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo/bson"
	"github.com/google/uuid"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"log"
	"monitor-service/config"
	"monitor-service/db"
	"sync"
	"time"
)

func Start() {
	doMonitorDevices()
	ticker := time.NewTicker(30 * time.Second)
	for {
		log.Println("[MONITOR] Waiting for ticker...")
		startAt := <-ticker.C
		log.Println("[MONITOR] Job start at", startAt)
		doMonitorDevices()
		log.Println("[MONITOR] Job done at", time.Now())
	}
}

func doMonitorDevices() {
	devices := make([]model.Device, 0)
	if err := dao.Collection("device").Find(nil).All(&devices); err != nil {
		log.Println("[MONITOR]", "Fail to load devices by error:", err.Error())
		return
	}
	wg := sync.WaitGroup{}

	for _, device := range devices {
		wg.Add(1)
		go func(d model.Device) {
			defer wg.Done()
			doMonitorDevice(d)
		}(device)
	}

	wg.Wait()

}
func doMonitorDevice(device model.Device) {
	log.Println("[MONITOR]", "Monitoring device", device.DeviceId, device.Name)

	frameDelay := 500 //ms
	totalPics := 10

	if frames, err := service.CaptureFrameContinuously(service.NewClientOpts(config.Get().MQTTBroker), device.DeviceId, frameDelay, totalPics); err != nil {
		saveCaptureFailEvent(device)
		return
	} else {
		var faces []model.Face
		if err := dao.Collection("face").Find(bson.M{
			"deskId": device.DeskId,
			"owner":  device.Owner,
		}).All(&faces); err != nil {
			saveRecognizeFailEvent(device, err)
			return
		}
		if len(faces) == 0 {
			saveRecognizeFailEvent(device, errors.New("NO_FACE_DATA_TO_RECOGNIZE"))
			return
		}
		if response, err := service.CallBulkRecognizeWithProvidedFacesData(service.NewClientOpts(config.Get().MQTTBroker), device.DeskId, frames, faces); err != nil {
			saveRecognizeFailEvent(device, err)
			return
		} else {
			log.Println("[MONITOR] Device:", device.DeviceId, "Found labels:", response.Labels)

			evt, err := saveRecognizeSuccessEvent(device, response.Labels)
			if err != nil {
				log.Println("[MONITOR] Device:", device.DeviceId, "Fail to save event by error:", err.Error())
				return
			}

			isUserPresent := evt.Result == model.ResultPresent
			log.Println("[MONITOR]", "Is user present:", isUserPresent)

			if isUserPresent && shouldSendNotification(device) {
				sendNotification(device)
			}
		}

	}
	log.Println("[MONITOR] Processing done for device", device.DeviceId, device.Name)
}
func shouldSendNotification(device model.Device) bool {
	if events, err := getRecentRecognizeResult(device); err != nil {
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

func saveCaptureFailEvent(d model.Device) error {
	//event := newEvent(rule)
	event := model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  d.DeviceId,
		Timestamp: time.Now(),
		Type:      model.EventCaptureFail,
		UserId:    d.Owner,
	}
	return dao.Collection("event").Insert(&event)
}

func saveRecognizeSuccessEvent(device model.Device, labels []string) (*model.Event, error) {
	event := model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  device.DeviceId,
		Timestamp: time.Now(),
		Type:      model.EventRecognizeSuccess,
		UserId:    device.Owner,
		Labels:    labels,
		Result:    model.ResultMissing,
	}
	for _, l := range labels {
		if l == device.Owner.Hex() {
			event.Result = model.ResultPresent
		}
	}
	err := dao.Collection("event").Insert(event)
	if err == nil {
		go broadcastEvent(&event)
	}
	return &event, err
}

func broadcastEvent(evt *model.Event) {
	log.Println("[EVENT]", "Broadcasting event...")
	ops := service.NewClientOpts(config.Get().MQTTBroker)
	ops.ClientID = uuid.New().String()

	ops.OnConnect = func(client mqtt.Client) {
		log.Println("[EVENT]", "MQTT connected successfully")

	}
	client := mqtt.NewClient(ops)
	if c := client.Connect(); c.Wait() && c.Error() != nil {
		log.Println("[EVENT]", "Fail to connect to MQTT by error")
	}
	defer client.Disconnect(100)

	if payload, err := json.Marshal(evt); err != nil {
		log.Println("[EVENT]", "Fail to marshal event")
	} else {
		if p := client.Publish("/3ml/event/broadcast", 0, false, payload); p.Wait() && p.Error() != nil {
			log.Println("[EVENT]", "Fail to publish event to MQTT", err.Error())
		} else {
			log.Println("[EVENT]", "Event broadcast successfully")
		}
	}
}

func saveRecognizeFailEvent(device model.Device, err error) error {
	event := model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  device.DeviceId,
		Timestamp: time.Now(),
		Type:      model.EventRecognizeFail,
		UserId:    device.Owner,
		Error:     err.Error(),
	}
	return dao.Collection("event").Insert(event)
}

func newEvent(rule model.Rule) *model.Event {
	return &model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  rule.DeviceId,
		Timestamp: time.Now(),
	}
}

func getRecentRecognizeResult(device model.Device) ([]model.Event, error) {
	events := make([]model.Event, 0)
	if err := dao.Collection("event").Find(bson.M{"type": "RECOGNIZE_SUCCESS", "deviceId": device.DeviceId}).Sort("-timestamp").Limit(10).All(&events);
		err != nil {
		return nil, err
	} else {
		return events, err
	}
}
