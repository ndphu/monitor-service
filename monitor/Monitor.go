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
	"sync"
	"time"
)

func Start() {
	doMonitor()
	ticker := time.NewTicker(30 * time.Second)
	for {
		log.Println("[MONITOR] Waiting for ticker...")
		startAt := <-ticker.C
		log.Println("[MONITOR] Job start at", startAt)
		doMonitor()
		log.Println("[MONITOR] Job done at", time.Now())
	}
}

func doMonitor() {
	rules := make([]model.Rule, 0)
	if err := dao.Collection("rule").Find(nil).All(&rules); err != nil {
		log.Println("[MONITOR] Fail to load rules by error:", err.Error())
		return
	}
	log.Println("[MONITOR] Working on", len(rules), "rules")

	wg := sync.WaitGroup{}

	for _, rule := range rules {
		wg.Add(1)
		go func(r model.Rule) {
			defer wg.Done()
			processRule(r)
		}(rule)

	}

	wg.Wait()
}

func processRule(r model.Rule) {
	log.Println("[MONITOR] Processing rule", r.Id.Hex())

	frameDelay := 1000 //ms
	totalPics := 5

	if frames, err := service.CaptureFrameContinuously(service.NewClientOpts(config.Get().MQTTBroker), r.DeviceId, frameDelay, totalPics); err != nil {
		saveCaptureFailEvent(r)
		return
	} else {
		if response, err := service.CallBulkRecognize(service.NewClientOpts(config.Get().MQTTBroker), r.ProjectId, frames); err != nil {
			saveRecognizeFailEvent(r, err)
			return
		} else {
			log.Println("[MONITOR] Device:", r.DeviceId, "Found labels:", response.Labels)

			if err := saveRecognizeSuccessEvent(r, response.Labels); err != nil {
				log.Println("[MONITOR] Device:", r.DeviceId, "Fail to save event by error:", err.Error())
			}

			matchPeople := false
			for _, label := range response.Labels {
				if label == r.ProjectId {
					matchPeople = true
					break
				}
			}

			log.Println("[MONITOR]", "Match People:", matchPeople)

			if matchPeople {
				if events, err := getRecentRecognizeResult(r); err != nil {
					log.Println("[MONITOR]", "Fail to get recent events by error", err.Error())
				} else {
					log.Println("[MONITOR]", "Found", len(events), "events")
					shouldSendNotification := true

					consecutiveMissing := 0

					for _, event := range events {
						hasLabel := false
						for _, l := range event.Labels {
							if l == r.ProjectId {
								hasLabel = true
								break
							}
						}
						if hasLabel {
							log.Println("reset consecutive count")
							consecutiveMissing = 0
						} else {
							consecutiveMissing ++
							log.Println("current consecutive", consecutiveMissing)
							if consecutiveMissing == 3 {
								shouldSendNotification = false
								break
							}
						}
					}
					// TODO: should check last present after consecutive too

					log.Println("[MONITOR]", "Rule:", r.Id.Hex(), "Should send notification:", shouldSendNotification)
					if shouldSendNotification {
						sendNotification(r)
					}
				}
			}
		}

	}
	log.Println("[MONITOR] Processing done for rule", r.Id.Hex())
}

func sendNotification(rule model.Rule) {
	nf := model.Notification{
		ProjectId: rule.ProjectId,
		DeviceId:  rule.DeviceId,
		Timestamp: time.Now(),
		RuleId:    rule.Id,
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

func saveCaptureFailEvent(rule model.Rule) error {
	event := newEvent(rule)
	event.Type = model.EventCaptureFail
	return dao.Collection("event").Insert(event)
}

func saveRecognizeSuccessEvent(rule model.Rule, labels []string) error {
	event := newEvent(rule)
	event.Labels = labels
	event.Type = model.EventRecognizeSuccess
	event.Result = model.ResultMissing
	for _, l := range labels {
		if l == rule.ProjectId {
			event.Result = model.ResultPresent
		}
	}
	return dao.Collection("event").Insert(event)
}

func saveRecognizeFailEvent(rule model.Rule, err error) error {
	event := newEvent(rule)
	event.Error = err.Error()
	event.Type = model.EventRecognizeFail
	return dao.Collection("event").Insert(event)
}

func newEvent(rule model.Rule) *model.Event {
	return &model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  rule.DeviceId,
		Timestamp: time.Now(),
	}
}

func getRecentRecognizeResult(rule model.Rule) ([]model.Event, error) {
	events := make([]model.Event, 0)
	if err := dao.Collection("event").Find(bson.M{"type": "RECOGNIZE_SUCCESS"}).Sort("-timestamp").Limit(rule.Interval * 2).All(&events);
		err != nil {
		return nil, err
	} else {
		return events, err
	}
}
