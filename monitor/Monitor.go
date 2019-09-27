package monitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo/bson"
	"github.com/google/uuid"
	"github.com/ndphu/swd-commons/model"
	"image"
	"log"
	"monitor-service/config"
	"monitor-service/db"
	"sync"
	"time"
)

var trackingMap = make(map[string]*DetectionTracking)

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

	clientId := uuid.New().String()
	opts := GetDefaultOps(clientId)
	deviceId := r.DeviceId

	var pics [][]byte

	done := make(chan bool)

	opts.OnConnect = func(c mqtt.Client) {
		topic := getFrameOutTopic(deviceId)

		secTicker := time.NewTicker(time.Duration(frameDelay) * time.Millisecond)
		c.Subscribe(topic, 0, func(client mqtt.Client, message mqtt.Message) {
			select {
			case <-secTicker.C:
				pics = append(pics, message.Payload())
				break
			default:
				break
			}
			if len(pics) == totalPics {
				client.Disconnect(0)
				done <- true
			}
		}).Wait()
	}

	captureClient := mqtt.NewClient(opts)
	if token := captureClient.Connect(); token.Wait() && token.Error() != nil {
		log.Println("[MONITOR] Fail to connect to MQTT", token.Error())
		return
	}

	defer captureClient.Disconnect(500)

	timeout := time.NewTimer((time.Duration(frameDelay*totalPics) * time.Millisecond) + 5*time.Second)

	select {
	case <-timeout.C:
		log.Println("[MONITOR] Capture timeout.")
		if err := saveCaptureFailEvent(r); err != nil {
			log.Println("[MONITOR] Fail to save event by error", err.Error())
		} else {
			log.Println("[MONITOR] Save event successfully")
		}
		return
	case <-done:
		log.Println("[MONITOR] Capture completed. Frame count:", len(pics))
		break
	}

	if response, err := recognizeBulk(r.ProjectId, pics); err != nil {
		log.Println("[MONITOR] Fail to recognize bulk by error:", err.Error())
		saveRecognizeFailEvent(r, err)
	} else {
		log.Println("[MONITOR] Device:", r.DeviceId, "Found labels:", response.Labels)

		saveRecognizeSuccessEvent(r, response.Labels)

		if trackingMap[r.DeviceId] == nil {
			trackingMap[r.DeviceId] = &DetectionTracking{
			}
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

				log.Println("[MONITOR]", "Rule:", r.Id, "Should send notification:", shouldSendNotification)
				if shouldSendNotification {
					sendNotification(r)
				}
			}
		}

		//if matchPeople {
		//	trackingMap[r.DeviceId].FirstMissing = time.Unix(0, 0)
		//	if trackingMap[r.DeviceId].FirstSeen.IsZero() {
		//		log.Println("[MONITOR]", "First see user camera at", time.Now())
		//		trackingMap[r.DeviceId].FirstSeen = time.Now()
		//	} else {
		//		since := time.Since(trackingMap[r.DeviceId].FirstSeen)
		//		if since.Minutes() > float64(r.Interval) {
		//			sendNotification(r)
		//		} else {
		//			log.Println("[MONITOR]", "User is on camera for", since.Seconds(), "seconds")
		//
		//		}
		//	}
		//} else {
		//	if trackingMap[r.DeviceId].FirstMissing.IsZero() {
		//		log.Println("[MONITOR]", "First seen user MISSING at", time.Now())
		//		trackingMap[r.DeviceId].FirstMissing = time.Now()
		//	} else {
		//		since := time.Since(trackingMap[r.DeviceId].FirstMissing)
		//		if since.Seconds() > 45.0 {
		//			log.Println("[MONITOR]", "User may leaving his desk. Reset tracking.")
		//			trackingMap[r.DeviceId] = nil
		//		} else {
		//			log.Println("[MONITOR]", "User still MISSING for ", since.Seconds(), "seconds")
		//		}
		//	}
		//}
	}

	log.Println("[MONITOR] Processing done for rule", r.Id.Hex())
}

func sendNotification(rule model.Rule) {
	// TOPIC: /3ml/project/{id}/notification
	nf := model.Notification{
		Id:        bson.NewObjectId(),
		ProjectId: rule.ProjectId,
		DeviceId:  rule.DeviceId,
		Type:      rule.Action.Type,
		Timestamp: time.Now(),
		RuleId:    rule.Id,
	}

	if err := dao.Collection("notification").Insert(nf); err != nil {
		log.Println("[MONITOR]", "Fail to persist notification by error:", err.Error())
	} else {
		log.Println("[MONITOR]", "Notification persisted successfully")
	}

	if payload, err := json.Marshal(nf); err != nil {
		log.Println("[MONITOR]", "Fail to marshal notification")
	} else {
		topic := fmt.Sprintf("/3ml/project/%s/notification", rule.ProjectId)
		log.Println("[MONITOR]", "Publishing notification to topic", topic)
		clientId := uuid.New().String()
		opts := GetDefaultOps(clientId)
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Println("[MONITOR]", "Fail to connect to MQTT", token.Error())
			return
		}
		if token := client.Publish(topic, 0, false, payload); token.Wait() && token.Error() != nil {
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

func GetDefaultOps(clientId string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().AddBroker(config.Get().MQTTBroker).SetClientID(clientId)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetConnectTimeout(30 * time.Second)
	return opts
}

func getFrameOutTopic(deviceId string) string {
	return "/3ml/device/" + deviceId + "/framed/out"
}

func recognizeBulk(projectId string, images [][]byte) (*BulkRecognizeResponse, error) {
	reqId := uuid.New().String()
	rpcReqPayload, _ := json.Marshal(BulkRecognizeRequest{
		RequestId: reqId,
		Images:    images,
	})

	rpcResponse := make(chan BulkRecognizeResponse)

	rpcRequestTopic := "/3ml/rpc/recognizeFacesBulk/request"
	rpcResponseTopic := "/3ml/rpc/" + "/response/" + reqId

	clientId := uuid.New().String()

	opts := GetDefaultOps(clientId)

	c := mqtt.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error().Error())
	}

	defer c.Disconnect(500)

	c.Subscribe(rpcResponseTopic, 0, func(c mqtt.Client, m mqtt.Message) {
		resp := BulkRecognizeResponse{}
		if err := json.Unmarshal(m.Payload(), &resp); err != nil {
			log.Println("[RPC]", reqId, "fail to unmarshal response")
			rpcResponse <- BulkRecognizeResponse{
				Error: err,
			}
		} else {
			rpcResponse <- resp
		}
	}).Wait()
	c.Publish(rpcRequestTopic, 9, false, rpcReqPayload).Wait()
	rpcTimeout := time.NewTimer(15 * time.Second)
	select {
	case resp := <-rpcResponse:
		return &resp, nil
	case <-rpcTimeout.C:
		log.Println("[RPC]", reqId, "timeout occurred.")
		return nil, errors.New("timeout")
	}
}

type BulkRecognizeRequest struct {
	Images              [][]byte `json:"payload"`
	IncludeFacesDetails bool     `json:"includeFacesDetails"`
	RequestId           string   `json:"requestId"`
	ResponseTo          string   `json:"responseTo"`
}

type BulkRecognizeResponse struct {
	Labels          []string      `json:"labels"`
	FaceDetailsList []FaceDetails `json:"faceDetailsList"`
	Error           error         `json:"error"`
}

type FaceDetails struct {
	Rect       image.Rectangle `json:"rect"`
	Descriptor []float32       `json:"descriptor"`
}

type DetectionTracking struct {
	FirstSeen    time.Time `json:"firstSeen"`
	FirstMissing time.Time `json:"firstMissing"`
}
