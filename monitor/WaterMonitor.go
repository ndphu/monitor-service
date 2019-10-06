package monitor

import (
	"errors"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/globalsign/mgo/bson"
	"github.com/ndphu/swd-commons/model"
	"github.com/ndphu/swd-commons/service"
	"log"
	"monitor-service/config"
	"monitor-service/db"
	"regexp"
	"strconv"
	"time"
)

var (
	TopicScaleRegex   = regexp.MustCompile(`^/3ml/device/(.*?)/scale$`)
	TopicLiftUpRegex  = regexp.MustCompile(`^/3ml/device/(.*?)/liftUp`)
	TopicPutDownRegex = regexp.MustCompile(`^/3ml/device/(.*?)/putDown$`)
)

func monitorWaterMonitorDevices() {

	opts := service.NewClientOpts(config.Get().MQTTBroker)
	opts.OnConnect = func(client mqtt.Client) {
		log.Println("[MQTT]", "Connected to broker")
		client.Subscribe("/3ml/device/+/scale", 0, func(client mqtt.Client, message mqtt.Message) {
			if scaleValue, err := strconv.Atoi(string(message.Payload())); err != nil {
				log.Println("[MQTT]", "Fail to read water event value", err.Error())
			} else {
				handleScaleEvent(message.Topic(), scaleValue)
			}

		})
		client.Subscribe("/3ml/device/+/liftUp", 0, func(client mqtt.Client, message mqtt.Message) {
			handleLiftUpEvent(message.Topic())
		})
		client.Subscribe("/3ml/device/+/putDown", 0, func(client mqtt.Client, message mqtt.Message) {
			handlePutDownEvent(message.Topic())
		})
		log.Println("[MQTT]", "Subscribed to generic device topics")
	}
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalln("[MQTT]", "Fail to connect to message broker", tok.Error())
	}

	<-shutdownChan
	log.Println("[EVENT_MONITOR]", "Exiting event monitoring...")
	defer c.Disconnect(100)
}

func handleLiftUpEvent(topic string) {
	device, err := findDeviceFromTopic(topic, TopicLiftUpRegex)
	if err != nil {
		log.Println("Device not found", topic, err.Error())
		return
	}
	event, err := persistEvent(device, model.EventScaleLiftUp, 0)
	if err != nil {
		log.Println("Fail to persist scale event")
		return
	}

	broadcastEvent(event)
}

func handlePutDownEvent(topic string) {
	device, err := findDeviceFromTopic(topic, TopicPutDownRegex)
	if err != nil {
		log.Println("Device not found", topic, err.Error())
		return
	}
	event, err := persistEvent(device, model.EventScalePutDown, 0)
	if err != nil {
		log.Println("Fail to persist scale event")
		return
	}

	broadcastEvent(event)
}

func handleScaleEvent(topic string, scaleValue int) {
	log.Println("Read scale value", scaleValue, "from topic", topic)
	device, err := findDeviceFromTopic(topic, TopicScaleRegex)
	if err != nil {
		log.Println("Device not found", topic, err.Error())
		return
	}

	event, err := persistEvent(device, model.EventScaleUpdate, scaleValue)
	if err != nil {
		log.Println("Fail to persist scale event")
		return
	}

	broadcastEvent(event)
}

func findDeviceFromTopic(topic string, re *regexp.Regexp) (*model.Device, error) {
	log.Println("findDeviceFromTopic", topic)
	if match := re.FindStringSubmatch(topic); len(match) > 1 {
		deviceSerial := match[1]
		log.Println("Found device serial:", deviceSerial)
		d := model.Device{}
		if err := dao.Collection("device").Find(bson.M{"deviceId": deviceSerial}).One(&d); err != nil {
			log.Println("Fail to lookup device with serial", deviceSerial, err.Error())
			return nil, err
		}
		log.Println("Found device:", d.Type)
		return &d, nil
	}
	log.Println("Topic not match with regex")
	return nil, errors.New("TOPIC_NOT_MATCH")
}

func persistEvent(device *model.Device, eType string, scale int) (*model.Event, error) {
	event := model.Event{
		Id:        bson.NewObjectId(),
		DeviceId:  device.DeviceId,
		Type:      eType,
		DeskId:    device.DeskId,
		UserId:    device.Owner,
		Timestamp: time.Now(),
	}
	if model.EventScaleUpdate == eType {
		event.Result = strconv.Itoa(scale)
	}
	err := dao.Collection("event").Insert(event)
	return &event, err
}
