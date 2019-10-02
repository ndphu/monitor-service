package monitor

import (
	"log"
	"time"
)

var shutdownChan = make(chan bool)

func Start() {
	go monitorEvents()
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

func Stop() {
	shutdownChan <- true
}
