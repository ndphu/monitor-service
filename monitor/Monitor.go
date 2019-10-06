package monitor

var shutdownChan = make(chan bool)

func Start() {
	go monitorEvents()
	go monitorCameraDevices()
	go monitorWaterMonitorDevices()
	<-shutdownChan
}

func Stop() {
	shutdownChan <- true
}
