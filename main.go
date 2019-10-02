package main

import (
	"log"
	"monitor-service/monitor"
	"os"
	"os/signal"
)

func main()  {
	go monitor.Start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	log.Println("Interrupt signal received. Exiting...")
	monitor.Stop()

	os.Exit(0)
}
