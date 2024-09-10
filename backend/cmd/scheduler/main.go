package main

import (
	"flag"
	"log"
	"scheduler/pkg/common"
	"scheduler/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Scheduler service http port")
)

func main() {
	flag.Parse()
	log.Println("starting scheduler service")
	dbConnString := common.GetConnString()
	s := scheduler.NewScheduler(*schedulerPort, dbConnString)
	if err := s.Start(); err != nil {
		log.Fatalf("error while starting scheduler service: %s", err)
	}
}
