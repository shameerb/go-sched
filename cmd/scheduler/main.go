package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/common"
	"github.com/shameerb/go-sched/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Scheduler service http Port")
)

func main() {
	flag.Parse()
	log.Println("starting scheduler service...")
	dbConnString := common.GetDBConnString()
	s := scheduler.NewScheduler(*schedulerPort, dbConnString)
	if err := s.Start(); err != nil {
		log.Fatalf("error while starting the scheduler service: %s", err)
	}
}
