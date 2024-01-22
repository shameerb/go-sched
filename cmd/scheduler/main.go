package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/common"
	"github.com/shameerb/go-sched/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which scheduler serves the request")
)

func main() {
	flag.Parse()
	log.Println("schedulerPort", schedulerPort)
	dbConnectionString := common.GetDBConnectionString()
	sh := scheduler.NewServer(*schedulerPort, dbConnectionString)
	if err := sh.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
