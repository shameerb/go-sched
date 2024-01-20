package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/common"
	"github.com/shameerb/go-sched/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "Port which serves the request")
)

func main() {
	flag.Parse()
	log.Println("cordinatorPort", coordinatorPort)
	dbConnectionString := common.GetDBConnectionString()
	cs := coordinator.NewServer(*coordinatorPort, dbConnectionString)
	if err := cs.Start(); err != nil {
		log.Fatalf("Failed to start the coordinator server: %+v", err)
	}

}
