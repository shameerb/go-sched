package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/common"
	"github.com/shameerb/go-sched/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "grpc server port (tcp)")
)

func main() {
	flag.Parse()
	log.Println("starting coordinator service...")
	dbConnString := common.GetDBConnString()
	s := coordinator.NewCoordinator(*coordinatorPort, dbConnString)
	if err := s.Start(); err != nil {
		log.Fatalf("error while starting the coordinator service: %s", err)
	}
}
