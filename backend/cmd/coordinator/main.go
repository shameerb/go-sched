package main

import (
	"flag"
	"log"
	"scheduler/pkg/common"
	"scheduler/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "grpc server port (tcp)")
)

func main() {
	flag.Parse()
	dbConnString := common.GetConnString()
	coordinator := coordinator.NewCoordinator(*coordinatorPort, dbConnString)
	if err := coordinator.Start(); err != nil {
		log.Fatalf("error while starting coordinator service: %s\n", err)
	}
}
