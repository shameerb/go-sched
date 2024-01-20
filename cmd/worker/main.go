package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/worker"
)

var (
	serverPort      = flag.String("worker_port", "", "Port on which the worker serves the grpc request")
	coordinatorPort = flag.String("coordinator", ":8080", "Network address of the coordinator")
)

func main() {
	flag.Parse()
	worker := worker.NewServer(*serverPort, *coordinatorPort)
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start the worker: %+v", err)
	}
}
