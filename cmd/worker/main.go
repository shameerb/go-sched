package main

import (
	"flag"
	"log"

	"github.com/shameerb/go-sched/pkg/worker"
)

var (
	workerPort  = flag.String("worker_port", "", "grpc server port (tcp)")
	coordinator = flag.String("coordinator", ":8080", "Network address for coordinator")
)

func main() {
	flag.Parse()
	log.Println("starting worker service...")
	s := worker.NewWorker(*workerPort, *coordinator)
	if err := s.Start(); err != nil {
		log.Fatalf("error while starting the worker service: %s", err)
	}
}
