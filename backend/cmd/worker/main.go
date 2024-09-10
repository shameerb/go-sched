package main

import (
	"flag"
	"log"

	"scheduler/pkg/worker"
)

var (
	httpPort    = flag.String("http_port", ":8082", "http server port")
	grpcPort    = flag.String("grpc_port", "", "grpc server port (tcp)")
	coordinator = flag.String("coordinator", ":8080", "Network address for coordinator")
)

func main() {
	flag.Parse()
	log.Println("starting worker service...")
	s := worker.NewWorker(*httpPort, *grpcPort, *coordinator)
	if err := s.Start(); err != nil {
		log.Fatalf("error while starting the worker service: %s", err)
	}
}
