package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/shameerb/go-sched/pkg/common"
	pb "github.com/shameerb/go-sched/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	coordinatorAddress       string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorConnection    *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	ReceivedTasks            map[string]*pb.TaskRequest
	ReceivedTasksMutex       sync.Mutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

func NewServer(port, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  common.DefaultHeartBeat,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		ReceivedTasks:      make(map[string]*pb.TaskRequest),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (w *WorkerServer) Start() error {
	w.startWorkerPool(workerPoolSize)
	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	return w.awaitShutdown()
}

func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

func (w *WorkerServer) worker() {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			w.processTask(task)
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETE)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId:      task.GetTaskId(),
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
}

func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing task: %+v", task)
	time.Sleep(taskProcessTime)
	log.Printf("Completed task: %+v", task)
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	var err error
	w.coordinatorConnection, err = grpc.Dial(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Println("Could not connect to the coordinator")
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorConnection)
	log.Println("Connected to coordinator service !")
	return nil
}

func (w *WorkerServer) startGRPCServer() error {
	var err error
	if w.serverPort == "" {
		w.listener, err = net.Listen("tcp", ":0")
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port)
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.serverPort)
	}
	log.Printf("Starting worker server on %s\n", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)
	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}
	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Error while closing the listener: %v", err)
		}
	}
	if err := w.coordinatorConnection.Close(); err != nil {
		log.Printf("Error while closing client connection with coordinator: %v", err)
	}
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// ideally you can put a retry with exponential backoff to send a heartbeat. Failing once and killing is not a viable solution.
			if err := w.sendHeartBeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				return
			}
		case <-w.ctx.Done():
			return
		}
	}

}

func (w *WorkerServer) sendHeartBeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
	}

	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %+v", req)
	w.ReceivedTasksMutex.Lock()
	defer w.ReceivedTasksMutex.Unlock()
	w.ReceivedTasks[req.GetTaskId()] = req
	w.taskQueue <- req
	return &pb.TaskResponse{
		TaskId:  req.TaskId,
		Message: "Task submitted",
		Success: true,
	}, nil
}

func (w *WorkerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return w.Stop()
}

func (w *WorkerServer) Stop() error {
	// close(w.taskQueue)
	// send signal to all goroutines to stop
	w.cancel()
	// wait for all goroutines to finish
	w.wg.Wait()
	w.closeGRPCConnection()
	log.Println("Worker server stopped")
	return nil
}
