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

	"github.com/shameerb/go-sched/pkg/common"
	pb "github.com/shameerb/go-sched/pkg/grpcapi"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
)

type Worker struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	port                     string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorAddr          string
	coordinatorServiceConn   *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	// archive of tasks, Public to be accesible from outside
	ReceivedTasks      map[string]*pb.TaskRequest
	ReceivedTasksMutex sync.Mutex
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
}

func NewWorker(port, coordinatorPort string) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		id:                uuid.New().ID(),
		port:              port,
		coordinatorAddr:   coordinatorPort,
		heartbeatInterval: common.DefaultHeartBeat,
		taskQueue:         make(chan *pb.TaskRequest, 100),
		ReceivedTasks:     make(map[string]*pb.TaskRequest),
		ctx:               ctx,
		cancel:            cancel,
	}
}

/*
- worker pool to execute the tasks
- start a grpc server for submit task
- client grpc connection to coordinator
  - health check to coordinator
*/
func (w *Worker) Start() error {
	w.startWorkerPool()
	// create a grpc server to receive tasks from coordinator
	if err := w.startGrpcServer(); err != nil {
		log.Fatalf("error in starting grpc server: %s", err)
	}
	// connect with coordinator
	// todo: ideally retry a set nos of times since connection to coordinator might be lost when coordinator restarts or because of network interruption.
	if err := w.connectToCoordinator(); err != nil {
		log.Fatalf("failed to establish connection with coordinator service: %s", err)
	}
	// send heartbeat to coordinator (keepalive)
	go w.healthCheck()
	return w.awaitShutdown()
}

func (w *Worker) startWorkerPool() {
	for i := 0; i < workerPoolSize; i++ {
		w.wg.Add(1)
		go w.startWorkerGoRoutines()
	}
}

func (w *Worker) startWorkerGoRoutines() {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			w.processTask(task)
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) updateTaskStatus(t *pb.TaskRequest, s pb.TaskStatus) {
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId:      t.GetTaskId(),
		Status:      s,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
}

func (w *Worker) processTask(t *pb.TaskRequest) {
	log.Printf("starting task: %+v", t)
	time.Sleep(taskProcessTime)
	log.Printf("finished task: %+v", t)
}

func (w *Worker) connectToCoordinator() error {
	log.Println("connecting to coordinator service")
	var err error
	// todo: fail after a timeout, else it hangs on a Dial in case the coordinator is down !!!
	w.coordinatorServiceConn, err = grpc.Dial(w.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Printf("could not connect to coordinator: %s", err)
		return err
	}
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorServiceConn)
	log.Println("connected to coordinator service")
	return nil
}

func (w *Worker) healthCheck() {
	w.wg.Add(1)
	defer w.wg.Done()
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendHeartBeat(); err != nil {
				// ideally keep sending the heartbeat and keep a count on the number of times. If the count passed a threshold then panic.
				// This is becase in case of a coordinator restart, the worker will hangup.
				log.Printf("failed to send heartbeat: %v", err)
				log.Fatalf("failed to connect to coordinator: %s", err)
			}
		}
	}
}

func (w *Worker) sendHeartBeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.port
	}
	if _, err := w.coordinatorServiceClient.SendHeartBeat(context.Background(), &pb.HeartBeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	}); err != nil {
		return err
	}
	return nil
}

func (w *Worker) startGrpcServer() error {
	var err error
	if w.port == "" {
		w.listener, err = net.Listen("tcp", ":0")
		w.port = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port)
	} else {
		w.listener, err = net.Listen("tcp", w.port)
	}

	if err != nil {
		return fmt.Errorf("failed to create listener for grpc server: %s", err)
	}
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)
	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("grpc server failed: %s", err)
		}
	}()
	return nil
}

// submitTask implementation of grpc interface and send the task to the channel
func (w *Worker) SubmitTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("recieved task: %+v", in)
	// add task to the archive map
	w.ReceivedTasksMutex.Lock()
	defer w.ReceivedTasksMutex.Unlock()
	w.ReceivedTasks[in.GetTaskId()] = in
	w.taskQueue <- in
	return &pb.TaskResponse{
		TaskId:  in.TaskId,
		Message: "task submitted",
		Success: true,
	}, nil
}

func (w *Worker) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return w.Stop()
}

func (w *Worker) Stop() error {
	// signal all the goroutines with the context to stop
	// do you need to close the task Queue? since you are using ctx done for all goroutines, its fine if you dont.
	// close(w.taskQueue)
	w.cancel()
	// wait for all the goroutines to exit and complete their processing
	w.wg.Wait()
	// close all worker grpc connections
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}
	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("error while closing the listener: %v", err)
		}
	}
	if err := w.coordinatorServiceConn.Close(); err != nil {
		log.Printf("error while closing client connection with coordinator: %v", err)
	}
	return nil
}
