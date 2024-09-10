package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"scheduler/pkg/common"
	pb "scheduler/pkg/grpcapi"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
	distPath        = "./dist"
	// distPath = "/Users/shameer/Documents/Personal/ComputerScience/General/Projects/Rough/Projects/scheduler/frontend/packages/viewer/dist"
)

type Task struct {
	Id      string `json:"id"`
	Command string `json:"command"`
}

type Worker struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	httpPort                 string
	grpcPort                 string
	coordinatorAddr          string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorServiceConn   *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	webSocketQueue           chan *Task
	ReceivedTasks            map[string]*pb.TaskRequest
	ReceivedTasksMutex       sync.Mutex
	ws                       *websocket.Conn
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

func NewWorker(httpPort, grpcPort, coordinatorPort string) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		id:                uuid.New().ID(),
		httpPort:          httpPort,
		grpcPort:          grpcPort,
		coordinatorAddr:   coordinatorPort,
		heartbeatInterval: common.DefaultHeartBeat,
		taskQueue:         make(chan *pb.TaskRequest, 100),
		webSocketQueue:    make(chan *Task, 100),
		ReceivedTasks:     make(map[string]*pb.TaskRequest),
		ctx:               ctx,
		cancel:            cancel,
	}
}

/*
- worker pool to execute tasks
- grpc server to accept tasks
- client grpc coodrinator to send health check and update task status
*/
func (w *Worker) Start() error {
	w.startWorkerPool()
	if err := w.startHttpServer(); err != nil {
		log.Fatalf("error creating http server: %s", err)
	}
	if err := w.startGrpcServer(); err != nil {
		log.Fatalf("error creating grpc server: %s", err)
	}
	if err := w.connectToCoordinator(); err != nil {
		log.Fatalf("error connecting to coordinator: %s", err)
	}
	go w.healthCheck()
	return w.awaitShutdown()
}

func (w *Worker) startHttpServer() error {
	r := mux.NewRouter()
	r.HandleFunc("/ws", w.createWsConnection)
	r.PathPrefix("/").Handler(http.FileServer(http.Dir(distPath))).Methods("GET")

	go func() {
		if err := http.ListenAndServe(w.httpPort, r); err != nil {
			log.Fatalf("error starting http server: %s\n", err)
		}
	}()
	return nil
}

func (w *Worker) createWsConnection(wr http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	var err error
	w.ws, err = upgrader.Upgrade(wr, r, nil)
	if err != nil {
		log.Println("err")
		return
	}
	go func() {
		for {
			task := <-w.webSocketQueue
			var err error
			msg, err := json.Marshal(task)
			if err != nil {
				log.Printf("error while marshalling received task: %v", err)
				continue
			}
			err = w.ws.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("connection lost when read %v", err)
			}
		}
		// for {
		// 	task := <-w.webSocketQueue
		// 	err := w.ws.WriteMessage(websocket.TextMessage, []byte(task.Command))
		// 	if err != nil {
		// 		log.Printf("connection lost when read %v", err)
		// 	}
		// }
	}()
}

func (w *Worker) startGrpcServer() error {
	var err error
	if w.grpcPort == "" {
		w.listener, err = net.Listen("tcp", ":0")
		w.grpcPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port)
	} else {
		w.listener, err = net.Listen("tcp", w.grpcPort)
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

func (w *Worker) connectToCoordinator() error {
	log.Println("connecting to coordinator service")
	var err error
	w.coordinatorServiceConn, err = grpc.NewClient(w.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("could not connect to coordinator: %s", err)
		return err
	}
	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorServiceConn)
	log.Println("connected to coordinator service")
	return nil
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

func (w *Worker) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	updatetaskRequest := &pb.UpdateTaskStatusRequest{
		TaskId: task.GetTaskId(),
		Status: status,
	}
	switch status {
	case pb.TaskStatus_STARTED:
		updatetaskRequest.StartedAt = time.Now().Unix()
	case pb.TaskStatus_COMPLETED:
		updatetaskRequest.CompletedAt = time.Now().Unix()
	}
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), updatetaskRequest)
}

func (w *Worker) processTask(task *pb.TaskRequest) {
	log.Printf("starting task: %+v", task)
	w.webSocketQueue <- &Task{
		Id:      task.TaskId,
		Command: task.Data,
	}
	time.Sleep(taskProcessTime)
	log.Printf("completed task: %+v", task)
}

func (w *Worker) healthCheck() {
	w.wg.Add(1)
	defer w.wg.Done()
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartBeat(); err != nil {
				log.Printf("failed to send heartbeat: %v", err)
				log.Printf("failed to connect to coordinator: %s", err)
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) sendHeartBeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.grpcPort
	}
	if _, err := w.coordinatorServiceClient.SendHeartBeat(context.Background(), &pb.HeartBeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	}); err != nil {
		return err
	}
	return nil
}

func (w *Worker) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return w.stop()
}

func (w *Worker) stop() error {
	// signal all goroutines with context to end.
	w.cancel()
	// wait for all goroutines to exit and complete processing
	w.wg.Wait()
	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("error while closing listener: %v", err)
		}
	}
	if err := w.coordinatorServiceConn.Close(); err != nil {
		log.Printf("error while closing client connection to coordinator: %v", err)
	}
	return nil
}

// implementing grpc server side functions
func (w *Worker) SubmitTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("received task: %+v", in)
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
