package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/shameerb/go-sched/pkg/common"
	pb "github.com/shameerb/go-sched/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	shutdownTimeout  = 5 * time.Second
	defaultMaxMisses = 1
	scanInterval     = 10 * time.Second
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	serverPort          string
	listener            net.Listener
	grpcServer          *grpc.Server
	WorkerPool          map[uint32]*workerInfo
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeys      []uint32
	WorkerPoolKeysMutex sync.RWMutex
	maxHeartbeatMisses  uint8
	heartbeatInterval   time.Duration
	roundRobinIndex     uint32
	dbConnectionString  string
	dbPool              *pgxpool.Pool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func NewServer(port string, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartBeat,
		dbConnectionString: dbConnectionString,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *CoordinatorServer) Start() error {
	var err error
	go s.manageWorkerPool()
	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server failed to start: %w", err)
	}
	s.dbPool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}
	go s.scanDatabase()
	return s.awaitShutdown()
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", s.serverPort)
	if err != nil {
		return err
	}
	log.Printf("Starting the gRPC servier on %s\n", s.serverPort)
	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)
	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			// log.Fatalf is not a good idea here. Ideally it should propogate the error upwards and let the main function deal with the panic/error
			// this will directly exit the main thread itself with a non zero status code
			// propogate teh error upwards to parent using error channels where the parent is waiting on a select.
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()
	return nil
}

// for an interval ticker, it keeps checking for workers that are inactive and deregisters them.
func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()
	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.removeInactiveWorkers()
		case <-s.ctx.Done():
			return
		}
	}
}

// loop through the list of workers and check if their heartbeat is old.
func (s *CoordinatorServer) removeInactiveWorkers() {
	// since the map of workers are being accessed by multiple grpc clients, its possible that you will access it concurrently. Hence a lock is necessary
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()
	for workerId, worker := range s.WorkerPool {
		// check if the heartbeat miss count is within limit
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", workerId)
			worker.grpcConnection.Close()
			delete(s.WorkerPool, workerId)
			s.WorkerPoolKeysMutex.Lock()
			workerCount := len(s.WorkerPool)
			s.WorkerPoolKeys = make([]uint32, 0, workerCount)
			for k := range s.WorkerPool {
				s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
			}
			s.WorkerPoolKeysMutex.Unlock()
		} else {
			// todo: should this not check if the misses are actually happening?
			worker.heartbeatMisses++
		}
	}
}

func (s *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Why is this a go routine. At every interval anyway this will run on the main thread. same as the check for inactive workers
			go s.executeAllScheduledTasks()
		case <-s.ctx.Done():
			log.Println("Shutting down database scanner")
			return
		}
	}
}

func (s *CoordinatorServer) executeAllScheduledTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := s.dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Unable to start a transaction: %v\n", err)
		return
	}
	// We are forcing a rollback on a commited transaction and checking the exception. If the transaction was not committed then the rollback would succeed.
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != "tx is closed" {
			log.Printf("ERROR: %#v", err)
			log.Printf("Failed to rollback transaction: %v \n", err)
		}
	}()

	rows, err := tx.Query(ctx, `SELECT id, command FROM tasks WHERE scheduled_at < (NOW()+INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("Error executing query: %v\n", err)
		return
	}
	defer rows.Close()

	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("Failed to scan row: %v\n", err)
			continue
		}
		tasks = append(tasks, &pb.TaskRequest{TaskId: id, Data: command})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v\n", err)
		return
	}
	for _, task := range tasks {
		if err := s.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit task %s: %v\n", task.GetTaskId(), err)
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at=NOW() WHERE id=$1`, task.GetTaskId()); err != nil {
			log.Printf("Failed to update task %s: %v\n", task.GetTaskId(), err)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v \n", err)
	}
}

func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return s.Stop()
}

func (s *CoordinatorServer) Stop() error {
	// signal all the goroutines with the context to stop
	s.cancel()
	// wait for all the goroutines to exit and complete their processing
	s.wg.Wait()
	// close all worker grpc connections
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()
	for _, worker := range s.WorkerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}
	s.dbPool.Close()
	return nil
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no worker available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.WorkerPoolKeysMutex.Lock()
	defer s.WorkerPoolKeysMutex.Unlock()
	workerCount := len(s.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}
	worker := s.WorkerPool[s.WorkerPoolKeys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()
	workerID := in.WorkerId
	if worker, ok := s.WorkerPool[workerID]; ok {
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker: ", workerID)
		conn, err := grpc.Dial(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		s.WorkerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		s.WorkerPoolKeysMutex.Lock()
		defer s.WorkerPoolKeysMutex.Unlock()
		workerCount := len(s.WorkerPool)
		s.WorkerPoolKeys = make([]uint32, 0, workerCount)
		for k := range s.WorkerPool {
			s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
		}
		log.Println("Registered worker: ", workerID)
	}
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	data := in.Data
	taskId := uuid.New().String()
	task := &pb.TaskRequest{
		TaskId: taskId,
		Data:   data,
	}
	if err := s.submitTaskToWorker(task); err != nil {
		return nil, err
	}

	return &pb.ClientTaskResponse{
		Message: "Task submitted successfully",
		TaskId:  taskId,
	}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, in *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := in.Status
	taskId := in.TaskId
	var timestamp time.Time
	var column string

	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(in.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_COMPLETE:
		timestamp = time.Unix(in.GetStartedAt(), 0)
		column = "completed_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(in.GetStartedAt(), 0)
		column = "failed_at"
	default:
		log.Println("Invalid status in UpdateTaskStatusRequest")
		return nil, errors.New("Unsupported status")
	}

	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := s.dbPool.Exec(ctx, sqlStatement, timestamp, taskId)
	if err != nil {
		log.Printf("Could not update task status for task %s: %+v", taskId, err)
		return nil, err
	}
	return &pb.UpdateTaskStatusResponse{Success: true}, nil

}
