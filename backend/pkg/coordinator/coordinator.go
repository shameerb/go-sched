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

	"scheduler/pkg/common"
	pb "scheduler/pkg/grpcapi"

	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
runs a grpc server
	- receives health checks from workers
	- map of workers
	- polls psql and sends request to worker
*/

const (
	scanInterval     = 10 * time.Second
	defaultMaxMisses = 1
	txnTimeout       = 30 * time.Second
)

type workerInfo struct {
	addr            string
	heartbeatMisses uint8
	grpcClientConn  *grpc.ClientConn
	// client itself
	workerServiceClient pb.WorkerServiceClient
}

type Coordinator struct {
	pb.UnimplementedCoordinatorServiceServer
	port                string
	dbConnString        string
	pool                *pgxpool.Pool
	listener            net.Listener
	grpcServer          *grpc.Server
	ctx                 context.Context
	cancel              context.CancelFunc
	workerPool          map[uint32]*workerInfo
	workerPoolMutex     sync.Mutex
	workerPoolKeys      []uint32
	workerPoolKeysMutex sync.RWMutex
	maxHeartBeatMisses  uint8
	heartBeatInterval   time.Duration
	roundRobinIndex     uint32
	wg                  sync.WaitGroup
}

func NewCoordinator(port, dbConnString string) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		port:               port,
		dbConnString:       dbConnString,
		maxHeartBeatMisses: defaultMaxMisses,
		heartBeatInterval:  common.DefaultHeartBeat,
		workerPool:         make(map[uint32]*workerInfo),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (c *Coordinator) Start() error {
	c.startGrpcServer()
	// pollForTasks and sent to worker
	// manageWorkers - healthcheck for all workers update map
	var err error
	c.pool, err = common.ConnectToDatabase(c.ctx, c.dbConnString)
	if err != nil {
		log.Fatalf("error connecting to database: %s", err)
	}
	go c.pollForTasks()
	go c.manageWorkers()
	return c.shutdown()
}

func (c *Coordinator) startGrpcServer() error {
	var err error
	c.listener, err = net.Listen("tcp", c.port)
	if err != nil {
		return fmt.Errorf("failed to create listener for grpc server: %s", err)
	}
	c.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(c.grpcServer, c)
	go func() {
		if err := c.grpcServer.Serve(c.listener); err != nil {
			log.Fatalf("grpc server failed: %s", err)
		}
	}()
	return nil
}

func (c *Coordinator) manageWorkers() {
	c.wg.Add(1)
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(c.maxHeartBeatMisses) * c.heartBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.removeInactive()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Coordinator) removeInactive() {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()
	for id, wi := range c.workerPool {
		if wi.heartbeatMisses > c.maxHeartBeatMisses {
			wi.grpcClientConn.Close()
			delete(c.workerPool, id)
			c.recreateWorkerPoolKeys()
		} else {
			wi.heartbeatMisses++
		}
	}
}

func (c *Coordinator) recreateWorkerPoolKeys() {
	c.workerPoolKeysMutex.Lock()
	defer c.workerPoolKeysMutex.Unlock()
	workerCount := len(c.workerPool)
	c.workerPoolKeys = make([]uint32, 0, workerCount)
	for k := range c.workerPool {
		c.workerPoolKeys = append(c.workerPoolKeys, k)
	}
}

func (c *Coordinator) pollForTasks() {
	c.wg.Add(1)
	defer c.wg.Done()
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go c.runScheduledTask()
		case <-c.ctx.Done():
			log.Println("shutting down goroutine for poll tasks")
			return
		}
	}
}

func (c *Coordinator) runScheduledTask() {
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		log.Printf("unable to start a transaction: %v", err)
	}
	rows, err := tx.Query(ctx, `SELECT id, command FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("error fetchin rows: %s", err)
		return
	}
	defer rows.Close()
	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("failed to scan row: %v", err)
			continue
		}
		tasks = append(tasks, &pb.TaskRequest{TaskId: id, Data: command})
	}
	if err := rows.Err(); err != nil {
		log.Printf("error iterating rows: %v", err)
		return
	}
	for _, task := range tasks {
		if err := c.submitTaskToWorker(task); err != nil {
			log.Printf("failed to submit task: %s, %v", task.GetTaskId(), err)
			continue
		}
		log.Printf("Update tasks: %s", task.GetTaskId())
		if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at=NOW() WHERE id=$1`, task.GetTaskId()); err != nil {
			log.Printf("failed to update task %s: %v", task.GetTaskId(), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("failed to commit transaction: %v\n", err)
	}
}

func (c *Coordinator) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := c.getNextWorker()
	if worker == nil {
		return errors.New("no available worker")
	}
	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (c *Coordinator) getNextWorker() *workerInfo {
	c.workerPoolKeysMutex.RLock()
	defer c.workerPoolKeysMutex.RUnlock()
	workerCount := len(c.workerPoolKeys)
	if workerCount == 0 {
		return nil
	}
	worker := c.workerPool[c.workerPoolKeys[c.roundRobinIndex%uint32(workerCount)]]
	c.roundRobinIndex++
	return worker
}

func (c *Coordinator) SendHeartBeat(ctx context.Context, in *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()
	id := in.GetWorkerId()
	if worker, ok := c.workerPool[id]; !ok {
		log.Printf("registering worker : %d", id)
		conn, err := grpc.NewClient(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.workerPool[id] = &workerInfo{
			addr:                in.GetAddress(),
			grpcClientConn:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		c.recreateWorkerPoolKeys()
		log.Printf("registration successful: %d", id)
	} else {
		worker.heartbeatMisses = 0
	}
	return &pb.HeartBeatResponse{Acknowledged: true}, nil
}

func (c *Coordinator) UpdateTaskStatus(ctx context.Context, in *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := in.GetStatus()
	id := in.GetTaskId()
	var column string
	var timestamp time.Time
	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(in.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(in.GetFailedAt(), 0)
		column = "failed_at"
	case pb.TaskStatus_COMPLETED:
		timestamp = time.Unix(in.GetCompletedAt(), 0)
		column = "completed_at"
	default:
		log.Printf("invalid status in UpdateTaskRequest grpc call: %s - %s", id, status)
		return nil, errors.New("invalid status")
	}
	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := c.pool.Exec(ctx, sqlStatement, timestamp, id)
	if err != nil {
		log.Printf("failed to update task status in psql %s: %+v", id, err)
		return nil, err
	}
	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (c *Coordinator) shutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return c.stop()
}

func (c *Coordinator) stop() error {
	// cancel all open connections
	// signall al goroutines with this context to abort
	c.cancel()
	// wait for all the goroutines to exit and complete thier processing
	c.wg.Wait()
	// call the close on all the workers grpc client
	for _, wi := range c.workerPool {
		if wi.grpcClientConn != nil {
			wi.grpcClientConn.Close()
		}
	}
	c.grpcServer.GracefulStop()
	c.pool.Close()
	return nil
}
