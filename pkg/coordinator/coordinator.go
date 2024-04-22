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

	"github.com/shameerb/go-sched/pkg/common"
	pb "github.com/shameerb/go-sched/pkg/grpcapi"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	scanInterval     = 10 * time.Second
	defaultMaxMisses = 1
	txnTimeout       = 30 * time.Second
)

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

// remote add, grpc client, heartbeat updates count, grpcConnection
type workerInfo struct {
	addr                string
	heartbeatMisses     uint8
	grpcClientConn      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func NewCoordinator(port, dbConnString string) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		workerPool:         make(map[uint32]*workerInfo),
		maxHeartBeatMisses: defaultMaxMisses,
		heartBeatInterval:  common.DefaultHeartBeat,
		port:               port,
		dbConnString:       dbConnString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

/*
- Start a grpc server
- manage the worker pool
  - keep a map of all workers
  - healthcheck for all workers (healthcheck is updated by the worker through a push)

- poll the psql for tasks that are to be executed.
  - get the next worker to submit the task to (round robin algo)
  - call the submit on the grpc client to the worker.

-
*/
func (c *Coordinator) Start() error {
	// create a grpc server for workers to publish their healthchecks
	if err := c.startGrpcServer(); err != nil {
		log.Fatalf("error in starting grpc server: %s", err)
	}

	// connect to the database
	var err error
	c.pool, err = common.ConnectToDatabase(c.ctx, c.dbConnString)
	if err != nil {
		log.Fatalf("error connecting to psql database: %s", err)
	}

	// goroutine to poll the database/table for new tasks.
	go c.pollForTasks()
	go c.manageWorkers()
	return c.awaitShutdown()
}

// for an interval ticker, it keeps checking for workers that are inactive and deregisters them.
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

// loop through the list of workers and check if their heartbeat is old.
func (c *Coordinator) removeInactive() {
	// since the map of workers are being accessed by multiple grpc clients, its possible that you will access it concurrently. Hence a lock is necessary
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()
	for id, worker := range c.workerPool {
		if worker.heartbeatMisses > c.maxHeartBeatMisses {
			log.Printf("removing the worker: %d", id)
			worker.grpcClientConn.Close()
			delete(c.workerPool, id)
			c.recreateWorkerPoolKeys()
		} else {
			worker.heartbeatMisses++
		}
	}
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
			// log.Fatalf is not a good idea here. Ideally it should propogate the error upwards and let the main function deal with the panic/error
			// this will directly exit the main thread itself with a non zero status code
			// propogate teh error upwards to parent using error channels where the parent is waiting on a select.
			log.Fatalf("grpc server failed: %s", err)
		}
	}()
	return nil
}

func (c *Coordinator) pollForTasks() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Why is this a go routine. At every interval anyway this will run on the main thread. same as the check for inactive workers
			go c.runScheduledTasks()
		case <-c.ctx.Done():
			log.Printf("shutting down polling for tasks.")
			return
		}
	}
}

func (c *Coordinator) runScheduledTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		log.Printf("unable to start a transaction: %v", err)
		return
	}
	// We are forcing a rollback on a commited transaction and checking the exception. If the transaction was not committed then the rollback would succeed.
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != "tx is closed" {
			log.Printf("error: %#v", err)
			log.Printf("failed to rollback transaction: %v", err)
		}
	}()

	rows, err := tx.Query(ctx, `SELECT id, command FROM tasks WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL ORDER BY scheduled_at FOR UPDATE SKIP LOCKED`)
	if err != nil {
		log.Printf("error fetching rows: %s", err)
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
			// ideally mark these as errored and send to a deadletter queue.
			log.Printf("failed to submit task: %s, %v", task.GetTaskId(), err)
			continue
		}
		log.Printf("Update tasks: %s", task.GetTaskId())
		if _, err := tx.Exec(ctx, `UPDATE tasks SET picked_at=NOW() WHERE id=$1`, task.GetTaskId()); err != nil {
			log.Printf("failed to update task %s: %v", task.GetTaskId(), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
	}
}

func (c *Coordinator) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := c.getNextWorker()
	if worker == nil {
		return errors.New("no worker available to execute the tasks")
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
	// doesnt this require a lock on the worker pool to read ? what happens if its getting removed at the same time its being read ?
	worker := c.workerPool[c.workerPoolKeys[c.roundRobinIndex%uint32(workerCount)]]
	c.roundRobinIndex++
	return worker
}

// implementation for sendheartbeat used by the workers to update their heartbeat. It uses a push instead of a poll from the broker to all workers.
func (c *Coordinator) SendHeartBeat(ctx context.Context, in *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()
	id := in.GetWorkerId()
	if worker, ok := c.workerPool[id]; !ok {
		// register the worker if missing in the worker pool
		log.Printf("registering worker : %d", id)
		conn, err := grpc.Dial(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.workerPool[id] = &workerInfo{
			addr:                in.GetAddress(),
			grpcClientConn:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		c.recreateWorkerPoolKeys()
		log.Printf("registration successfull: %d", id)
		c.workerPoolKeysMutex.Unlock()
	} else {
		// reset the heartbeat count to 0
		worker.heartbeatMisses = 0
	}
	return &pb.HeartBeatResponse{Acknowledged: true}, nil
}

func (c *Coordinator) recreateWorkerPoolKeys() {
	c.workerPoolKeysMutex.Lock()
	workerCount := len(c.workerPool)
	c.workerPoolKeys = make([]uint32, 0, workerCount)
	for k := range c.workerPool {
		c.workerPoolKeys = append(c.workerPoolKeys, k)
	}
}

// UpdateTaskStatus used by the worker to update the task status. Since there are hundreds of tasks sending status updates. It might not scale.
// might make sense to put a queue in between to capture the trail of events and process the status in a serialized manner.
// The problem is the order of the status for the same id. You will need to make sure that timestamp is synchronized across the servers (difficult problem to solve)
func (c *Coordinator) UpdateTaskStatus(ctx context.Context, in *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := in.GetStatus()
	id := in.GetTaskId()
	var timestamp time.Time
	var column string
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
		log.Printf("invalid status in UpdateTaskStatus grpc call: %s - %s", id, status)
		return nil, errors.New("invalid status")
	}
	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)
	_, err := c.pool.Exec(ctx, sqlStatement, timestamp, id)
	if err != nil {
		log.Printf("failed to update task in psql %s : %+v", id, err)
		return nil, err
	}
	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (c *Coordinator) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return c.Stop()
}

func (c *Coordinator) Stop() error {
	// signal all the goroutines with the context to stop
	c.cancel()
	// wait for all the goroutines to exit and complete their processing
	c.wg.Wait()
	// close all worker grpc connections
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()
	for _, worker := range c.workerPool {
		if worker.grpcClientConn != nil {
			worker.grpcClientConn.Close()
		}
	}
	c.grpcServer.GracefulStop()
	c.listener.Close()
	c.pool.Close()
	return nil
}
