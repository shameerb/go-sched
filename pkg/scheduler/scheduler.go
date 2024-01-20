package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/shameerb/go-sched/pkg/common"
)

// create an http server to take in the task and save to db.
type SchedulerServer struct {
	serverPort         string
	dbConnectionString string
	dbPool             *pgxpool.Pool
	ctx                context.Context
	cancel             context.CancelFunc
	httpServer         *http.Server
}

type CommandRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"`
}

type Task struct {
	Id          string
	Command     string
	ScheduledAt pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
}

func NewServer(port, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:         port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		log.Printf("Could not connect to database :%v\n", err)
		return err
	}
	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status/", s.handleGetTaskStatus)

	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}
	log.Printf("Starting scheudler server on %s\n", s.serverPort)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil {
			log.Fatalf("Server error: %s\n", err)
		}
	}()

	return s.awaitShutdown()
}

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var commandReq CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Printf("Received schedule request: %+v", commandReq)

	scheduledTime, err := time.Parse(time.RFC3339, commandReq.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid dat eformat. Use ISO 8601 format.", http.StatusBadRequest)
		return
	}

	unixTimestamp := time.Unix(scheduledTime.Unix(), 0)
	taskId, err := s.insertTaskIntoDB(context.Background(), Task{Command: commandReq.Command, ScheduledAt: pgtype.Timestamp{Time: unixTimestamp}})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskID      string `json:"task_id"`
	}{
		Command:     commandReq.Command,
		ScheduledAt: unixTimestamp.Unix(),
		TaskID:      taskId,
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	taskId := r.URL.Query().Get("task_id")
	if taskId == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	var task Task
	if err := s.dbPool.QueryRow(context.Background(), "SELECT * FROM tasks WHERE id = $1", taskId).Scan(&task.Id, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt); err != nil {
		http.Error(w, fmt.Sprintf("Failed to get task status. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	response := struct {
		TaskID      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduledAt string `json:"scheduled_at,omitempty"`
		PickedAt    string `json:"picked_at,omitempty"`
		StartedAt   string `json:"started_at,omitempty"`
		CompletedAt string `json:"completed_at,omitempty"`
		FailedAt    string `json:"failed_at,omitempty"`
	}{
		TaskID:      task.Id,
		Command:     task.Command,
		ScheduledAt: "",
		PickedAt:    "",
		StartedAt:   "",
		CompletedAt: "",
		FailedAt:    "",
	}

	if task.ScheduledAt.Status == 2 {
		response.ScheduledAt = task.ScheduledAt.Time.String()
	}
	if task.PickedAt.Status == 2 {
		response.PickedAt = task.PickedAt.Time.String()
	}
	if task.StartedAt.Status == 2 {
		response.StartedAt = task.StartedAt.Time.String()
	}
	if task.CompletedAt.Status == 2 {
		response.CompletedAt = task.CompletedAt.Time.String()
	}
	if task.FailedAt.Status == 2 {
		response.FailedAt = task.FailedAt.Time.String()
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed to marshal JSON response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id "
	var insertId string
	if err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt.Time).Scan(&insertId); err != nil {
		return "", err
	}

	return insertId, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	s.dbPool.Close()
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.httpServer.Shutdown(ctx)
	}
	log.Println("Scheduler server and database pool stopped")
	return nil
}
