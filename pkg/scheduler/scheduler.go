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

	"github.com/shameerb/go-sched/pkg/common"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	port         string
	dbConnString string
	pool         *pgxpool.Pool
	httpServer   *http.Server
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewScheduler(port, dbConnString string) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		port:         port,
		dbConnString: dbConnString,
		ctx:          ctx,
		cancel:       cancel,
	}
}

/*
	todo: implement the following tasks

- http server
  - schedule task and push to psql
  - get status of task

- psql pool initialize
*/
func (s *Scheduler) Start() error {
	var err error
	s.pool, err = common.ConnectToDatabase(s.ctx, s.dbConnString)
	if err != nil {
		log.Fatalf("failed to get connection to database: %s", err)
	}
	// initialize an http server
	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status/", s.handleGetTaskStatus)
	s.httpServer = &http.Server{
		Addr: s.port,
	}
	log.Printf("statring scheduler service http server on %s\n", s.port)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil {
			log.Fatalf("error starting http server: %s\n", err)
		}
	}()

	return s.awaitShutdown()
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

func (s *Scheduler) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var commandReq CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	scheduledTime, err := time.Parse(time.RFC3339, commandReq.ScheduledAt)
	if err != nil {
		http.Error(w, "invalid date format. Use ISO 8601", http.StatusBadRequest)
		return
	}
	unixTimestamp := time.Unix(scheduledTime.Unix(), 0)
	// insert into database
	taskId, err := s.insertIntoDB(context.Background(), Task{Command: commandReq.Command, ScheduledAt: pgtype.Timestamp{Time: unixTimestamp}})
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to submit task. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskId      string `json:"task_id"`
	}{
		Command:     commandReq.Command,
		ScheduledAt: unixTimestamp.Unix(),
		TaskId:      taskId,
	}
	jsonresponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonresponse)
}

func (s *Scheduler) insertIntoDB(ctx context.Context, task Task) (string, error) {
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id "
	var insertId string
	if err := s.pool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt.Time).Scan(&insertId); err != nil {
		return "", err
	}
	return insertId, nil
}

func (s *Scheduler) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	taskId := r.URL.Query().Get("task_id")
	if taskId == "" {
		http.Error(w, "task id is required parameter", http.StatusBadRequest)
		return
	}
	var task Task
	getQuery := "SELECT * FROM tasks WHERE id = $1 "
	if err := s.pool.QueryRow(context.Background(), getQuery, taskId).Scan(&task.Id, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt); err != nil {
		http.Error(w, fmt.Sprintf("failed to get status: %s", err.Error()), http.StatusInternalServerError)
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
	if task.ScheduledAt.Valid {
		response.ScheduledAt = task.ScheduledAt.Time.String()
	}
	if task.PickedAt.Valid {
		response.PickedAt = task.PickedAt.Time.String()
	}
	if task.StartedAt.Valid {
		response.StartedAt = task.StartedAt.Time.String()
	}
	if task.CompletedAt.Valid {
		response.CompletedAt = task.CompletedAt.Time.String()
	}
	if task.FailedAt.Valid {
		response.FailedAt = task.FailedAt.Time.String()
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "faield to marshal json response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)

}

func (s *Scheduler) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	return s.Stop()
}

func (s *Scheduler) Stop() error {
	s.pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
