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

	"scheduler/pkg/common"

	"github.com/gorilla/mux"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	distPath = "./dist"

// distPath = "/Users/shameer/Documents/Personal/ComputerScience/General/Projects/Rough/Projects/scheduler/frontend/packages/scheduler/dist"
)

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

type Scheduler struct {
	port         string
	dbConnString string
	httpServer   *http.Server
	pool         *pgxpool.Pool
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

func (s *Scheduler) Start() error {
	var err error
	s.pool, err = common.ConnectToDatabase(s.ctx, s.dbConnString)
	if err != nil {
		log.Fatalf("failed to get connect to db: %s\n", err)
	}

	if err := s.startHttpServer(); err != nil {
		log.Fatalf("error creating http server: %s", err)
	}

	return s.shutdown()
}

func (s *Scheduler) startHttpServer() error {
	// create a new router
	r := mux.NewRouter()

	// Api routes
	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/status/{id}", s.getStatus).Methods("GET")
	api.HandleFunc("/schedule", s.scheduleTask).Methods("POST")

	// serve static files
	r.PathPrefix("/").Handler(http.FileServer(http.Dir(distPath))).Methods("GET")

	log.Printf("starting scheduler service: %s\n", s.port)
	go func() {
		if err := http.ListenAndServe(s.port, r); err != nil {
			log.Fatalf("error starting http server: %s\n", err)
		}
	}()
	return nil
}

func (s *Scheduler) scheduleTask(w http.ResponseWriter, r *http.Request) {
	fmt.Println("scheduleTask")
	var commandRequest CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	scheduledTime, err := time.Parse(time.RFC3339, commandRequest.ScheduledAt)
	if err != nil {
		http.Error(w, "invalid date format. Use ISO 8601", http.StatusBadRequest)
		return
	}
	unixTimestamp := time.Unix(scheduledTime.Unix(), 0)
	taskId, err := s.insertIntoDb(context.Background(), Task{
		Command:     commandRequest.Command,
		ScheduledAt: pgtype.Timestamp{Time: unixTimestamp},
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to submit task to db: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskId      string `json:"task_id"`
	}{
		Command:     commandRequest.Command,
		ScheduledAt: unixTimestamp.Unix(),
		TaskId:      taskId,
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (s *Scheduler) getStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Println("getStatus")
	vars := mux.Vars(r)
	taskId := vars["id"]
	fmt.Printf("taskId: %s\n", taskId)

	var task Task
	getQuery := "SELECT * FROM tasks WHERE id = $1"
	if err := s.pool.QueryRow(context.Background(), getQuery, taskId).Scan(&task.Id, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt); err != nil {
		http.Error(w, fmt.Sprintf("failed to get status: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	response := struct {
		TaskId      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduledAt string `json:"scheduled_at,omitempty"`
		PickedAt    string `json:"picked_at,omitempty"`
		StartedAt   string `json:"started_at,omitempty"`
		CompletedAt string `json:"completed_at,omitempty"`
		FailedAt    string `json:"failed_at,omitempty"`
	}{
		TaskId:      task.Id,
		Command:     task.Command,
		ScheduledAt: task.ScheduledAt.Time.String(),
		PickedAt:    task.ScheduledAt.Time.String(),
		StartedAt:   task.StartedAt.Time.String(),
		CompletedAt: task.CompletedAt.Time.String(),
		FailedAt:    task.FailedAt.Time.String(),
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "faield to marshal json response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (s *Scheduler) insertIntoDb(ctx context.Context, task Task) (string, error) {
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id"
	var insertId string
	if err := s.pool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt.Time).Scan(&insertId); err != nil {
		return "", err
	}
	return insertId, nil
}

func (s *Scheduler) shutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	return s.stop()
}

func (s *Scheduler) stop() error {
	s.pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
