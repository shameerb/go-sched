## task-scheduler
Distributed scheduler service

### Features
Takes up tasks to be scheduled at a particular time. Can be scaled up to multiple workers consuming the tasks.

### Components
- Scheduler 
    - Api server for receiving the task request and checking the status of the task
- Coordinator
    - Central registry service managing worker health and responsible for assigning tasks to the workers
    - Picks up the pending tasks to be run from a persistent store stored by the scheduler (postgres) and sends it to the appropriate workers
- Worker
    - Runs a pool of goroutine workers that runs the task from an in memory store (channel) and sends an update to the coordinator about the task status.
    - updates its heartbeat periodically to the coordinator service.

### Run
- for local run create the user/database/tables and set the env variables
```bash

set -x POSTGRES_HOST localhost # for fish shell, use export for normal bash
set -x POSTGRES_DB postgres
set -x POSTGRES_PASSWORD postgres
set -x POSTGRES_USER postgres
```

```bash
docker-compose up -d --build

curl -X POST localhost:8081/schedule -d '{"command":"Hi","scheduled_at":"2023-12-25T22:34:00+05:30"}'
curl localhost:8081/status?task_id=<task-id>
```

### Endpoints (UI)
- scheduler
    - http://localhost:8081
- worker
    - http://localhost:8100
    - http://localhost:8101


### Enhancements (todos)
- scheduler
    - multiple schedulers to manage the load of incoming traffic
- coordinator
    - different strategies for submitting task (round robin, least connection) to workers
- worker
    - should reconnect to coordinator in case of network failure or restarts (keep a threshold)
    - process once. task processing and sending should be idempotent
- scale
    - should allow for scale of scheduler, coordinator service as well. Leader Election, traffic balancing etc.