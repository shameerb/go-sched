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
    - Runs a pool of goroutine workers that runs the task from an in memory store (channel) and sends an update to the coordinator about the task status

### Commands
```bash
curl -X POST localhost:8081/schedule -d '{"command":"Hi","scheduled_at":"2023-12-25T22:34:00+05:30"}'
curl localhost:8081/status?task_id=<task-id>
```

#### Reference
- https://jyotinder.substack.com/p/designing-a-distributed-task-scheduler
