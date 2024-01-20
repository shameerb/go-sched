# go-sched
Distributed scheduler service


```bash
curl -X POST localhost:8081/schedule -d '{"command":"Hi","scheduled_at":"2023-12-25T22:34:00+05:30"}'
curl localhost:8081/status?task_id=<task-id>
```

## Reference
- https://jyotinder.substack.com/p/designing-a-distributed-task-scheduler
- https://queue.acm.org/detail.cfm?id=2745840