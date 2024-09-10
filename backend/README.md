## Services
- scheduler
    - accepts requests and adds them to psql - task table
- coordinator
    - runs loop to check for tasks to be run
    - picks up a worker and sends a request to the worker
    - runs health check for workers
- worker
    - accepts tasks from grpc to run the task
    - publishes health check post alive call

set -x POSTGRES_USER postgres
set -x  POSTGRES_PASSWORD postgres
set -x  POSTGRES_DB postgres
set -x  POSTGRES_HOST scheduler


### Tasks
- [ ] printout the list of web servers at the end of docker-compose up using env host:ports for scheduler and worker's
- [ ] address from scheduler, coordinator to worker dynamically from dockerfile
- [ ] api endpoints in the frontend code needs to be env
- [ ] setting the number of workers to be spawned