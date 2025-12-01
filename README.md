# Tempo

Tempos is a background job scheduling and control library

features
 * schedule task to be executed as soon as possible, regularly or any time in the future 
 * limit parallel execution of tasks
 * task status can be queried, running and pending tasks can be canceled.
 * clean shutdown


## Braindump
for now this is only a brain dump of ideas and links

* jobs have a type, similar to go-quarts
* jobs can be scheduled like cron, but cron syntax is not mandatory
* anacron like functionality is provided, figuring out the last execution is up to the job implementor
* jobs can be monitored ( status + status message) and canceled
* option to prevent triggering a job if previous job did not finish yet
* use a pool to run N jobs in parallel
* keep a log of executions
    * classify with info, warn, error (info all good, warn corrective actions ware made, error un recoverable problem)


## How to

### handle shutdown in task

if you have a long-running task, you might want to interrupt quickly the execution to verify if
a shutdown was triggered ( ctx.done() ) and stop your work and clean up, this allows for clean shutdown.
```go
myTask := func(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            fmt.Println("Shutdown received, cleaning up...")
            cleanup()
            return
            
        case <-ticker.C:
            // Do periodic work
            doWork()
        }
    }
}

```