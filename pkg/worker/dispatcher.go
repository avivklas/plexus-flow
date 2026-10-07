package worker

// Dispatcher receives the tasks the engine commits for execution. The local
// Executor is one implementation; the remote broker hands tasks to workers
// connected over gRPC.
type Dispatcher interface {
	DispatchActivity(task ActivityTask)
	DispatchCompensation(task CompensationTask)
}

// DispatchActivity implements Dispatcher by running the activity in-process.
func (e *Executor) DispatchActivity(task ActivityTask) { e.ExecuteActivity(task) }

// DispatchCompensation implements Dispatcher by running the compensation in-process.
func (e *Executor) DispatchCompensation(task CompensationTask) { e.ExecuteCompensation(task) }
