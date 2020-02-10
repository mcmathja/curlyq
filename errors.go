package curlyq

import (
	"fmt"
)

// ErrFailedToRetryJob indicates an error when scheduling a retry.
// It is considered a fatal error that should shut down the consumer.
type ErrFailedToRetryJob struct {
	Job Job
	Err error
}

func (e ErrFailedToRetryJob) Error() string {
	return fmt.Sprintf("Failed to retry job %s: %s", e.Job.ID, e.Err.Error())
}

// ErrFailedToKillJob indicates an error when marking a job as dead.
// It is considered a fatal error that should shut down the consumer.
type ErrFailedToKillJob struct {
	Job Job
	Err error
}

func (e ErrFailedToKillJob) Error() string {
	return fmt.Sprintf("Failed to kill job %s: %s", e.Job.ID, e.Err.Error())
}

// ErrFailedToAckJob indicates an error when acknowledging a completed job.
// It is considered a fatal error that should shut down the consumer.
type ErrFailedToAckJob struct {
	Job Job
	Err error
}

func (e ErrFailedToAckJob) Error() string {
	return fmt.Sprintf("Failed to acknowledge job %s: %s", e.Job.ID, e.Err.Error())
}

// ErrExceededMaxBackoff indicates a polling loop exceeded a maximum number of backoffs.
// It is considered a fatal error that should shut down the consumer.
type ErrExceededMaxBackoff struct {
	Attempt int
	Process string
}

func (e ErrExceededMaxBackoff) Error() string {
	return fmt.Sprintf("Process %s exceeded maximum %d backoff attempts", e.Process, e.Attempt)
}
