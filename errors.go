package curlyq

import (
	"fmt"
)

type ErrFailedToRetryJob struct {
	Job Job
	Err error
}

func (e ErrFailedToRetryJob) Error() string {
	return fmt.Sprintf("Failed to retry job %s: %s", e.Job.ID, e.Err.Error())
}

type ErrFailedToKillJob struct {
	Job Job
	Err error
}

func (e ErrFailedToKillJob) Error() string {
	return fmt.Sprintf("Failed to kill job %s: %s", e.Job.ID, e.Err.Error())
}

type ErrFailedToAckJob struct {
	Job Job
	Err error
}

func (e ErrFailedToAckJob) Error() string {
	return fmt.Sprintf("Failed to acknowledge job %s: %s", e.Job.ID, e.Err.Error())
}
