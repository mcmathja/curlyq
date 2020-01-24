package curlyq

import (
	"fmt"
)

type queue struct {
	name string

	activeJobsList     string
	consumersSet       string
	deadJobsSet        string
	inflightJobsPrefix string
	jobDataHash        string
	scheduledJobsSet   string
}

type queueOpts struct {
	Name string
}

func newQueue(opts *queueOpts) *queue {
	if opts.Name == "" {
		panic("A queue name must be provided.")
	}

	activeJobsList := fmt.Sprintf("%s:active", opts.Name)
	consumersSet := fmt.Sprintf("%s:consumers", opts.Name)
	deadJobsSet := fmt.Sprintf("%s:dead", opts.Name)
	inflightJobsPrefix := fmt.Sprintf("%s:inflight", opts.Name)
	jobDataHash := fmt.Sprintf("%s:data", opts.Name)
	scheduledJobsSet := fmt.Sprintf("%s:scheduled", opts.Name)

	return &queue{
		name: opts.Name,

		activeJobsList:     activeJobsList,
		consumersSet:       consumersSet,
		deadJobsSet:        deadJobsSet,
		inflightJobsPrefix: inflightJobsPrefix,
		jobDataHash:        jobDataHash,
		scheduledJobsSet:   scheduledJobsSet,
	}
}
