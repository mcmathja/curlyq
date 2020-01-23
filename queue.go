package curlyq

import (
	"fmt"
)

type Queue struct {
	name string

	activeJobsList     string
	consumersSet       string
	deadJobsSet        string
	inflightJobsPrefix string
	jobDataHash        string
	scheduledJobsSet   string
}

type QueueOpts struct {
	Name   string
	Prefix string
}

func NewQueue(opts *QueueOpts) *Queue {
	if opts.Name == "" {
		panic("A queue name must be provided.")
	}

	basePath := fmt.Sprintf("curlyq:%s", opts.Name)
	if opts.Prefix != "" {
		basePath = fmt.Sprintf("%s:%s", opts.Prefix, basePath)
	}

	activeJobsList := fmt.Sprintf("%s:active", basePath)
	consumersSet := fmt.Sprintf("%s:consumers", basePath)
	deadJobsSet := fmt.Sprintf("%s:dead", basePath)
	inflightJobsPrefix := fmt.Sprintf("%s:inflight", basePath)
	jobDataHash := fmt.Sprintf("%s:data", basePath)
	scheduledJobsSet := fmt.Sprintf("%s:scheduled", basePath)

	return &Queue{
		name: opts.Name,

		activeJobsList:     activeJobsList,
		consumersSet:       consumersSet,
		deadJobsSet:        deadJobsSet,
		inflightJobsPrefix: inflightJobsPrefix,
		jobDataHash:        jobDataHash,
		scheduledJobsSet:   scheduledJobsSet,
	}
}
