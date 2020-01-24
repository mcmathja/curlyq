package curlyq

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis"
	"github.com/gofrs/uuid"
)

// Consumers execute jobs and manage the state of the queue.
type Consumer struct {
	opts *ConsumerOpts

	// Computed properties
	id          string
	queue       *queue
	inflightSet string
	processes   sync.WaitGroup

	// Scripts
	ackJobScript                *redis.Script
	enqueueScheduledJobsScript  *redis.Script
	getJobsScript               *redis.Script
	killJobScript               *redis.Script
	reenqueueActiveJobsScript   *redis.Script
	reenqueueOrphanedJobsScript *redis.Script
	registerConsumerScript      *redis.Script
	retryJobScript              *redis.Script
}

// ConsumerOpts exposes options used when creating a new Consumer.
type ConsumerOpts struct {
	// Client is the go-redis instance used to communicate with Redis.
	Client *redis.Client
	// Queue specifies the name of the queue that this consumer will consume from.
	Queue string

	// How long to wait for executors to finish before exiting forcibly.
	// A zero value indicates that we should wait indefinitely.
	// Default: 0
	ShutdownGracePeriod time.Duration

	// How frequently the custodian should clean up jobs.
	// Default: 1 minute
	CustodianPollInterval time.Duration
	// Max number of jobs to clean up during a single check.
	// Default: 50
	CustodianMaxJobs uint
	// How long to wait after a missed heartbeat before a consumer is considered dead.
	// Default: 1 minute
	// Minimum: 5 seconds
	CustodianConsumerTimeout time.Duration

	// How many job executors to run simultaneously.
	// Default: 10
	ExecutorsConcurrency uint
	// How frequently we should poll for jobs.
	// Default: 3 seconds
	ExecutorsPollInterval time.Duration
	// How many jobs to buffer locally.
	// Default: Same as ExecutorsConcurrency
	ExecutorsBufferSize uint
	// The number of times to attempt a job before killing it.
	// Default: 5
	ExecutorsMaxAttempts uint

	// How frequently we should heartbeat.
	// Default: 1 minute
	// Minimum: 15 seconds
	HeartbeatInterval time.Duration

	// How frequently the scheduler should check for scheduled jobs.
	// Default: 15 seconds
	SchedulerPollInterval time.Duration
	// Max number of jobs to schedule during each check.
	// Default: 50
	SchedulerMaxJobs uint
}

// withDefaults returns a new ConsumerOpts with default values applied.
func (o *ConsumerOpts) withDefaults() *ConsumerOpts {
	opts := *o

	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 1 * time.Minute
	} else if opts.HeartbeatInterval < 15*time.Second {
		opts.HeartbeatInterval = 15 * time.Second
	}

	if opts.ExecutorsConcurrency <= 0 {
		opts.ExecutorsConcurrency = 10
	}

	if opts.ExecutorsPollInterval <= 0 {
		opts.ExecutorsPollInterval = 3 * time.Second
	}

	if opts.ExecutorsBufferSize <= 0 {
		opts.ExecutorsBufferSize = opts.ExecutorsConcurrency
	}

	if opts.ExecutorsMaxAttempts <= 0 {
		opts.ExecutorsMaxAttempts = 5
	}

	if opts.SchedulerPollInterval <= 0 {
		opts.SchedulerPollInterval = 15 * time.Second
	}

	if opts.SchedulerMaxJobs <= 0 {
		opts.SchedulerMaxJobs = 50
	}

	if opts.CustodianPollInterval <= 0 {
		opts.CustodianPollInterval = 1 * time.Minute
	}

	if opts.CustodianMaxJobs <= 0 {
		opts.CustodianMaxJobs = 50
	}

	if opts.CustodianConsumerTimeout <= 0 {
		opts.CustodianConsumerTimeout = 1 * time.Minute
	} else if opts.CustodianConsumerTimeout < 5*time.Second {
		opts.CustodianConsumerTimeout = 5 * time.Second
	}

	return &opts
}

// NewConsumer instantiates a new Consumer.
func NewConsumer(opts *ConsumerOpts) *Consumer {
	// Required arguments
	if opts.Client == nil {
		panic("A redis client must be provided.")
	}

	if opts.Queue == "" {
		panic("A queue must be provided.")
	}

	// Set up ID and paths
	id := uuid.Must(uuid.NewV4()).String()
	queue := newQueue(&queueOpts{
		Name: opts.Queue,
	})
	inflightSet := fmt.Sprintf("%s:%s", queue.inflightJobsPrefix, id)

	// Embed Lua scripts
	prepScripts()
	ackJobScript := loadLua("/lua/ack_job.lua")
	enqueueScheduledJobsScript := loadLua("/lua/enqueue_scheduled_jobs.lua")
	getJobsScript := loadLua("/lua/get_jobs.lua")
	killJobScript := loadLua("/lua/kill_job.lua")
	reenqueueActiveJobsScript := loadLua("/lua/reenqueue_active_jobs.lua")
	reenqueueOrphanedJobsScript := loadLua("/lua/reenqueue_orphaned_jobs.lua")
	registerConsumerScript := loadLua("/lua/register_consumer.lua")
	retryJobScript := loadLua("/lua/retry_job.lua")

	return &Consumer{
		opts: opts.withDefaults(),

		id:          id,
		queue:       queue,
		inflightSet: inflightSet,

		ackJobScript:                ackJobScript,
		enqueueScheduledJobsScript:  enqueueScheduledJobsScript,
		getJobsScript:               getJobsScript,
		killJobScript:               killJobScript,
		reenqueueActiveJobsScript:   reenqueueActiveJobsScript,
		reenqueueOrphanedJobsScript: reenqueueOrphanedJobsScript,
		registerConsumerScript:      registerConsumerScript,
		retryJobScript:              retryJobScript,
	}
}

// HandlerFunc is a convenience alias.
// It represents a function used to process a job.
type HandlerFunc func(context.Context, Job) error

// Public API

// ConsumeCtx starts the consumer with a user-supplied context.
// The Consumer runs indefinitely until the provided context is canceled.
// An error is returned if the Consumer cannot shut down gracefully.
func (c *Consumer) ConsumeCtx(ctx context.Context, handler HandlerFunc) error {
	// Fire off a synchronous heartbeat before polling for any jobs.
	// This ensures that cleanup works even if we fail during startup.
	err := c.registerConsumer(ctx)
	if err != nil {
		return err
	}

	// Spin up the child processes.
	c.processes.Add(4)
	go c.runHeartbeat(ctx)
	go c.runScheduler(ctx)
	go c.runCustodian(ctx)
	go c.runExecutors(ctx, handler)

	// Block until the provided context is done.
	<-ctx.Done()

	// Wait for the child processes to finish.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.processes.Wait()
	}()

	if c.opts.ShutdownGracePeriod <= 0 {
		// Wait forever for the processes to complete their current tasks.
		<-done
	} else {
		// Wait up until ShutdownGracePeriod for the processes to complete.
		select {
		case <-done:
			break
		case <-time.After(c.opts.ShutdownGracePeriod):
			return fmt.Errorf("Failed to shut down within ShutdownGracePeriod.")
		}
	}

	log.Println("Consumer shut down successfully.")
	return nil
}

// Consumer starts the consumer with a default context.
// The Consumer runs until the process receives one of the specified signals.
// An error is returned if the Consumer cannot shut down gracefully.
func (c *Consumer) Consume(handler HandlerFunc, signals ...os.Signal) error {
	if len(signals) == 0 {
		signals = []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
		}
	}

	// Start the consumer with a cancellable context.
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	go func() {
		errChan <- c.ConsumeCtx(ctx, handler)
	}()

	// Wait until we receive a signal and then cancel the context.
	termChan := make(chan os.Signal)
	signal.Notify(termChan, signals...)
	<-termChan
	cancel()

	// Return the result of the Execute call.
	return <-errChan
}

// Processing Loops

// runExecutors starts two processing loops.
// The first handles executing queued jobs with the user-supplied handler function.
// The second handles polling Redis for new jobs and putting them into the queue.
func (c *Consumer) runExecutors(ctx context.Context, handler HandlerFunc) {
	defer c.processes.Done()
	log.Println("Starting executors.")

	ticker := time.NewTicker(c.opts.ExecutorsPollInterval)
	executors := make(chan struct{}, c.opts.ExecutorsConcurrency)
	queue := make(chan *Job, c.opts.ExecutorsBufferSize)

	// Clean up
	defer func() {
		// Stop polling for jobs.
		ticker.Stop()

		// Drain the queue of any buffered jobs that we haven't started work on.
		close(queue)
		jobs := []*Job{}
		for job := range queue {
			jobs = append(jobs, job)
		}

		if len(jobs) > 0 {
			c.reenqueueActiveJobs(ctx, jobs)
		}

		// Wait for existing executors to finish running jobs.
		for i := uint(0); i < c.opts.ExecutorsConcurrency; i++ {
			executors <- struct{}{}
		}
		close(executors)
	}()

	// Execution loop
	go func() {
		defer func() {
			// We've broken out of the loop.
			<-executors
		}()

		for {
			executors <- struct{}{}
			select {
			case job, open := <-queue:
				if !open {
					return
				}

				// Execute the job concurrently.
				go func() {
					err := c.executeJob(ctx, job, handler)
					if err != nil {
						if job.Attempt < c.opts.ExecutorsMaxAttempts {
							c.retryJob(ctx, job)
						} else {
							c.killJob(ctx, job)
						}
					} else {
						c.ackJob(ctx, job)
					}
					<-executors
				}()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Polling loop
	for {
		hasMoreWork := false
		count := cap(queue) - len(queue)

		if count > 0 {
			log.Println("Polling for jobs...")
			jobs, err := c.getJobs(ctx, count)
			if err != nil {
				log.Println("Error retrieving jobs: ", err)
			} else {
				log.Println("Finished polling. Retrieved", len(jobs), "job(s).")
				for _, job := range jobs {
					queue <- job
				}

				hasMoreWork = len(jobs) == count
			}
		} else {
			log.Println("Waiting while buffer is full...")
		}

		select {
		case <-ctx.Done():
			return
		default:
			if hasMoreWork {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

// runHeartbeat starts a processing loop that periodically
// registers this consumer's presence to other consumers.
func (c *Consumer) runHeartbeat(ctx context.Context) {
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.HeartbeatInterval)
	defer func() {
		ticker.Stop()
		log.Println("Heartbeat process shut down.")
	}()

	for {
		log.Println("Updating heartbeat...")
		err := c.registerConsumer(ctx)
		if err != nil {
			log.Println("Failed to update heartbeat:", err)
		} else {
			log.Println("Heartbeat updated successfully.")
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

// runScheduler starts a processing loop that handles
// moving scheduled jobs to the active queue.
func (c *Consumer) runScheduler(ctx context.Context) {
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.SchedulerPollInterval)
	defer func() {
		ticker.Stop()
		log.Println("Scheduler process shut down.")
	}()

	for {
		log.Println("Scheduling jobs...")

		hasMoreWork := false
		count, err := c.enqueueScheduledJobs(ctx)
		if err != nil {
			log.Println("Scheduler failed to run:", err)
		} else {
			log.Println("Scheduler completed successfully. Scheduled", count, "job(s).")
			hasMoreWork = uint(count) == c.opts.SchedulerMaxJobs
		}

		select {
		case <-ctx.Done():
			return
		default:
			if hasMoreWork {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

// runCustodian starts a processing loop that handles
// cleaning up orphaned jobs from dead consumers.
func (c *Consumer) runCustodian(ctx context.Context) {
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.CustodianPollInterval)
	defer func() {
		ticker.Stop()
		log.Println("Custodian process shut down.")
	}()

	for {
		log.Println("Cleaning up orphaned jobs...")

		hasMoreWork := false
		count, err := c.reenqueueOrphanedJobs(ctx)
		if err != nil {
			log.Println("Custodian failed to run:", err)
		} else {
			log.Println("Custodian completed successfully. Cleaned up", count, "job(s).")
			hasMoreWork = uint(count) == c.opts.CustodianMaxJobs
		}

		select {
		case <-ctx.Done():
			return
		default:
			if hasMoreWork {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

// Redis operations

// ackJob acknowledges that a job has been successfully completed.
// It returns a boolean indicating if the job was successfully acked.
// It returns an error if the Redis script fails.
func (c *Consumer) ackJob(ctx context.Context, job *Job) (bool, error) {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.inflightSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		job.ID,
	}

	return c.ackJobScript.Run(client, keys, args...).Bool()
}

// enqueueScheduledJobs enqueues jobs from the scheduled set that are ready to be run.
// It returns the number of jobs that were scheduled.
// It returns an error if the Redis script fails.
func (c *Consumer) enqueueScheduledJobs(ctx context.Context) (int, error) {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.queue.scheduledJobsSet,
		c.queue.activeJobsList,
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		c.opts.SchedulerMaxJobs,
	}

	return c.enqueueScheduledJobsScript.Run(client, keys, args...).Int()
}

// getJobs polls Redis for jobs that are ready to be processed.
// It returns a slice of Jobs for this consumer to process on success.
// It returns an error if the Redis script fails or it cannot parse the job data.
func (c *Consumer) getJobs(ctx context.Context, count int) ([]*Job, error) {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.queue.activeJobsList,
		c.inflightSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		count,
	}

	result, err := c.getJobsScript.Run(client, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	messages := result.([]interface{})
	jobs := make([]*Job, len(messages))
	for idx, message := range messages {
		job := &Job{}
		err := job.fromMessage([]byte(message.(string)))
		if err != nil {
			return nil, err
		}

		jobs[idx] = job
	}

	return jobs, nil
}

// killJob marks a job as permanently failed by moving it to the dead set.
// It returns a boolean indicating if the job was successfully killed.
// It returns an error if the Redis script fails or it cannot marshal the job.
func (c *Consumer) killJob(ctx context.Context, job *Job) (bool, error) {
	diedAt := float64(time.Now().Unix())
	job.Attempt = job.Attempt + 1
	msg, err := job.message()
	if err != nil {
		return false, err
	}

	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.inflightSet,
		c.queue.deadJobsSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		job.ID,
		diedAt,
		msg,
	}

	return c.killJobScript.Run(client, keys, args...).Bool()
}

// reenqueueActiveJobs reschedules jobs that are still unstarted at shutdown.
// It returns an error if the Redis script fails.
func (c *Consumer) reenqueueActiveJobs(ctx context.Context, jobs []*Job) error {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.inflightSet,
		c.queue.activeJobsList,
	}

	args := make([]interface{}, len(jobs))
	for idx, job := range jobs {
		args[idx] = job.ID
	}

	return c.reenqueueActiveJobsScript.Run(client, keys, args...).Err()
}

// rescheduleOrphanedJobs reschedules jobs orphaned by timed-out consumers.
// It returns the total number of jobs that were rescheduled.
// It returns an error if the Redis script fails.
func (c *Consumer) reenqueueOrphanedJobs(ctx context.Context) (int, error) {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.queue.consumersSet,
		c.queue.activeJobsList,
	}

	expiredBefore := time.Now().Add(-c.opts.HeartbeatInterval).Add(-c.opts.CustodianConsumerTimeout)
	args := []interface{}{
		float64(expiredBefore.Unix()),
		c.opts.CustodianMaxJobs,
	}

	return c.reenqueueOrphanedJobsScript.Run(client, keys, args...).Int()
}

// registerConsumer marks a consumer as active and visible to other consumers.
// It returns an error if the Redis script fails.
func (c *Consumer) registerConsumer(ctx context.Context) error {
	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.queue.consumersSet,
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		c.inflightSet,
	}

	return c.registerConsumerScript.Run(client, keys, args...).Err()
}

// retryJob reschedules a job that errored during execution.
// It returns an error if the Redis script fails or it cannot marshal the job.
func (c *Consumer) retryJob(ctx context.Context, job *Job) (bool, error) {
	jitter := rand.Float64()
	backoff := math.Pow(2, float64(job.Attempt)) + jitter
	retryAt := float64(time.Now().Add(time.Duration(backoff) * time.Second).Unix())
	job.Attempt = job.Attempt + 1
	msg, err := job.message()
	if err != nil {
		return false, err
	}

	client := c.opts.Client.WithContext(ctx)

	keys := []string{
		c.inflightSet,
		c.queue.scheduledJobsSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		job.ID,
		retryAt,
		msg,
	}

	return c.retryJobScript.Run(client, keys, args...).Bool()
}

// Helpers

// executeJob runs a job with the user provided handler function.
// It returns an error if the handler returns an error or panics.
func (c *Consumer) executeJob(ctx context.Context, job *Job, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Job %s panicked during execution: %v", job.ID, r)
		}
	}()

	return handler(ctx, *job)
}
