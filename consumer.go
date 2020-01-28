package curlyq

import (
	"context"
	"fmt"
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

// A Consumer executes jobs and manages the state of the queue.
type Consumer struct {
	opts *ConsumerOpts

	// Computed properties
	id          string
	client      *redis.Client
	queue       *queue
	inflightSet string
	processes   sync.WaitGroup
	errors      chan error

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
	// Address specifies the address of the Redis backing your queue.
	// CurlyQ will generate a go-redis instance based on this address.
	Address string
	// Client is a custom go-redis instance used to communicate with Redis.
	// If provided, this option overrides the value set in Address.
	Client *redis.Client
	// Logger provides a concrete implementation of the Logger interface.
	// If not provided, it will default to using the stdlib's log package.
	Logger Logger
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

	if opts.Logger == nil {
		opts.Logger = &DefaultLogger{}
	}

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
	if opts.Address == "" && opts.Client == nil {
		panic("A redis client must be provided.")
	}

	if opts.Queue == "" {
		panic("A queue must be provided.")
	}

	// Computed properties
	var client *redis.Client
	if opts.Client != nil {
		client = opts.Client
	} else {
		client = redis.NewClient(&redis.Options{
			Addr: opts.Address,
		})
	}

	id := uuid.Must(uuid.NewV4()).String()
	queue := newQueue(&queueOpts{
		Name: opts.Queue,
	})
	inflightSet := fmt.Sprintf("%s:%s", queue.inflightJobsPrefix, id)
	errors := make(chan error)

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
		client:      client,
		queue:       queue,
		inflightSet: inflightSet,
		errors:      errors,

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
func (c *Consumer) ConsumeCtx(ctx context.Context, handler HandlerFunc) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Fire off a synchronous heartbeat before polling for any jobs.
	// This ensures that cleanup works even if we fail during startup.
	err = c.registerConsumer(ctx)
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
	select {
	case <-ctx.Done():
	case err = <-c.errors:
		cancel()
	}

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

	return err
}

// Consume starts the consumer with a default context.
// The Consumer runs until the process receives one of the specified signals.
// An error is returned if the Consumer cannot shut down gracefully.
func (c *Consumer) Consume(handler HandlerFunc, signals ...os.Signal) error {
	if len(signals) == 0 {
		signals = []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
		}
	}

	// Start the consumer with a cancelable context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error)
	go func() {
		errChan <- c.ConsumeCtx(ctx, handler)
	}()

	// Wait until we receive a signal or an error.
	termChan := make(chan os.Signal)
	signal.Notify(termChan, signals...)
	select {
	case err := <-errChan:
		return err
	case <-termChan:
		cancel()
	}

	// Capture any errors that occur during shutdown.
	return <-errChan
}

// Processing Loops

// runExecutors starts two processing loops.
// The first handles executing queued jobs with the user-supplied handler function.
// The second handles polling Redis for new jobs and putting them into the queue.
func (c *Consumer) runExecutors(ctx context.Context, handler HandlerFunc) {
	defer c.processes.Done()
	c.opts.Logger.Debug("Executors: starting process")

	ticker := time.NewTicker(c.opts.ExecutorsPollInterval)
	queue := make(chan *Job, c.opts.ExecutorsBufferSize)

	// Set up a token bucket to limit concurrent active executors.
	tokens := make(chan struct{}, c.opts.ExecutorsConcurrency)
	for i := uint(0); i < c.opts.ExecutorsConcurrency; i++ {
		tokens <- struct{}{}
	}

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
			<-tokens
		}
		close(tokens)

		c.opts.Logger.Debug("Executors: process shut down")
	}()

	// Execution loop
	go func() {
		for {
			token, open := <-tokens
			if !open {
				return
			}

			select {
			case job, open := <-queue:
				if !open {
					tokens <- token
					return
				}

				// Execute the job concurrently.
				go func() {
					err := c.executeJob(ctx, job, handler)
					if err != nil {
						if job.Attempt < c.opts.ExecutorsMaxAttempts {
							_, err := c.retryJob(ctx, job)
							if err != nil {
								c.abort(ErrFailedToRetryJob{
									Job: *job,
									Err: err,
								})
							}
						} else {
							_, err := c.killJob(ctx, job)
							if err != nil {
								c.abort(ErrFailedToKillJob{
									Job: *job,
									Err: err,
								})
							}
						}
					} else {
						_, err := c.ackJob(ctx, job)
						if err != nil {
							c.abort(ErrFailedToAckJob{
								Job: *job,
								Err: err,
							})
						}
					}
					tokens <- token
				}()
			case <-ctx.Done():
				tokens <- token
				return
			}
		}
	}()

	// Polling loop
	for {
		hasMoreWork := false
		count := cap(queue) - len(queue)

		if count > 0 {
			c.opts.Logger.Debug("Executors: polling for new jobs...")
			jobs, err := c.getJobs(ctx, count)
			if err != nil {
				c.opts.Logger.Error("Executors: error retrieving jobs", "error", err)
			} else {
				c.opts.Logger.Debug("Executors: successfully polled", "job_count", len(jobs))
				for _, job := range jobs {
					queue <- job
				}

				hasMoreWork = len(jobs) == count
			}
		} else {
			c.opts.Logger.Debug("Executors: waiting while buffer is full...")
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
	c.opts.Logger.Debug("Heartbeat: process starting")
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.HeartbeatInterval)
	defer func() {
		ticker.Stop()
		c.opts.Logger.Debug("Heartbeat: process shut down")
	}()

	for {
		c.opts.Logger.Debug("Heartbeat: updating consumer...")
		err := c.registerConsumer(ctx)
		if err != nil {
			c.opts.Logger.Error("Heartbeat: failed to update", "error", err)
		} else {
			c.opts.Logger.Debug("Heartbeat: update successful")
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
	c.opts.Logger.Debug("Scheduler: starting process")
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.SchedulerPollInterval)
	defer func() {
		ticker.Stop()
		c.opts.Logger.Debug("Scheduler: process shut down")
	}()

	for {
		c.opts.Logger.Debug("Scheduler: enqueueing jobs...")

		hasMoreWork := false
		count, err := c.enqueueScheduledJobs(ctx)
		if err != nil {
			c.opts.Logger.Error("Scheduler: failed to enqueue jobs", "error", err)
		} else {
			c.opts.Logger.Debug("Scheduler: jobs enqueued successfully", "job_count", count)
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
	c.opts.Logger.Debug("Custodian: starting process")
	defer c.processes.Done()

	ticker := time.NewTicker(c.opts.CustodianPollInterval)
	defer func() {
		ticker.Stop()
		c.opts.Logger.Debug("Custodian: process shut down")
	}()

	for {
		c.opts.Logger.Debug("Custodian: re-enqueueing orphaned jobs...")

		hasMoreWork := false
		count, err := c.reenqueueOrphanedJobs(ctx)
		if err != nil {
			c.opts.Logger.Error("Custodian: failed to re-enqueue jobs", "error", err)
		} else {
			c.opts.Logger.Debug("Custodian: successfully re-enqueued jobs", "job_count", count)
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
	client := c.client.WithContext(ctx)

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
	client := c.client.WithContext(ctx)

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
	client := c.client.WithContext(ctx)

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

	client := c.client.WithContext(ctx)

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
	client := c.client.WithContext(ctx)

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
	client := c.client.WithContext(ctx)

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
	client := c.client.WithContext(ctx)

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

	client := c.client.WithContext(ctx)

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

func (c *Consumer) abort(err error) {
	select {
	case c.errors <- err:
	default:
	}
}
