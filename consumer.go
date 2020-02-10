package curlyq

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/gofrs/uuid"
)

// A Consumer executes jobs and manages the state of the queue.
type Consumer struct {
	opts *ConsumerOpts

	// Computed properties
	client      *redis.Client
	errors      chan error
	id          string
	inflightSet string
	queue       *queue
	onAbort     *sync.Once
	processes   *sync.WaitGroup

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
	// Queue specifies the name of the queue that this consumer will consume from.
	Queue string

	// Logger provides a concrete implementation of the Logger interface.
	// If not provided, it will default to using the stdlib's log package.
	Logger Logger
	// The maximum number of times to retry a job before killing it.
	// Default: 20
	JobMaxAttempts int
	// The maximum delay between retry attempts.
	// Default: 1 week
	JobMaxBackoff time.Duration
	// How long to wait for executors to finish before exiting forcibly.
	// A zero value indicates that we should wait indefinitely.
	// Default: 0
	ShutdownGracePeriod time.Duration

	// How long to wait after a missed heartbeat before a consumer is considered dead.
	// Default: 1 minute
	// Minimum: 5 seconds
	CustodianConsumerTimeout time.Duration
	// The maximum number of failed attempts before aborting.
	// A zero value indiciates the custodian should never abort.
	// Default: 0
	CustodianMaxAttempts int
	// The longest amount of time to wait between failed attempts.
	// Default: 30 seconds
	CustodianMaxBackoff time.Duration
	// Max number of jobs to clean up during a single check.
	// Default: 50
	CustodianMaxJobs int
	// How frequently the custodian should clean up jobs.
	// Default: 1 minute
	CustodianPollInterval time.Duration

	// The maximum number of failed attempts before aborting.
	// A zero value indiciates the hearbeart should never abort.
	// Default: 0
	HeartbeatMaxAttempts int
	// The longest amount of time to wait between failed attempts.
	// Default: 30 seconds
	HeartbeatMaxBackoff time.Duration
	// How frequently we should heartbeat.
	// Default: 1 minute
	// Minimum: 15 seconds
	HeartbeatPollInterval time.Duration

	// How many jobs to buffer locally.
	// Default: 10
	PollerBufferSize int
	// The maximum number of failed attempts before aborting.
	// A zero value indiciates the poller should never abort.
	// Default: 0
	PollerMaxAttempts int
	// The longest amount of time to wait between failed attempts.
	// Default: 30 seconds
	PollerMaxBackoff time.Duration
	// How long we should block on Redis for new jobs on each call.
	// Default: 5 seconds
	// Minimum: 1 second
	PollerPollDuration time.Duration

	// How many jobs to process simultaneously.
	// Default: 5
	ProcessorConcurrency int

	// The maximum number of failed attempts before aborting.
	// A zero value indiciates the scheduler should never abort.
	// Default: 0
	SchedulerMaxAttempts int
	// The longest amount of time to wait between failed attempts.
	// Default: 30 seconds
	SchedulerMaxBackoff time.Duration
	// Max number of jobs to schedule during each check.
	// Default: 50
	SchedulerMaxJobs int
	// How frequently the scheduler should check for scheduled jobs.
	// Default: 5 seconds
	SchedulerPollInterval time.Duration
}

// withDefaults returns a new ConsumerOpts with default values applied.
func (o *ConsumerOpts) withDefaults() *ConsumerOpts {
	opts := *o

	if opts.Logger == nil {
		opts.Logger = &DefaultLogger{}
	}

	if opts.JobMaxAttempts <= 0 {
		opts.JobMaxAttempts = 20
	}

	if opts.JobMaxBackoff <= 0 {
		opts.JobMaxBackoff = 168 * time.Hour
	}

	if opts.CustodianConsumerTimeout <= 0 {
		opts.CustodianConsumerTimeout = 1 * time.Minute
	} else if opts.CustodianConsumerTimeout < 5*time.Second {
		opts.CustodianConsumerTimeout = 5 * time.Second
	}

	if opts.CustodianMaxBackoff <= 0 {
		opts.CustodianMaxBackoff = 30 * time.Second
	}

	if opts.CustodianMaxJobs <= 0 {
		opts.CustodianMaxJobs = 50
	}

	if opts.CustodianPollInterval <= 0 {
		opts.CustodianPollInterval = 1 * time.Minute
	}

	if opts.HeartbeatMaxBackoff <= 0 {
		opts.HeartbeatMaxBackoff = 30 * time.Second
	}

	if opts.HeartbeatPollInterval <= 0 {
		opts.HeartbeatPollInterval = 1 * time.Minute
	} else if opts.HeartbeatPollInterval < 15*time.Second {
		opts.HeartbeatPollInterval = 15 * time.Second
	}

	if opts.PollerBufferSize <= 0 {
		opts.PollerBufferSize = 10
	}

	if opts.PollerMaxBackoff <= 0 {
		opts.PollerMaxBackoff = 30 * time.Second
	}

	if opts.PollerPollDuration <= 0 {
		opts.PollerPollDuration = 5 * time.Second
	} else if opts.PollerPollDuration < 1*time.Second {
		opts.PollerPollDuration = 1 * time.Second
	}

	if opts.ProcessorConcurrency <= 0 {
		opts.ProcessorConcurrency = 5
	}

	if opts.SchedulerPollInterval <= 0 {
		opts.SchedulerPollInterval = 5 * time.Second
	}

	if opts.SchedulerMaxBackoff <= 0 {
		opts.SchedulerMaxBackoff = 30 * time.Second
	}

	if opts.SchedulerMaxJobs <= 0 {
		opts.SchedulerMaxJobs = 50
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

	queue := newQueue(&queueOpts{
		Name: opts.Queue,
	})

	errors := make(chan error)
	id := uuid.Must(uuid.NewV4()).String()
	inflightSet := fmt.Sprintf("%s:%s", queue.inflightJobsPrefix, id)
	onAbort := &sync.Once{}
	processes := &sync.WaitGroup{}

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

		client: client,
		queue:  queue,

		errors:      errors,
		id:          id,
		inflightSet: inflightSet,
		onAbort:     onAbort,
		processes:   processes,

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
	c.opts.Logger.Info("Consumer started polling", "id", c.id, "queue", c.queue.name)
	defer c.opts.Logger.Info("Consumer finished polling", "id", c.id, "queue", c.queue.name)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Configure the Redis client with the provided context.
	c.client = c.client.WithContext(ctx)

	// Control mechanisms for managing local job buffer.
	buffer := make(chan *Job, c.opts.PollerBufferSize)
	bufferFree := sync.NewCond(&sync.Mutex{})
	go func() {
		<-ctx.Done()
		bufferFree.Broadcast()
	}()

	// Fire off a synchronous heartbeat before polling for any jobs.
	// This ensures that cleanup works even if we fail during startup.
	err = c.registerConsumer()
	if err != nil {
		return err
	}

	// Spin up the child processes.
	c.processes.Add(5)
	go c.runHeartbeat(ctx)
	go c.runScheduler(ctx)
	go c.runCustodian(ctx)
	go c.runProcessor(ctx, buffer, bufferFree, handler)
	go c.runPoller(ctx, buffer, bufferFree)

	// Block until the provided context is done.
	select {
	case <-ctx.Done():
	case err = <-c.errors:
		cancel()
	}

	// Wait for the child processes to finish.
	c.opts.Logger.Info("Consumer shutting down", "id", c.id, "queue", c.queue.name)
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
			return fmt.Errorf("failed to shut down within ShutdownGracePeriod")
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
			os.Interrupt,
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

// runCustodian starts a processing loop that handles
// cleaning up orphaned jobs from dead consumers.
func (c *Consumer) runCustodian(ctx context.Context) {
	defer c.processes.Done()

	c.opts.Logger.Debug("Custodian: process starting")
	defer c.opts.Logger.Debug("Custodian: process finished")

	ticker := time.NewTicker(c.opts.CustodianPollInterval)
	defer ticker.Stop()

	attempts := 0
	for {
		c.opts.Logger.Debug("Custodian: re-enqueueing orphaned jobs...")

		if ctx.Err() != nil {
			return
		}

		count, err := c.reenqueueOrphanedJobs()
		if err != nil {
			attempts++
			c.opts.Logger.Warn("Custodian: failed to re-enqueue jobs", "attempt", attempts, "error", err)
			c.backoff(ctx, "Custodian", attempts, c.opts.CustodianMaxBackoff, c.opts.CustodianMaxAttempts)
			continue
		} else {
			attempts = 0
			c.opts.Logger.Debug("Custodian: successfully re-enqueued jobs", "job_count", count)
			if count == c.opts.CustodianMaxJobs {
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

	c.opts.Logger.Debug("Heartbeat: process starting")
	defer c.opts.Logger.Debug("Heartbeat: process finished")

	ticker := time.NewTicker(c.opts.HeartbeatPollInterval)
	defer ticker.Stop()

	attempts := 0
	for {
		if ctx.Err() != nil {
			return
		}

		c.opts.Logger.Debug("Heartbeat: updating consumer...")
		err := c.registerConsumer()
		if err != nil {
			attempts++
			c.opts.Logger.Warn("Heartbeat: failed to update", "attempt", attempts, "error", err)
			c.backoff(ctx, "Heartbeat", attempts, c.opts.HeartbeatMaxBackoff, c.opts.HeartbeatMaxAttempts)
			continue
		} else {
			attempts = 0
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

// runPoller starts a processing loop that handles
// polling Redis for new jobs and buffering them locally.
func (c *Consumer) runPoller(ctx context.Context, buffer chan *Job, bufferFree *sync.Cond) {
	defer c.processes.Done()

	c.opts.Logger.Debug("Poller: process started")
	defer c.opts.Logger.Debug("Poller: process finished")

	defer func() {
		close(buffer)
		jobs := []*Job{}
		for job := range buffer {
			jobs = append(jobs, job)
		}

		if len(jobs) > 0 {
			c.reenqueueActiveJobs(jobs)
		}
	}()

	pollAttempts := 0
	retrieveAttempts := 0
	for {
		bufferFree.L.Lock()
		for len(buffer) >= cap(buffer) && ctx.Err() == nil {
			bufferFree.Wait()
		}
		bufferFree.L.Unlock()

		if ctx.Err() != nil {
			return
		}

		c.opts.Logger.Debug("Poller: polling for new jobs...")
		err := c.pollActiveJobs()
		if err == redis.Nil {
			pollAttempts = 0
			c.opts.Logger.Debug("Poller: no new jobs detected")
			continue
		} else if err != nil {
			pollAttempts++
			c.opts.Logger.Warn("Poller: error polling jobs", "attempt", pollAttempts, "error", err)
			c.backoff(ctx, "Poller", pollAttempts, c.opts.PollerMaxBackoff, c.opts.PollerMaxAttempts)
			continue
		} else {
			pollAttempts = 0
			c.opts.Logger.Debug("Poller: detected new jobs")
		}

		count := cap(buffer) - len(buffer)
		c.opts.Logger.Debug("Poller: retrieving jobs...", "job_count", count)
		jobs, err := c.getJobs(count)
		if err != nil {
			retrieveAttempts++
			c.opts.Logger.Warn("Poller: error retrieving jobs", "attempt", retrieveAttempts, "error", err)
			c.backoff(ctx, "Poller", retrieveAttempts, c.opts.PollerMaxBackoff, c.opts.PollerMaxAttempts)
		} else {
			retrieveAttempts = 0
			c.opts.Logger.Debug("Poller: successfully retrieved jobs", "job_count", len(jobs))
			for _, job := range jobs {
				buffer <- job
			}
		}
	}
}

// runProcessor starts a processing loop that handles
// processing buffered jobs with the user-supplied handler function.
func (c *Consumer) runProcessor(ctx context.Context, buffer chan *Job, bufferFree *sync.Cond, handler HandlerFunc) {
	defer c.processes.Done()

	c.opts.Logger.Debug("Processor: process started")
	defer c.opts.Logger.Debug("Processor: process finished")

	// A token bucket to limit concurrent active executors.
	tokens := make(chan struct{}, c.opts.ProcessorConcurrency)
	for i := 0; i < c.opts.ProcessorConcurrency; i++ {
		tokens <- struct{}{}
	}

	defer func() {
		for i := 0; i < c.opts.ProcessorConcurrency; i++ {
			<-tokens
		}
		close(tokens)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tokens:
		}

		select {
		case <-ctx.Done():
			tokens <- struct{}{}
			return
		case job, open := <-buffer:
			if !open {
				tokens <- struct{}{}
				return
			}
			bufferFree.L.Lock()
			bufferFree.Broadcast()
			bufferFree.L.Unlock()

			// Execute the job concurrently.
			go func() {
				c.opts.Logger.Debug("Processing job", "id", job.ID)
				err := c.executeJob(ctx, job, handler)
				if err != nil {
					c.opts.Logger.Debug("Job failed", "id", job.ID)
					if job.Attempt < c.opts.JobMaxAttempts {
						c.opts.Logger.Debug("Retrying job", "id", job.ID, "retries", job.Attempt)
						_, err := c.retryJob(job)
						if err != nil {
							c.abort(ErrFailedToRetryJob{
								Job: *job,
								Err: err,
							})
						}
					} else {
						c.opts.Logger.Debug("Killing job", "id", job.ID, "retries", job.Attempt)
						_, err := c.killJob(job)
						if err != nil {
							c.abort(ErrFailedToKillJob{
								Job: *job,
								Err: err,
							})
						}
					}
				} else {
					c.opts.Logger.Debug("Job successful", "id", job.ID)
					_, err := c.ackJob(job)
					if err != nil {
						c.abort(ErrFailedToAckJob{
							Job: *job,
							Err: err,
						})
					}
				}
				tokens <- struct{}{}
			}()
		}
	}
}

// runScheduler starts a processing loop that handles
// moving scheduled jobs to the active queue.
func (c *Consumer) runScheduler(ctx context.Context) {
	defer c.processes.Done()

	c.opts.Logger.Debug("Scheduler: process starting")
	defer c.opts.Logger.Debug("Scheduler: process finished")

	ticker := time.NewTicker(c.opts.SchedulerPollInterval)
	defer ticker.Stop()

	attempts := 0
	for {
		c.opts.Logger.Debug("Scheduler: enqueueing jobs...")

		if ctx.Err() != nil {
			return
		}

		count, err := c.enqueueScheduledJobs()
		if err != nil {
			attempts++
			c.opts.Logger.Warn("Scheduler: failed to enqueue jobs", "attempt", attempts, "error", err)
			c.backoff(ctx, "Scheduler", attempts, c.opts.SchedulerMaxBackoff, c.opts.SchedulerMaxAttempts)
			continue
		} else {
			c.opts.Logger.Debug("Scheduler: jobs enqueued successfully", "job_count", count)
			attempts = 0
			if count == c.opts.SchedulerMaxJobs {
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
func (c *Consumer) ackJob(job *Job) (bool, error) {
	keys := []string{
		c.inflightSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		job.ID,
	}

	return c.ackJobScript.Run(c.client, keys, args...).Bool()
}

// enqueueScheduledJobs enqueues jobs from the scheduled set that are ready to be run.
// It returns the number of jobs that were scheduled.
// It returns an error if the Redis script fails.
func (c *Consumer) enqueueScheduledJobs() (int, error) {
	keys := []string{
		c.queue.scheduledJobsSet,
		c.queue.activeJobsList,
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		c.opts.SchedulerMaxJobs,
	}

	return c.enqueueScheduledJobsScript.Run(c.client, keys, args...).Int()
}

// getJobs polls Redis for jobs that are ready to be processed.
// It returns a slice of Jobs for this consumer to process on success.
// It returns an error if the Redis script fails or it cannot parse the job data.
func (c *Consumer) getJobs(count int) ([]*Job, error) {
	keys := []string{
		c.queue.consumersSet,
		c.queue.activeJobsList,
		c.inflightSet,
		c.queue.jobDataHash,
	}

	args := []interface{}{
		count,
		c.inflightSet,
	}

	result, err := c.getJobsScript.Run(c.client, keys, args...).Result()
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
func (c *Consumer) killJob(job *Job) (bool, error) {
	diedAt := float64(time.Now().Unix())
	job.Attempt = job.Attempt + 1
	msg, err := job.message()
	if err != nil {
		return false, err
	}

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

	return c.killJobScript.Run(c.client, keys, args...).Bool()
}

// pollActiveJobs blocks until jobs are in the queue in Redis.
// It returns an error if it times out or the context is canceled.
func (c *Consumer) pollActiveJobs() error {
	return c.client.BRPopLPush(
		c.queue.activeJobsList,
		c.queue.activeJobsList,
		c.opts.PollerPollDuration,
	).Err()
}

// reenqueueActiveJobs reschedules jobs that are still unstarted at shutdown.
// It returns an error if the Redis script fails.
func (c *Consumer) reenqueueActiveJobs(jobs []*Job) error {
	keys := []string{
		c.inflightSet,
		c.queue.activeJobsList,
	}

	args := make([]interface{}, len(jobs))
	for idx, job := range jobs {
		args[idx] = job.ID
	}

	return c.reenqueueActiveJobsScript.Run(c.client, keys, args...).Err()
}

// rescheduleOrphanedJobs reschedules jobs orphaned by timed-out consumers.
// It returns the total number of jobs that were rescheduled.
// It returns an error if the Redis script fails.
func (c *Consumer) reenqueueOrphanedJobs() (int, error) {
	keys := []string{
		c.queue.consumersSet,
		c.queue.activeJobsList,
	}

	expiredBefore := time.Now().Add(-c.opts.HeartbeatPollInterval).Add(-c.opts.CustodianConsumerTimeout)
	args := []interface{}{
		float64(expiredBefore.Unix()),
		c.opts.CustodianMaxJobs,
	}

	return c.reenqueueOrphanedJobsScript.Run(c.client, keys, args...).Int()
}

// registerConsumer marks a consumer as active and visible to other consumers.
// It returns an error if the Redis script fails.
func (c *Consumer) registerConsumer() error {
	keys := []string{
		c.queue.consumersSet,
	}

	args := []interface{}{
		float64(time.Now().Unix()),
		c.inflightSet,
	}

	return c.registerConsumerScript.Run(c.client, keys, args...).Err()
}

// retryJob reschedules a job that errored during execution.
// It returns an error if the Redis script fails or it cannot marshal the job.
func (c *Consumer) retryJob(job *Job) (bool, error) {
	backoff := expBackoff(job.Attempt, c.opts.JobMaxBackoff)
	retryAt := float64(time.Now().Add(time.Duration(backoff)).Unix())
	job.Attempt = job.Attempt + 1
	msg, err := job.message()
	if err != nil {
		return false, err
	}

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

	return c.retryJobScript.Run(c.client, keys, args...).Bool()
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

// abort notifies the consumer of a fatal error.
// This starts the shut down process.
func (c *Consumer) abort(err error) {
	c.onAbort.Do(func() {
		c.opts.Logger.Error("Critical error triggered abort", "error", err)
		c.errors <- err
	})
}

// backoff is used to delay a processing loop that has encountered an error.
// It calculates a backoff and sleeps for that amount of time.
func (c *Consumer) backoff(ctx context.Context, process string, attempt int, maxDelay time.Duration, maxAttempts int) {
	if maxAttempts > 0 && attempt >= maxAttempts {
		c.abort(ErrExceededMaxBackoff{
			Attempt: attempt,
			Process: process,
		})
	}

	delay := expBackoff(attempt, maxDelay)
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
	case <-timer.C:
	}

	return
}

// expBackoff implements a simple exponential backoff function.
func expBackoff(attempt int, max time.Duration) time.Duration {
	base := math.Pow(2, float64(attempt))
	jittered := (1 + rand.Float64()) * (base / 2)
	scaled := jittered * float64(time.Second)
	capped := math.Min(scaled, float64(max))
	return time.Duration(capped)
}
