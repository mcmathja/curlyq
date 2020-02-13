package curlyq

import (
	"context"
	"time"

	"github.com/go-redis/redis"
)

// A Producer pushes jobs onto a queue.
type Producer struct {
	opts *ProducerOpts

	client *redis.Client
	queue  *queue

	pushJobScript     *redis.Script
	scheduleJobScript *redis.Script
}

// ProducerOpts exposes options used when creating a new Producer.
type ProducerOpts struct {
	// Address specifies the address of the Redis backing your queue.
	// CurlyQ will generate a go-redis instance based on this address.
	Address string
	// Client is a custom go-redis instance used to communicate with Redis.
	// If provided, this option overrides the value set in Address.
	Client *redis.Client
	// Logger provides a concrete implementation of the Logger interface.
	// If not provided, it will default to using the stdlib's log package.
	Logger Logger
	// Queue specifies the name of the queue that this producer will push to.
	Queue string
}

func (o *ProducerOpts) withDefaults() *ProducerOpts {
	opts := *o

	if opts.Logger == nil {
		opts.Logger = &DefaultLogger{}
	}

	return &opts
}

// NewProducer instantiates a new Producer.
func NewProducer(opts *ProducerOpts) *Producer {
	// Required arguments
	if opts.Address == "" && opts.Client == nil {
		panic("A redis client must be provided.")
	}

	if opts.Queue == "" {
		panic("A queue name must be provided.")
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

	// Embed Lua scripts
	prepScripts()
	pushJobScript := loadLua("/lua/push_job.lua")
	scheduleJobScript := loadLua("/lua/schedule_job.lua")

	return &Producer{
		opts: opts.withDefaults(),

		// Computed properties
		client: client,
		queue:  queue,

		// Scripts
		pushJobScript:     pushJobScript,
		scheduleJobScript: scheduleJobScript,
	}
}

// Public API

// PerformAfter enqueues a job to be performed after a certain amount of time.
// It calls to Redis using a default background context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) PerformAfter(duration time.Duration, job Job) (string, error) {
	return p.PerformAfterCtx(context.Background(), duration, job)
}

// PerformAfterCtx enqueues a job to be performed after a certain amount of time.
// It calls to Redis using a user-supplied context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) PerformAfterCtx(ctx context.Context, duration time.Duration, job Job) (string, error) {
	return p.PerformAtCtx(ctx, time.Now().Add(duration), job)
}

// PerformAt calls PerformAtCtx with a default context.
// It calls to Redis using a default background context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) PerformAt(at time.Time, job Job) (string, error) {
	return p.PerformAtCtx(context.Background(), at, job)
}

// PerformAtCtx schedules a job to be performed at a particular point in time.
// It calls to Redis using a user-supplied context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) PerformAtCtx(ctx context.Context, at time.Time, job Job) (string, error) {
	return p.scheduleJob(ctx, at, job)
}

// Perform calls PerformCtx with a default context.
// It calls to Redis using a default background context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) Perform(job Job) (string, error) {
	return p.PerformCtx(context.Background(), job)
}

// PerformCtx enqueues a job to be performed as soon as possible.
// It calls to Redis using a user-supplied context.
// It returns the ID of the enqueued job when successful or an error otherwise.
func (p *Producer) PerformCtx(ctx context.Context, job Job) (string, error) {
	return p.pushJob(ctx, job)
}

// Redis operations

// pushJob pushes a job onto the active queue.
// It returns an error if the Redis script fails, if it cannot marshal the job,
// or if a job with the provided ID already exists in Redis.
func (p *Producer) pushJob(ctx context.Context, job Job) (string, error) {
	msg, err := job.message()
	if err != nil {
		return "", err
	}

	client := p.client.WithContext(ctx)

	keys := []string{
		p.queue.jobDataHash,
		p.queue.activeJobsList,
		p.queue.signalList,
	}

	args := []interface{}{
		job.ID,
		msg,
	}

	enqueued, err := p.pushJobScript.Run(client, keys, args...).Int()
	if err != nil {
		return "", err
	}

	if enqueued == 0 {
		return "", ErrJobAlreadyExists{
			Job: job,
		}
	}

	p.opts.Logger.Info("Enqueued job", "job_id", job.ID)
	return job.ID, nil
}

// scheduleJob inserts the job into the scheduled set.
// It returns an error if the Redis script fails, if it cannot marshal the job,
// or if a job with the provided ID already exists in Redis.
func (p *Producer) scheduleJob(ctx context.Context, at time.Time, job Job) (string, error) {
	msg, err := job.message()
	if err != nil {
		return "", err
	}

	client := p.client.WithContext(ctx)

	keys := []string{
		p.queue.jobDataHash,
		p.queue.scheduledJobsSet,
	}

	args := []interface{}{
		job.ID,
		msg,
		float64(at.Unix()),
	}

	scheduled, err := p.scheduleJobScript.Run(client, keys, args...).Int()
	if err != nil {
		return "", err
	}

	if scheduled == 0 {
		return "", ErrJobAlreadyExists{
			Job: job,
		}
	}

	p.opts.Logger.Info("Scheduled job", "job_id", job.ID, "scheduled_at", at.Format(time.RFC1123Z))
	return job.ID, nil
}
