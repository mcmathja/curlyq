package curlyq

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis"
)

// Producers provide logic for pushing jobs onto a queue.
type Producer struct {
	opts *ProducerOpts

	queue *queue

	pushJobScript     *redis.Script
	scheduleJobScript *redis.Script
}

// ProducerOpts exposes options used when creating a new Producer.
type ProducerOpts struct {
	// Client is the go-redis instance used to communicate with Redis.
	Client *redis.Client
	// Queue specifies the name of the queue that this producer will push to.
	Queue string
}

// NewProducer instantiates a new Producer.
func NewProducer(opts *ProducerOpts) *Producer {
	// Required arguments
	if opts.Client == nil {
		panic("A redis client must be provided.")
	}

	if opts.Queue == "" {
		panic("A queue name must be provided.")
	}

	// Set up paths
	queue := newQueue(&queueOpts{
		Name: opts.Queue,
	})

	// Embed Lua scripts
	prepScripts()
	pushJobScript := loadLua("/lua/push_job.lua")
	scheduleJobScript := loadLua("/lua/schedule_job.lua")

	return &Producer{
		opts: opts,

		// Computed properties
		queue: queue,

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
// It returns error if the Redis script fails or it cannot marshal the job.
func (p *Producer) pushJob(ctx context.Context, job Job) (string, error) {
	msg, err := job.message()
	if err != nil {
		return "", err
	}

	client := p.opts.Client.WithContext(ctx)

	keys := []string{
		p.queue.jobDataHash,
		p.queue.activeJobsList,
	}

	args := []interface{}{
		job.ID,
		msg,
	}

	err = p.pushJobScript.Run(client, keys, args...).Err()
	if err != nil {
		return "", err
	}

	log.Printf("Enqueued job: %s", job.ID)
	return job.ID, nil
}

// scheduleJob inserts the job into the scheduled set.
// It returns error if the Redis script fails or it cannot marshal the job.
func (p *Producer) scheduleJob(ctx context.Context, at time.Time, job Job) (string, error) {
	msg, err := job.message()
	if err != nil {
		return "", err
	}

	client := p.opts.Client.WithContext(ctx)

	keys := []string{
		p.queue.jobDataHash,
		p.queue.scheduledJobsSet,
	}

	args := []interface{}{
		job.ID,
		msg,
		float64(at.Unix()),
	}

	err = p.scheduleJobScript.Run(client, keys, args...).Err()
	if err != nil {
		return "", err
	}

	log.Printf("Scheduled job: %s at %s", job.ID, at.Format(time.RFC1123Z))
	return job.ID, nil
}
