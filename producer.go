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

	pushJobScript     *redis.Script
	scheduleJobScript *redis.Script
}

// ProducerOpts exposes options used when creating a new Producer.
type ProducerOpts struct {
	// Client is the go-redis instance used to communicate with Redis.
	Client *redis.Client
	// Queue contains details about where data is stored in Redis.
	Queue *Queue
}

// NewProducer instantiates a new Producer.
func NewProducer(opts *ProducerOpts) *Producer {
	// Required arguments
	if opts.Client == nil {
		panic("A redis client must be provided.")
	}

	if opts.Queue == nil {
		panic("A queue must be provided.")
	}

	// Embed Lua scripts
	prepScripts()
	pushJobScript := loadLua("/lua/push_job.lua")
	scheduleJobScript := loadLua("/lua/schedule_job.lua")

	return &Producer{
		opts: opts,

		pushJobScript:     pushJobScript,
		scheduleJobScript: scheduleJobScript,
	}
}

// Public API

// PerformAfter enqueues a job to be performed after a certain amount of time.
// It calls to Redis using a default background context.
func (p *Producer) PerformAfter(job *Job, duration time.Duration) error {
	return p.PerformAfterCtx(context.Background(), job, duration)
}

// PerformAfterCtx enqueues a job to be performed after a certain amount of time.
// It calls to Redis using a user-supplied context.
func (p *Producer) PerformAfterCtx(ctx context.Context, job *Job, duration time.Duration) error {
	return p.PerformAtCtx(ctx, job, time.Now().Add(duration))
}

// PerformAt calls PerformAtCtx with a default context.
// It calls to Redis using a default background context.
func (p *Producer) PerformAt(job *Job, at time.Time) error {
	return p.PerformAtCtx(context.Background(), job, at)
}

// PerformAtCtx schedules a job to be performed at a particular point in time.
// It calls to Redis using a user-supplied context.
func (p *Producer) PerformAtCtx(ctx context.Context, job *Job, at time.Time) error {
	return p.scheduleJob(ctx, job, at)
}

// PerformNow calls PerformNowCtx with a default context.
// It calls to Redis using a default background context.
func (p *Producer) PerformNow(job *Job) error {
	return p.PerformNowCtx(context.Background(), job)
}

// PerformNowCtx enqueues a job to be performed as soon as possible.
// It calls to Redis using a user-supplied context.
func (p *Producer) PerformNowCtx(ctx context.Context, job *Job) error {
	return p.pushJob(ctx, job)
}

// Redis operations

// pushJob pushes a job onto the active queue.
// It returns error if the Redis script fails or it cannot marshal the job.
func (p *Producer) pushJob(ctx context.Context, job *Job) error {
	msg, err := job.message()
	if err != nil {
		return err
	}

	client := p.opts.Client.WithContext(ctx)

	keys := []string{
		p.opts.Queue.jobDataHash,
		p.opts.Queue.activeJobsList,
	}

	args := []interface{}{
		job.ID,
		msg,
	}

	err = p.pushJobScript.Run(client, keys, args...).Err()
	if err != nil {
		return err
	}

	log.Printf("Enqueued job: %s", job.ID)
	return nil
}

// scheduleJob inserts the job into the scheduled set.
// It returns error if the Redis script fails or it cannot marshal the job.
func (p *Producer) scheduleJob(ctx context.Context, job *Job, at time.Time) error {
	msg, err := job.message()
	if err != nil {
		return err
	}

	client := p.opts.Client.WithContext(ctx)

	keys := []string{
		p.opts.Queue.jobDataHash,
		p.opts.Queue.scheduledJobsSet,
	}

	args := []interface{}{
		job.ID,
		msg,
		float64(at.Unix()),
	}

	err = p.scheduleJobScript.Run(client, keys, args...).Err()
	if err != nil {
		return err
	}

	log.Printf("Scheduled job: %s at %s", job.ID, at.Format(time.RFC1123Z))
	return nil
}
