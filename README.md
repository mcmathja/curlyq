# CurlyQ

[![GoDoc](https://godoc.org/github.com/mcmathja/curlyq?status.svg)](https://godoc.org/github.com/mcmathja/curlyq)
[![Build Status](https://api.travis-ci.org/mcmathja/curlyq.svg?branch=master)](https://travis-ci.org/mcmathja/curlyq)
[![GoCover](https://gocover.io/_badge/github.com/mcmathja/curlyq)](https://gocover.io/github.com/mcmathja/curlyq)
[![Go Report Card](https://goreportcard.com/badge/github.com/mcmathja/curlyq)](https://goreportcard.com/report/github.com/mcmathja/curlyq)
[![License](https://img.shields.io/github/license/mcmathja/curlyq.svg)](https://github.com/mcmathja/curlyq/blob/master/LICENSE)

CurlyQ provides a simple, easy-to-use interface for performing background processing in Go. It supports scheduled jobs, job deduplication, and configurable concurrent execution out of the box.

## Quickstart
```go
package main

import (
	"context"
	"log"

	cq "github.com/mcmathja/curlyq"
)

func main() {
	// Create a new producer
	producer := cq.NewProducer(&cq.ProducerOpts{
		Address: "localhost:6379",
		Queue: "testq",
	})

	// Use the producer to push a job to the queue
	producer.Perform(cq.Job{
		Data: []byte("Some data!"),
	})

	// Create a new consumer
	consumer := cq.NewConsumer(&cq.ConsumerOpts{
		Address: "localhost:6379",
		Queue: "testq",
	})

	// Consume jobs from the queue with the consumer
	consumer.Consume(func(ctx context.Context, job cq.Job) error {
		log.Println(string(job.Data))
		return nil
	})
}
```

## The Basics

CurlyQ exposes three key types: Jobs, Producers, and Consumers.


### Jobs

A Job wraps your data. In most cases, that's all you'll ever need to know about it:

```go
job := cq.Job{
	Data: []byte("Some data."),
}
```

Every Job also exposes an `ID` field that uniquely identifies it among all jobs in the queue, and an `Attempt` field representing how many times it has been attempted so far.

### Producers

A Producer pushes jobs on to the queue. Create one by providing it with the address of your Redis instance and a queue name:

```go
producer := cq.NewProducer(&cq.ProducerOpts{
	Address: "my.redis.addr:6379",
	Queue: "queue_name",
})
```

You can also provide an existing [go-redis](https://github.com/go-redis/redis) instance if you would like to configure the queue to run on a more advanced Redis configuration or set up your own retry and timeout logic for network calls:

```go
import "github.com/go-redis/redis"

client := redis.NewClient(&redis.Client{
	Password: "p@55vvoRd",
	DB: 3,
	MaxRetries: 2,
})

producer := cq.NewProducer(&cq.ProducerOpts{
	Client: client,
	Queue: "queue_name",
})
```

Running `producer.Perform(job)` will add a job to the queue to be run asynchronously. You can also schedule a job to be enqueued at a particular time by running `producer.PerformAt(time, job)`, or after a certain wait period by running `producer.PerformAfter(duration, job)`. All of the `Perform` methods return the ID assigned to the job and an error if one occurred.

You can deduplicate jobs by pre-assigning them IDs:

```go
job := cq.Job{
	ID: "todays_job",
}

// Enqueue the job
producer.PerformAfter(10 * time.Second, job)

// Does nothing, because a job with the same ID is already on the queue
producer.Perform(job)
```

Once a job has been acknowledged, its ID becomes available for reuse.

See the documentation for [ProducerOpts](https://godoc.org/github.com/mcmathja/curlyq#ProducerOpts) for more details about available configuration options.

### Consumers

A Consumer pulls jobs off the queue and executes them using a provided handler function. Create one with the same basic options as a Producer:

```go
consumer := cq.NewConsumer(&cq.ConsumerOpts{
	Queue: "queue_name",

	// With an address:
	Address: "my.redis.addr:6379",
	// With a preconfigured go-redis client:
	Client: redisClient,
})
```

You start a consumer by running its `Consume` method with a handler function:

```go
consumer.Consume(func(ctx context.Context, job cq.Job) error {
	log.Println("Job %s has been processed!")
	return nil
})
```

If the provided handler function returns `nil`, the job is considered to have been processed successfully and is removed from the queue. If the handler returns an error or panics, the job is considered to have failed and will be retried or killed based on how many times it has been attempted.

`Consume` will continue to process jobs until your application receives an interrupt signal or the consumer encounters a fatal error. Fatal errors only occur when the consumer is unable to communicate with Redis for an essential operation, such as updating the status of a job in flight.

See the documentation for [ConsumerOpts](https://godoc.org/github.com/mcmathja/curlyq#ConsumerOpts) for more details about available configuration options.
