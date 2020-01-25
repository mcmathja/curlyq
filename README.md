# CurlyQ

[![Build Status](https://api.travis-ci.org/mcmathja/curlyq.svg?branch=master)](https://travis-ci.org/mcmathja/curlyq)
[![GoDoc](https://godoc.org/github.com/mcmathja/curlyq?status.svg)](https://godoc.org/github.com/mcmathja/curlyq)
[![GoCover](http://gocover.io/_badge/github.com/mcmathja/curlyq)](http://gocover.io/github.com/mcmathja/curlyq)

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
