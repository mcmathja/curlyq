package curlyq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"github.com/gofrs/uuid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Consumer", func() {
	var server *miniredis.Miniredis
	var client *redis.Client
	var queue string
	var consumer *Consumer

	createJobs := func(count int, attempt int) []*Job {
		jobs := make([]*Job, count)
		for idx := 0; idx < count; idx++ {
			uid, err := uuid.NewV4()
			Expect(err).NotTo(HaveOccurred())

			id := uid.String()
			data := []byte(id)
			jobs[idx] = &Job{
				ID:      id,
				Attempt: attempt,
				Data:    data,
			}
		}

		return jobs
	}

	extractIds := func(jobs []*Job) []string {
		ids := make([]string, len(jobs))
		for idx, job := range jobs {
			ids[idx] = job.ID
		}
		return ids
	}

	enqueueJobs := func(jobs []*Job) {
		var err error
		hashData := map[string]interface{}{}
		jobIDs := make([]interface{}, len(jobs))
		for idx, job := range jobs {
			msg, err := job.message()
			Expect(err).NotTo(HaveOccurred())

			jobIDs[idx] = job.ID
			hashData[job.ID] = msg
		}

		err = client.HMSet(consumer.queue.jobDataHash, hashData).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.RPush(consumer.queue.activeJobsList, jobIDs...).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.Del(consumer.queue.signalList).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.LPush(consumer.queue.signalList, 1).Err()
		Expect(err).NotTo(HaveOccurred())
	}

	scheduleJobs := func(jobs []*Job, at time.Time) {
		var err error
		hashData := map[string]interface{}{}
		jobEntries := make([]redis.Z, len(jobs))
		for idx, job := range jobs {
			msg, err := job.message()
			Expect(err).NotTo(HaveOccurred())

			hashData[job.ID] = msg
			jobEntries[idx] = redis.Z{
				Score:  float64(at.Unix()),
				Member: job.ID,
			}
		}

		err = client.HMSet(consumer.queue.jobDataHash, hashData).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.ZAdd(consumer.queue.scheduledJobsSet, jobEntries...).Err()
		Expect(err).NotTo(HaveOccurred())
	}

	assignJobs := func(jobs []*Job, consumer *Consumer) {
		var err error
		hashData := map[string]interface{}{}
		jobIDs := make([]interface{}, len(jobs))
		for idx, job := range jobs {
			msg, err := job.message()
			Expect(err).NotTo(HaveOccurred())

			jobIDs[idx] = job.ID
			hashData[job.ID] = msg
		}

		err = client.HMSet(consumer.queue.jobDataHash, hashData).Err()
		Expect(err).NotTo(HaveOccurred())

		err = client.SAdd(consumer.inflightSet, jobIDs...).Err()
		Expect(err).NotTo(HaveOccurred())
	}

	registerConsumer := func(consumer *Consumer, at time.Time) {
		err := client.ZAdd(consumer.queue.consumersSet, redis.Z{
			Score:  float64(at.Unix()),
			Member: consumer.inflightSet,
		}).Err()
		Expect(err).NotTo(HaveOccurred())
	}

	poller := func(poll func() []interface{}) (chan []interface{}, chan struct{}) {
		resultsChan := make(chan []interface{}, 1)
		stopChan := make(chan struct{})

		ticker := time.NewTicker(500 * time.Millisecond)
		go func() {
			for {
				select {
				case <-ticker.C:
					results := poll()
					select {
					case resultsChan <- results:
					default:
					}
				case <-stopChan:
					ticker.Stop()
					return
				}
			}
		}()

		return resultsChan, stopChan
	}

	activeJobsPoller := func() (chan []interface{}, chan struct{}) {
		return poller(func() []interface{} {
			activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())

			results := make([]interface{}, len(activeJobs))
			for idx, job := range activeJobs {
				results[idx] = job
			}

			return results
		})
	}

	scheduledJobsPoller := func() (chan []interface{}, chan struct{}) {
		return poller(func() []interface{} {
			scheduledJobs, err := client.ZRange(consumer.queue.scheduledJobsSet, 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())

			results := make([]interface{}, len(scheduledJobs))
			for idx, job := range scheduledJobs {
				results[idx] = job
			}

			return results
		})
	}

	deadJobsPoller := func() (chan []interface{}, chan struct{}) {
		return poller(func() []interface{} {
			deadJobs, err := client.ZRange(consumer.queue.deadJobsSet, 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())

			results := make([]interface{}, len(deadJobs))
			for idx, job := range deadJobs {
				results[idx] = job
			}

			return results
		})
	}

	inflightJobsPoller := func(c *Consumer) (chan []interface{}, chan struct{}) {
		return poller(func() []interface{} {
			inflightJobs, err := client.SMembers(c.inflightSet).Result()
			Expect(err).NotTo(HaveOccurred())

			results := make([]interface{}, len(inflightJobs))
			for idx, job := range inflightJobs {
				results[idx] = job
			}

			return results
		})
	}

	consumersPoller := func() (chan []interface{}, chan struct{}) {
		return poller(func() []interface{} {
			consumers, err := client.ZRangeWithScores(consumer.queue.consumersSet, 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())

			results := make([]interface{}, len(consumers))
			for idx, consumer := range consumers {
				results[idx] = consumer
			}

			return results
		})
	}

	processedJobsPoller := func() (chan []string, chan string) {
		lock := &sync.Mutex{}
		ids := []string{}
		ticker := time.NewTicker(100 * time.Millisecond)
		succeeded := make(chan []string)
		processed := make(chan string)

		go func() {
			for id := range processed {
				lock.Lock()
				ids = append(ids, id)
				lock.Unlock()
			}
			ticker.Stop()
		}()

		go func() {
			for range ticker.C {
				lock.Lock()
				succeeded <- ids
				lock.Unlock()
			}
		}()

		return succeeded, processed
	}

	BeforeEach(func() {
		s, err := miniredis.Run()
		Expect(err).NotTo(HaveOccurred())
		server = s

		client = redis.NewClient(&redis.Options{
			Addr: server.Addr(),
		})
		Expect(client.FlushDB().Err()).NotTo(HaveOccurred())

		queue = "test_queue"
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
		go server.Close()
	})

	Describe("NewConsumer", func() {
		It("Returns a valid consumer", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
			})

			Expect(consumer.client).To(Equal(client))
			Expect(consumer.queue.name).To(Equal(queue))

			id, err := uuid.FromString(consumer.id)
			Expect(err).NotTo(HaveOccurred())

			inflightSet := fmt.Sprintf("test_queue:inflight:%s", id)
			Expect(consumer.inflightSet).To(Equal(inflightSet))
		})

		It("Loads all of the expected Lua scripts", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
			})

			Expect(consumer.ackJobScript).NotTo(BeNil())
			Expect(consumer.enqueueScheduledJobsScript).NotTo(BeNil())
			Expect(consumer.getJobsScript).NotTo(BeNil())
			Expect(consumer.killJobScript).NotTo(BeNil())
			Expect(consumer.reenqueueActiveJobsScript).NotTo(BeNil())
			Expect(consumer.reenqueueOrphanedJobsScript).NotTo(BeNil())
			Expect(consumer.registerConsumerScript).NotTo(BeNil())
			Expect(consumer.retryJobScript).NotTo(BeNil())
		})

		It("Generates a client based on the provided Address", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Address: server.Addr(),
				Queue:   queue,
			})

			Expect(consumer.client).NotTo(Equal(client))

			err := client.Set("a", "b", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := consumer.client.Get("a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))
		})

		It("Uses Client instead of Address when both are provided", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Address: "some.random.address",
				Client:  client,
				Queue:   queue,
			})

			Expect(consumer.client).To(Equal(client))
		})

		It("Panics when neither a redis client nor address is provided", func() {
			Expect(func() {
				NewConsumer(&ConsumerOpts{
					Queue: queue,
				})
			}).To(Panic())
		})

		It("Panics when a queue is not provided", func() {
			Expect(func() {
				NewConsumer(&ConsumerOpts{
					Client: client,
				})
			}).To(Panic())
		})

		It("Applies the correct default options", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
			})

			Expect(consumer.opts.Logger).To(Equal(&DefaultLogger{}))
			Expect(consumer.opts.JobMaxAttempts).To(Equal(20))
			Expect(consumer.opts.JobMaxBackoff).To(Equal(168 * time.Hour))
			Expect(consumer.opts.ShutdownGracePeriod).To(Equal(time.Duration(0)))

			Expect(consumer.opts.CustodianConsumerTimeout).To(Equal(1 * time.Minute))
			Expect(consumer.opts.CustodianMaxAttempts).To(Equal(0))
			Expect(consumer.opts.CustodianMaxBackoff).To(Equal(30 * time.Second))
			Expect(consumer.opts.CustodianMaxJobs).To(Equal(50))
			Expect(consumer.opts.CustodianPollInterval).To(Equal(1 * time.Minute))

			Expect(consumer.opts.HeartbeatMaxAttempts).To(Equal(0))
			Expect(consumer.opts.HeartbeatMaxBackoff).To(Equal(30 * time.Second))
			Expect(consumer.opts.HeartbeatPollInterval).To(Equal(1 * time.Minute))

			Expect(consumer.opts.PollerBufferSize).To(Equal(10))
			Expect(consumer.opts.PollerMaxAttempts).To(Equal(0))
			Expect(consumer.opts.PollerMaxBackoff).To(Equal(30 * time.Second))
			Expect(consumer.opts.PollerPollDuration).To(Equal(5 * time.Second))

			Expect(consumer.opts.ProcessorConcurrency).To(Equal(5))

			Expect(consumer.opts.SchedulerMaxAttempts).To(Equal(0))
			Expect(consumer.opts.SchedulerMaxBackoff).To(Equal(30 * time.Second))
			Expect(consumer.opts.SchedulerMaxJobs).To(Equal(50))
			Expect(consumer.opts.SchedulerPollInterval).To(Equal(5 * time.Second))
		})

		It("Applies the minimum values to relevant options", func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client:                   client,
				Queue:                    queue,
				CustodianConsumerTimeout: 1 * time.Second,
				HeartbeatPollInterval:    1 * time.Second,
				PollerPollDuration:       500 * time.Millisecond,
			})

			Expect(consumer.opts.CustodianConsumerTimeout).To(Equal(5 * time.Second))
			Expect(consumer.opts.HeartbeatPollInterval).To(Equal(15 * time.Second))
			Expect(consumer.opts.PollerPollDuration).To(Equal(1 * time.Second))
		})
	})

	Describe("Public API", func() {
		BeforeEach(func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client:               client,
				Queue:                queue,
				Logger:               &EmptyLogger{},
				ShutdownGracePeriod:  2 * time.Second,
				ProcessorConcurrency: 2,
				PollerBufferSize:     4,
				PollerPollDuration:   1 * time.Second,
				JobMaxAttempts:       10,
			})
		})

		AfterEach(func() {
			consumer.processes.Wait()
		})

		Describe("Consume", func() {
			start := func(handler HandlerFunc) chan error {
				errors := make(chan error)
				go func() {
					errors <- consumer.Consume(handler, syscall.SIGALRM)
				}()

				return errors
			}

			It("Shuts down when the process is killed", func() {
				block := make(chan struct{})

				consumer.opts.ShutdownGracePeriod = 5 * time.Second
				errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					return nil
				})

				enqueueJobs(createJobs(1, 0))
				<-block

				process, err := os.FindProcess(os.Getpid())
				Expect(err).NotTo(HaveOccurred())
				process.Signal(syscall.SIGALRM)
				close(block)
				Eventually(errors).Should(Receive(BeNil()))
			})

			It("Returns an error when a job doesn't finish in ShutdownGracePeriod", func() {
				block := make(chan struct{})

				errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					<-block
					return nil
				})

				enqueueJobs(createJobs(1, 0))
				<-block
				process, err := os.FindProcess(os.Getpid())
				Expect(err).NotTo(HaveOccurred())
				process.Signal(syscall.SIGALRM)
				Eventually(errors).Should(Receive(HaveOccurred()))
				close(block)
			})

			It("Bubbles up errors from the underlying ConsumeCtx", func() {
				block := make(chan struct{})

				errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					go server.Close()
					return nil
				})

				enqueueJobs(createJobs(1, 0))
				<-block
				Eventually(errors).Should(Receive(BeAssignableToTypeOf(ErrFailedToAckJob{})))
				close(block)
			})
		})

		Describe("ConsumeCtx", func() {
			start := func(handler HandlerFunc) (context.CancelFunc, chan error) {
				errors := make(chan error)
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					errors <- consumer.ConsumeCtx(ctx, handler)
				}()

				return cancel, errors
			}

			It("Shuts down when the context is canceled", func() {
				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})

				cancel()
				Eventually(errors).Should(Receive(BeNil()))
			})

			It("Returns an error when a job doesn't finish in ShutdownGracePeriod", func() {
				block := make(chan struct{})

				cancel, errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					<-block
					return nil
				})

				enqueueJobs(createJobs(1, 0))
				<-block
				cancel()
				Eventually(errors).Should(Receive(HaveOccurred()))
				close(block)
			})

			It("Waits indefinitely for jobs to finish if ShutdownGracePeriod is zero", func() {
				block := make(chan struct{})

				consumer.opts.ShutdownGracePeriod = 0
				cancel, errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					<-block
					return nil
				})

				enqueueJobs(createJobs(1, 0))
				<-block
				cancel()
				Consistently(errors, 3*time.Second).ShouldNot(Receive())
				close(block)
			})

			It("Immediately returns an error if Redis is unavailable when starting up", func() {
				go server.Close()
				_, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				Eventually(errors).Should(Receive(HaveOccurred()))
			})

			It("Processes jobs off the queue", func() {
				jobs := createJobs(5, 0)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				succeeded, processed := processedJobsPoller()
				defer close(processed)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					processed <- job.ID
					return nil
				})
				defer cancel()

				Eventually(active).Should(Receive(BeEmpty()))
				Eventually(succeeded).Should(Receive(ConsistOf(extractIds(jobs))))
			})

			It("Reschedules jobs that return an error with less than JobMaxAttempts", func() {
				jobs := createJobs(5, consumer.opts.JobMaxAttempts-1)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				scheduled, stopPollingScheduled := scheduledJobsPoller()
				defer close(stopPollingScheduled)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					return fmt.Errorf("Retry")
				})
				defer cancel()

				Eventually(active).Should(Receive(BeEmpty()))
				Eventually(scheduled).Should(Receive(ConsistOf(extractIds(jobs))))
			})

			It("Reschedules jobs that panic with less than JobMaxAttempts", func() {
				jobs := createJobs(5, consumer.opts.JobMaxAttempts-1)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				scheduled, stopPollingScheduled := scheduledJobsPoller()
				defer close(stopPollingScheduled)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					panic("Retry")
				})
				defer cancel()

				Eventually(active).Should(Receive(BeEmpty()))
				Eventually(scheduled).Should(Receive(ConsistOf(extractIds(jobs))))
			})

			It("Kill jobs that return an error after JobMaxAttempts", func() {
				jobs := createJobs(5, consumer.opts.JobMaxAttempts)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				dead, stopPollingDead := deadJobsPoller()
				defer close(stopPollingDead)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					return fmt.Errorf("Fail")
				})
				defer cancel()

				Eventually(active).Should(Receive(BeEmpty()))
				Eventually(dead).Should(Receive(ConsistOf(extractIds(jobs))))
			})

			It("Kills jobs that panic after JobMaxAttempts", func() {
				jobs := createJobs(5, consumer.opts.JobMaxAttempts)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				dead, stopPollingDead := deadJobsPoller()
				defer close(stopPollingDead)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					panic("Fail")
				})
				defer cancel()

				Eventually(active).Should(Receive(BeEmpty()))
				Eventually(dead).Should(Receive(ConsistOf(extractIds(jobs))))
			})

			It("Reenqueues jobs when shutting down", func() {
				running := make(chan struct{})
				block := make(chan struct{})

				jobs := createJobs(10, consumer.opts.JobMaxAttempts)
				ids := extractIds(jobs)
				enqueueJobs(jobs)

				active, stopPollingActive := activeJobsPoller()
				defer close(stopPollingActive)

				succeeded, processed := processedJobsPoller()
				defer close(processed)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					running <- struct{}{}
					<-block
					processed <- job.ID
					return nil
				})

				for i := 0; i < consumer.opts.ProcessorConcurrency; i++ {
					<-running
				}

				Eventually(active).Should(Receive(ConsistOf(ids[6:])))
				cancel()
				Eventually(active).Should(Receive(ConsistOf(ids[2:])))
				close(block)
				Eventually(succeeded).Should(Receive(ConsistOf([]string{
					ids[0],
					ids[1],
				})))
				close(running)
			})

			It("Aborts if it cannot acknowledge a job", func() {
				block := make(chan struct{})
				jobs := createJobs(1, 0)
				enqueueJobs(jobs)

				cancel, errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					go server.Close()
					return nil
				})
				defer cancel()

				<-block
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrFailedToAckJob{}),
					MatchAllFields(Fields{
						"Job": Equal(Job{
							ID:      jobs[0].ID,
							Attempt: jobs[0].Attempt,
							Data:    jobs[0].Data,
						}),
						"Err": HaveOccurred(),
					}),
				)))
			})

			It("Aborts if it cannot retry a job", func() {
				block := make(chan struct{})
				jobs := createJobs(1, 0)
				enqueueJobs(jobs)

				cancel, errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					go server.Close()
					return fmt.Errorf("Error")
				})
				defer cancel()

				<-block
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrFailedToRetryJob{}),
					MatchAllFields(Fields{
						"Job": Equal(Job{
							ID:      jobs[0].ID,
							Attempt: jobs[0].Attempt + 1,
							Data:    jobs[0].Data,
						}),
						"Err": HaveOccurred(),
					}),
				)))
			})

			It("Aborts if it cannot kill a job", func() {
				block := make(chan struct{})
				jobs := createJobs(1, consumer.opts.JobMaxAttempts)
				enqueueJobs(jobs)

				cancel, errors := start(func(ctx context.Context, job Job) error {
					block <- struct{}{}
					go server.Close()
					return fmt.Errorf("Error")
				})
				defer cancel()

				<-block
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrFailedToKillJob{}),
					MatchAllFields(Fields{
						"Job": Equal(Job{
							ID:      jobs[0].ID,
							Attempt: jobs[0].Attempt + 1,
							Data:    jobs[0].Data,
						}),
						"Err": HaveOccurred(),
					}),
				)))
			})

			It("Periodically updates the heartbeat", func() {
				consumers, stopPollingConsumers := consumersPoller()
				defer close(stopPollingConsumers)

				consumer.opts.HeartbeatPollInterval = 100 * time.Millisecond
				cancel, _ := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				var previouslySeenAt float64
				for i := 0; i < 2; i++ {
					Eventually(consumers).Should(Receive(ConsistOf(MatchAllFields(Fields{
						"Member": Equal(consumer.inflightSet),
						"Score":  BeNumerically(">", previouslySeenAt),
					}))))
					previouslySeenAt = (<-consumers)[0].(redis.Z).Score
				}
			})

			It("Enqueues scheduled jobs that are due to be run", func() {
				consumer.opts.SchedulerMaxJobs = 3

				dueJobs := createJobs(5, 0)
				laterJobs := createJobs(5, 0)
				scheduleJobs(dueJobs, time.Now())
				scheduleJobs(laterJobs, time.Now().Add(5*time.Minute))

				scheduled, stopPollingScheduled := scheduledJobsPoller()
				defer close(stopPollingScheduled)

				succeeded, processed := processedJobsPoller()
				defer close(processed)

				cancel, _ := start(func(ctx context.Context, job Job) error {
					processed <- job.ID
					return nil
				})
				defer cancel()

				Eventually(succeeded).Should(Receive(ConsistOf(extractIds(dueJobs))))
				Eventually(scheduled).Should(Receive(ConsistOf(extractIds(laterJobs))))
			})

			It("Cleans up jobs orphaned by dead consumers", func() {
				consumer.opts.CustodianMaxJobs = 3

				inflightJobs := createJobs(5, 0)
				orphanedJobs := createJobs(5, 0)

				deadConsumer := NewConsumer(&ConsumerOpts{
					Client: client,
					Queue:  queue,
				})
				timeoutInterval := consumer.opts.HeartbeatPollInterval + consumer.opts.CustodianConsumerTimeout
				registerConsumer(deadConsumer, time.Now().Add(-timeoutInterval))
				assignJobs(orphanedJobs, deadConsumer)

				liveConsumer := NewConsumer(&ConsumerOpts{
					Client: client,
					Queue:  queue,
				})
				registerConsumer(liveConsumer, time.Now())
				assignJobs(inflightJobs, liveConsumer)

				liveInflight, stopPollingLiveInflight := inflightJobsPoller(liveConsumer)
				defer close(stopPollingLiveInflight)

				deadInflight, stopPollingDeadInflight := inflightJobsPoller(deadConsumer)
				defer close(stopPollingDeadInflight)

				succeeded, processed := processedJobsPoller()
				defer close(processed)

				Eventually(liveInflight).Should(Receive(ConsistOf(extractIds(inflightJobs))))
				Eventually(deadInflight).Should(Receive(ConsistOf(extractIds(orphanedJobs))))

				cancel, _ := start(func(ctx context.Context, job Job) error {
					processed <- job.ID
					return nil
				})
				defer cancel()

				Eventually(deadInflight).Should(Receive(ConsistOf([]string{})))
				Eventually(liveInflight).Should(Receive(ConsistOf(extractIds(inflightJobs))))
				Eventually(succeeded).Should(Receive(ConsistOf(extractIds(orphanedJobs))))
			})

			It("Aborts if the poller process can't poll Redis after PollerMaxAttempts", func() {
				consumer.opts.PollerMaxAttempts = 2
				consumer.opts.PollerMaxBackoff = 100 * time.Millisecond

				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				consumers, stopPollingConsumers := consumersPoller()
				defer close(stopPollingConsumers)

				Eventually(consumers).Should(Receive(HaveLen(1)))
				go server.Close()
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrExceededMaxBackoff{}),
					MatchAllFields(Fields{
						"Attempt": Equal(consumer.opts.PollerMaxAttempts),
						"Process": Equal("Poller"),
					}),
				)))
			})

			It("Aborts if the poller process can't retrieve jobs from Redis after PollerMaxAttempts", func() {
				consumer.opts.PollerMaxAttempts = 2
				consumer.opts.PollerMaxBackoff = 100 * time.Millisecond

				// Force an error in the getJobs script by setting an incorrect datatype
				enqueueJobs(createJobs(1, 0))
				err := client.HSet(consumer.inflightSet, "key", "value").Err()
				Expect(err).NotTo(HaveOccurred())

				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				consumers, stopPollingConsumers := consumersPoller()
				defer close(stopPollingConsumers)

				Eventually(consumers).Should(Receive(HaveLen(1)))
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrExceededMaxBackoff{}),
					MatchAllFields(Fields{
						"Attempt": Equal(consumer.opts.PollerMaxAttempts),
						"Process": Equal("Poller"),
					}),
				)))
			})

			It("Aborts if the scheduler process can't poll Redis after SchedulerMaxAttempts", func() {
				consumer.opts.SchedulerMaxAttempts = 2
				consumer.opts.SchedulerMaxBackoff = 100 * time.Millisecond
				consumer.opts.SchedulerPollInterval = 100 * time.Millisecond

				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				consumers, stopPollingConsumers := consumersPoller()
				Eventually(consumers).Should(Receive(HaveLen(1)))
				close(stopPollingConsumers)

				go server.Close()
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrExceededMaxBackoff{}),
					MatchAllFields(Fields{
						"Attempt": Equal(consumer.opts.SchedulerMaxAttempts),
						"Process": Equal("Scheduler"),
					}),
				)))
			})

			It("Aborts if the custodian process can't poll Redis after CustodianMaxAttempts", func() {
				consumer.opts.CustodianMaxAttempts = 2
				consumer.opts.CustodianMaxBackoff = 100 * time.Millisecond
				consumer.opts.CustodianPollInterval = 100 * time.Millisecond

				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				consumers, stopPollingConsumers := consumersPoller()
				Eventually(consumers).Should(Receive(HaveLen(1)))
				close(stopPollingConsumers)

				go server.Close()
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrExceededMaxBackoff{}),
					MatchAllFields(Fields{
						"Attempt": Equal(consumer.opts.CustodianMaxAttempts),
						"Process": Equal("Custodian"),
					}),
				)))
			})

			It("Aborts if the heartbeat process can't poll Redis after HeartbeatMaxAttempts", func() {
				consumer.opts.HeartbeatMaxAttempts = 2
				consumer.opts.HeartbeatMaxBackoff = 100 * time.Millisecond
				consumer.opts.HeartbeatPollInterval = 100 * time.Millisecond

				cancel, errors := start(func(ctx context.Context, job Job) error {
					return nil
				})
				defer cancel()

				consumers, stopPollingConsumers := consumersPoller()
				Eventually(consumers).Should(Receive(HaveLen(1)))
				close(stopPollingConsumers)

				go server.Close()
				Eventually(errors).Should(Receive(And(
					BeAssignableToTypeOf(ErrExceededMaxBackoff{}),
					MatchAllFields(Fields{
						"Attempt": Equal(consumer.opts.HeartbeatMaxAttempts),
						"Process": Equal("Heartbeat"),
					}),
				)))
			})
		})
	})

	Describe("Redis operations", func() {
		BeforeEach(func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
				Logger: &EmptyLogger{},
			})
		})

		Describe("ackJob", func() {
			var job *Job

			BeforeEach(func() {
				job = &Job{
					ID:   "TestID",
					Data: []byte("TestData"),
				}
			})

			Context("When this consumer owns the job", func() {
				BeforeEach(func() {
					var err error

					err = client.HSet(consumer.queue.jobDataHash, job.ID, job.Data).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Acknowledges the job, removing it from Redis", func() {
					acked, err := consumer.ackJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(acked).To(Equal(true))

					err = client.HGet(consumer.queue.jobDataHash, job.ID).Err()
					Expect(err).To(Equal(redis.Nil))
				})
			})

			Context("When this consumer does not have ownership of the job", func() {
				BeforeEach(func() {
					err := client.HSet(consumer.queue.jobDataHash, job.ID, job.Data).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not remove the job data", func() {
					acked, err := consumer.ackJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(acked).To(Equal(false))

					jobData, err := client.HGet(consumer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("TestData"))
				})
			})
		})

		Describe("enqueueScheduledJobs", func() {
			Context("When there are scheduled jobs", func() {
				BeforeEach(func() {
					for i := 1; i < 5; i++ {
						id := fmt.Sprintf("job_id-%d", i)
						at := time.Now().Add(time.Duration(i) * time.Minute)

						if i <= 2 {
							at = at.Add(-5 * time.Minute)
						}

						err := client.ZAdd(consumer.queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Enqueues scheduled jobs that are ready to be run", func() {
					count, err := consumer.enqueueScheduledJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(2))

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(2))
					Expect(activeJobs[0]).To(Equal("job_id-1"))
					Expect(activeJobs[1]).To(Equal("job_id-2"))

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(Equal([]string{"1"}))

					scheduledJobs, err := client.ZRange(consumer.queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(2))
					Expect(scheduledJobs[0]).To(Equal("job_id-3"))
					Expect(scheduledJobs[1]).To(Equal("job_id-4"))
				})

				Context("When there is already a signal", func() {
					BeforeEach(func() {
						err := client.LPush(consumer.queue.signalList, 1).Err()
						Expect(err).NotTo(HaveOccurred())
					})

					It("Doesn't add extras", func() {
						count, err := consumer.enqueueScheduledJobs()
						Expect(err).NotTo(HaveOccurred())
						Expect(count).To(Equal(2))

						signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(signals).To(Equal([]string{"1"}))
					})
				})
			})

			Context("When there are no scheduled jobs ready to run", func() {
				BeforeEach(func() {
					for i := 1; i < 5; i++ {
						id := fmt.Sprintf("job_id-%d", i)
						at := time.Now().Add(time.Duration(i) * time.Hour)

						err := client.ZAdd(consumer.queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Does nothing", func() {
					count, err := consumer.enqueueScheduledJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))

					scheduledJobs, err := client.ZRange(consumer.queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(4))
					Expect(scheduledJobs[0]).To(Equal("job_id-1"))
					Expect(scheduledJobs[1]).To(Equal("job_id-2"))
					Expect(scheduledJobs[2]).To(Equal("job_id-3"))
					Expect(scheduledJobs[3]).To(Equal("job_id-4"))

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(0))

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(BeEmpty())
				})
			})

			Context("When there are more than SchedulerMaxJobs ready", func() {
				BeforeEach(func() {
					consumer = NewConsumer(&ConsumerOpts{
						Client:           client,
						Queue:            queue,
						SchedulerMaxJobs: 3,
					})

					for i := 1; i < 5; i++ {
						id := fmt.Sprintf("job_id-%d", i)
						at := time.Now().Add(-10 * time.Minute).Add(time.Duration(i) * time.Minute)

						err := client.ZAdd(consumer.queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Enqueues only the maximum number allowed", func() {
					count, err := consumer.enqueueScheduledJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(3))

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(3))
					Expect(activeJobs[0]).To(Equal("job_id-1"))
					Expect(activeJobs[1]).To(Equal("job_id-2"))
					Expect(activeJobs[2]).To(Equal("job_id-3"))

					scheduledJobs, err := client.ZRange(consumer.queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(1))
					Expect(scheduledJobs[0]).To(Equal("job_id-4"))
				})
			})
		})

		Describe("getJobs", func() {
			Context("When the consumer is not registered", func() {
				It("Returns an error", func() {
					_, err := consumer.getJobs(10)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("When the consumer is registered", func() {
				BeforeEach(func() {
					registerConsumer(consumer, time.Now())
				})

				Context("When there are jobs in the queue", func() {
					BeforeEach(func() {
						for i := 0; i < 4; i++ {
							var err error

							id := fmt.Sprintf("job_id-%d", i)
							data := []byte(fmt.Sprintf("job_data-%d", i))

							job := &Job{
								ID:      id,
								Attempt: i,
								Data:    data,
							}

							msg, err := job.message()
							Expect(err).NotTo(HaveOccurred())

							err = client.RPush(consumer.queue.activeJobsList, id).Err()
							Expect(err).NotTo(HaveOccurred())

							err = client.HSet(consumer.queue.jobDataHash, id, msg).Err()
							Expect(err).NotTo(HaveOccurred())
						}

						err := client.LPush(consumer.queue.signalList, 1).Err()
						Expect(err).NotTo(HaveOccurred())
					})

					Context("When count is larger than or equal to the number of jobs in the queue", func() {
						It("Reserves all the jobs from the queue and returns them", func() {
							jobs, err := consumer.getJobs(10)
							Expect(err).NotTo(HaveOccurred())
							Expect(len(jobs)).To(Equal(4))

							for idx, job := range jobs {
								Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
								Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
							}

							activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(activeJobs).To(BeEmpty())

							inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{
								"job_id-0",
								"job_id-1",
								"job_id-2",
								"job_id-3",
							}))

							for idx, job := range jobs {
								Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
								Expect(job.Attempt).To(Equal(idx))
								Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
							}

							signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(signals).To(BeEmpty())
						})
					})

					Context("When count is less than the number of jobs in the queue", func() {
						It("Reserves up to count jobs from the queue and returns them", func() {
							jobs, err := consumer.getJobs(2)
							Expect(err).NotTo(HaveOccurred())
							Expect(len(jobs)).To(Equal(2))

							for idx, job := range jobs {
								Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
								Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
							}

							activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(activeJobs).To(Equal([]string{
								"job_id-2",
								"job_id-3",
							}))

							inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{
								"job_id-0",
								"job_id-1",
							}))

							for idx, job := range jobs {
								Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
								Expect(job.Attempt).To(Equal(idx))
								Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
							}

							signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(signals).To(Equal([]string{"1"}))
						})
					})
				})

				Context("When there are no jobs in the queue", func() {
					It("Does nothing", func() {
						jobs, err := consumer.getJobs(10)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(jobs)).To(Equal(0))

						signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(signals).To(BeEmpty())
					})
				})
			})
		})

		Describe("killJob", func() {
			var job *Job

			BeforeEach(func() {
				job = &Job{
					ID:   "TestID",
					Data: []byte("TestData"),
				}
			})

			Context("When this consumer owns the job", func() {
				BeforeEach(func() {
					var err error

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(consumer.queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Moves the job to the dead set", func() {
					killed, err := consumer.killJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(killed).To(Equal(true))

					jobData, err := client.HGet(consumer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(1))

					deadJobs, err := client.ZRange(consumer.queue.deadJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(deadJobs).To(Equal([]string{
						job.ID,
					}))

					inflightJobs, err := client.LRange(consumer.inflightSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())
				})
			})

			Context("When this consumer does not have ownership of the job", func() {
				BeforeEach(func() {
					var err error

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(consumer.queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not alter the job data", func() {
					killed, err := consumer.killJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(killed).To(Equal(false))

					jobData, err := client.HGet(consumer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(0))

					deadJobs, err := client.ZRange(consumer.queue.deadJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(deadJobs).To(BeEmpty())
				})
			})
		})

		Describe("reenqueueActiveJobs", func() {
			var jobs []*Job
			var jobIDs []string

			BeforeEach(func() {
				for i := 0; i < 4; i++ {
					id := fmt.Sprintf("job_id-%d", i)
					data := []byte(fmt.Sprintf("job_data-%d", i))
					job := &Job{
						ID:      id,
						Attempt: i,
						Data:    data,
					}

					jobs = append(jobs, job)
					jobIDs = append(jobIDs, id)

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(consumer.queue.jobDataHash, id, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Context("When this consumer owns the jobs", func() {
				BeforeEach(func() {
					for _, jobID := range jobIDs {
						err := client.SAdd(consumer.inflightSet, jobID).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Moves the jobs back to the active list", func() {
					err := consumer.reenqueueActiveJobs(jobs)
					Expect(err).NotTo(HaveOccurred())

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(ConsistOf(jobIDs))

					inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(Equal([]string{"1"}))
				})
			})

			Context("When this consumer does not own some of the jobs", func() {
				BeforeEach(func() {
					for idx, jobID := range jobIDs {
						if idx < 2 {
							err := client.SAdd(consumer.inflightSet, jobID).Err()
							Expect(err).NotTo(HaveOccurred())
						}
					}
				})

				It("Moves only the owned jobs back to the active list", func() {
					err := consumer.reenqueueActiveJobs(jobs)
					Expect(err).NotTo(HaveOccurred())

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(ConsistOf([]string{jobIDs[0], jobIDs[1]}))

					inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(Equal([]string{"1"}))
				})
			})
		})

		Describe("reenqueueOrphanedJobs", func() {
			Context("When there are no registered consumers", func() {
				It("Doesn't do anything", func() {
					count, err := consumer.reenqueueOrphanedJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))

					consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(BeEmpty())

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(BeEmpty())

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(BeEmpty())
				})
			})

			Context("When there are no out of date consumers", func() {
				var otherInflightSets []string
				var jobIDs []string
				BeforeEach(func() {
					for i := 0; i < 4; i++ {
						c := NewConsumer(&ConsumerOpts{
							Client: client,
							Queue:  queue,
						})
						otherInflightSets = append(otherInflightSets, c.inflightSet)

						err := client.ZAdd(consumer.queue.consumersSet, redis.Z{
							Score:  float64(time.Now().Unix()),
							Member: c.inflightSet,
						}).Err()
						Expect(err).NotTo(HaveOccurred())

						jobID := fmt.Sprintf("job-%d", i)
						jobIDs = append(jobIDs, jobID)
						err = client.SAdd(c.inflightSet, jobID).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Doesn't do anything", func() {
					count, err := consumer.reenqueueOrphanedJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))

					consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf(otherInflightSets))

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(BeEmpty())

					for i, inflightSet := range otherInflightSets {
						inflightJobs, err := client.SMembers(inflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
					}

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(BeEmpty())
				})
			})

			Context("When there are some out of date consumers", func() {
				var inflightSets []string
				var jobIDs []string
				BeforeEach(func() {
					inflightSets = make([]string, 4)
					for i := 0; i < 4; i++ {
						c := NewConsumer(&ConsumerOpts{
							Client: client,
							Queue:  queue,
						})
						inflightSets[i] = c.inflightSet

						var score time.Time
						if i < 2 {
							// The first two consumers are live.
							score = time.Now()
						} else {
							// The second two are expired.
							score = time.Now().Add(-consumer.opts.CustodianConsumerTimeout).Add(-consumer.opts.HeartbeatPollInterval).Add(time.Duration(-i) * time.Minute)
						}

						err := client.ZAdd(consumer.queue.consumersSet, redis.Z{
							Score:  float64(score.Unix()),
							Member: c.inflightSet,
						}).Err()
						Expect(err).NotTo(HaveOccurred())

						jobID := fmt.Sprintf("job-%d", i)
						jobIDs = append(jobIDs, jobID)
						err = client.SAdd(c.inflightSet, jobID).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Reschedules only their jobs", func() {
					retainedInflightSets := inflightSets[:2]
					removedInflightSets := inflightSets[2:]

					count, err := consumer.reenqueueOrphanedJobs()
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(2))

					consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf(retainedInflightSets))

					activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(ConsistOf([]string{
						jobIDs[2],
						jobIDs[3],
					}))

					for i, inflightSet := range retainedInflightSets {
						inflightJobs, err := client.SMembers(inflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
					}

					for _, inflightSet := range removedInflightSets {
						inflightJobs, err := client.SMembers(inflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(inflightJobs).To(ConsistOf([]string{}))
					}

					signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(Equal([]string{"1"}))
				})

				Context("When there are more jobs to process than CustodianMaxJobs", func() {
					BeforeEach(func() {
						consumer = NewConsumer(&ConsumerOpts{
							Client:           client,
							Queue:            queue,
							CustodianMaxJobs: 3,
						})

						var err error

						err = client.SAdd(inflightSets[2], "extra_job_1").Err()
						Expect(err).NotTo(HaveOccurred())

						err = client.SAdd(inflightSets[3], "extra_job_2").Err()
						Expect(err).NotTo(HaveOccurred())
					})

					It("Only processes as many jobs as it can, working from the oldest consumer first", func() {
						retainedInflightSets := inflightSets[:3]
						removedInflightSets := inflightSets[3:]
						modifiedInflightSet := inflightSets[2]

						count, err := consumer.reenqueueOrphanedJobs()
						Expect(err).NotTo(HaveOccurred())
						Expect(count).To(Equal(3))

						consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(retainedInflightSets))

						activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(len(activeJobs)).To(Equal(3))
						Expect(activeJobs).To(ContainElement(jobIDs[3]))
						Expect(activeJobs).To(ContainElement("extra_job_2"))
						Expect(activeJobs).To(SatisfyAny(
							ContainElement(jobIDs[2]),
							ContainElement("extra_job_1"),
						))

						for i, inflightSet := range retainedInflightSets {
							if inflightSet != modifiedInflightSet {
								inflightJobs, err := client.SMembers(inflightSet).Result()
								Expect(err).NotTo(HaveOccurred())
								Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
							}
						}

						for _, inflightSet := range removedInflightSets {
							inflightJobs, err := client.SMembers(inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(BeEmpty())
						}

						inflightJobs, err := client.SMembers(modifiedInflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(inflightJobs).To(SatisfyAny(
							ContainElement(jobIDs[2]),
							ContainElement("extra_job_1"),
						))
						for _, job := range activeJobs {
							Expect(inflightJobs).NotTo(ContainElement(job))
						}

						signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(signals).To(Equal([]string{"1"}))
					})
				})

				Context("When the final job processed is the last job in a consumer's set", func() {
					BeforeEach(func() {
						consumer = NewConsumer(&ConsumerOpts{
							Client:           client,
							Queue:            queue,
							CustodianMaxJobs: 1,
						})
					})

					It("Leaves the consumer in the consumer set for a later call to process", func() {
						retainedInflightSets := inflightSets[:3]
						removedInflightSets := inflightSets[3:]

						count, err := consumer.reenqueueOrphanedJobs()
						Expect(err).NotTo(HaveOccurred())
						Expect(count).To(Equal(1))

						consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(inflightSets))

						activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(activeJobs).To(ConsistOf([]string{
							jobIDs[3],
						}))

						for i, inflightSet := range retainedInflightSets {
							inflightJobs, err := client.SMembers(inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
						}

						for _, inflightSet := range removedInflightSets {
							inflightJobs, err := client.SMembers(inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{}))
						}

						signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(signals).To(Equal([]string{"1"}))
					})
				})

				Context("When an expired consumer has no jobs", func() {
					BeforeEach(func() {
						err := client.Del(inflightSets[2]).Err()
						Expect(err).NotTo(HaveOccurred())
					})

					It("Removes it and moves on to other consumers", func() {
						retainedInflightSets := inflightSets[:2]
						removedInflightSets := inflightSets[2:]

						count, err := consumer.reenqueueOrphanedJobs()
						Expect(err).NotTo(HaveOccurred())
						Expect(count).To(Equal(1))

						consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(retainedInflightSets))

						activeJobs, err := client.LRange(consumer.queue.activeJobsList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(activeJobs).To(ConsistOf([]string{
							jobIDs[3],
						}))

						for i, inflightSet := range retainedInflightSets {
							inflightJobs, err := client.SMembers(inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
						}

						for _, inflightSet := range removedInflightSets {
							inflightJobs, err := client.SMembers(inflightSet).Result()
							Expect(err).NotTo(HaveOccurred())
							Expect(inflightJobs).To(ConsistOf([]string{}))
						}

						signals, err := client.LRange(consumer.queue.signalList, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(signals).To(Equal([]string{"1"}))
					})
				})
			})
		})

		Describe("registerConsumer", func() {
			Context("When the consumer is not registered", func() {
				It("Adds it to the consumers set", func() {
					err := consumer.registerConsumer()
					Expect(err).NotTo(HaveOccurred())

					consumers, err := client.ZRange(consumer.queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf([]string{consumer.inflightSet}))
				})
			})

			Context("When the consumer was previously registered", func() {
				BeforeEach(func() {
					err := client.ZAdd(consumer.queue.consumersSet, redis.Z{
						Member: consumer.inflightSet,
						Score:  float64(time.Now().Add(-5 * time.Minute).Unix()),
					}).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Updates it's last seen time", func() {
					err := consumer.registerConsumer()
					Expect(err).NotTo(HaveOccurred())

					consumers, err := client.ZRangeWithScores(consumer.queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(consumers)).To(Equal(1))

					stored := consumers[0]
					Expect(stored.Member).To(Equal(consumer.inflightSet))
					Expect(stored.Score).To(BeNumerically(">", float64(time.Now().Add(-1*time.Minute).Unix())))
				})
			})
		})

		Describe("retryJob", func() {
			var job *Job

			BeforeEach(func() {
				job = &Job{
					ID:      "TestID",
					Data:    []byte("TestData"),
					Attempt: 6,
				}
			})

			Context("When this consumer owns the job", func() {
				BeforeEach(func() {
					var err error

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(consumer.queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Moves the job to the scheduled set at a time determined by the backoff routine", func() {
					retried, err := consumer.retryJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(retried).To(Equal(true))

					jobData, err := client.HGet(consumer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(7))

					scheduledJobs, err := client.ZRangeWithScores(consumer.queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(1))
					Expect(scheduledJobs[0].Member).To(Equal(job.ID))
					Expect(scheduledJobs[0].Score).To(BeNumerically(">=", float64(time.Now().Add(32*time.Second).Unix())))
					Expect(scheduledJobs[0].Score).To(BeNumerically("<=", float64(time.Now().Add(64*time.Second).Unix())))

					inflightJobs, err := client.LRange(consumer.inflightSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())
				})
			})

			Context("When this consumer does not have ownership of the job", func() {
				BeforeEach(func() {
					var err error

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(consumer.queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not alter the job data", func() {
					retried, err := consumer.retryJob(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(retried).To(Equal(false))

					jobData, err := client.HGet(consumer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(6))

					scheduledJobs, err := client.ZRange(consumer.queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(scheduledJobs).To(BeEmpty())
				})
			})
		})
	})
})
