package curlyq

import (
	"context"
	"fmt"
	"strconv"
	"syscall"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/gofrs/uuid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var client *redis.Client
	var queue *Queue
	var server *miniredis.Miniredis

	BeforeEach(func() {
		s, err := miniredis.Run()
		Expect(err).NotTo(HaveOccurred())
		server = s

		client = redis.NewClient(&redis.Options{
			Addr: server.Addr(),
		})
		Expect(client.FlushDB().Err()).NotTo(HaveOccurred())

		queue = NewQueue(&QueueOpts{
			Name: "test",
		})
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
		server.Close()
	})

	Describe("NewConsumer", func() {
		var consumer *Consumer

		Context("When provided with valid values", func() {
			BeforeEach(func() {
				consumer = NewConsumer(&ConsumerOpts{
					Client: client,
					Queue:  queue,
				})
			})

			It("Returns a valid consumer", func() {
				Expect(consumer.opts.Client).To(Equal(client))
				Expect(consumer.opts.Queue).To(Equal(queue))

				id, err := uuid.FromString(consumer.id)
				Expect(err).NotTo(HaveOccurred())

				inflightSet := fmt.Sprintf("curlyq:test:inflight:%s", id)
				Expect(consumer.inflightSet).To(Equal(inflightSet))
			})

			It("Loads all of the expected Lua scripts", func() {
				Expect(consumer.ackJobScript).NotTo(BeNil())
				Expect(consumer.enqueueScheduledJobsScript).NotTo(BeNil())
				Expect(consumer.getJobsScript).NotTo(BeNil())
				Expect(consumer.killJobScript).NotTo(BeNil())
				Expect(consumer.reenqueueActiveJobsScript).NotTo(BeNil())
				Expect(consumer.reenqueueOrphanedJobsScript).NotTo(BeNil())
				Expect(consumer.registerConsumerScript).NotTo(BeNil())
				Expect(consumer.retryJobScript).NotTo(BeNil())
			})

			It("Applies the correct default options", func() {
				Expect(consumer.opts.ShutdownGracePeriod).To(Equal(time.Duration(0)))
				Expect(consumer.opts.CustodianPollInterval).To(Equal(1 * time.Minute))
				Expect(consumer.opts.CustodianMaxJobs).To(Equal(uint(50)))
				Expect(consumer.opts.CustodianConsumerTimeout).To(Equal(1 * time.Minute))
				Expect(consumer.opts.ExecutorsConcurrency).To(Equal(uint(10)))
				Expect(consumer.opts.ExecutorsPollInterval).To(Equal(3 * time.Second))
				Expect(consumer.opts.ExecutorsBufferSize).To(Equal(uint(10)))
				Expect(consumer.opts.ExecutorsMaxAttempts).To(Equal(uint(5)))
				Expect(consumer.opts.HeartbeatInterval).To(Equal(1 * time.Minute))
				Expect(consumer.opts.SchedulerPollInterval).To(Equal(15 * time.Second))
				Expect(consumer.opts.SchedulerPollInterval).To(Equal(15 * time.Second))
				Expect(consumer.opts.SchedulerMaxJobs).To(Equal(uint(50)))
			})
		})

		Context("When options are provided that are below minimum values", func() {
			BeforeEach(func() {
				consumer = NewConsumer(&ConsumerOpts{
					Client:                   client,
					Queue:                    queue,
					CustodianConsumerTimeout: 1 * time.Second,
					HeartbeatInterval:        1 * time.Second,
				})
			})

			It("Applies the minimum values", func() {
				Expect(consumer.opts.CustodianConsumerTimeout).To(Equal(5 * time.Second))
				Expect(consumer.opts.HeartbeatInterval).To(Equal(15 * time.Second))
			})
		})

		Context("When options are applied that affect other options", func() {
			Context("And the other options are not set", func() {
				BeforeEach(func() {
					consumer = NewConsumer(&ConsumerOpts{
						Client:               client,
						Queue:                queue,
						ExecutorsConcurrency: 25,
					})
				})

				It("Applies the correct values to the other options", func() {
					Expect(consumer.opts.ExecutorsBufferSize).To(Equal(uint(25)))
				})
			})

			Context("And the other options are set", func() {
				BeforeEach(func() {
					consumer = NewConsumer(&ConsumerOpts{
						Client:               client,
						Queue:                queue,
						ExecutorsConcurrency: 25,
						ExecutorsBufferSize:  10,
					})
				})

				It("Retains the values of the other options", func() {
					Expect(consumer.opts.ExecutorsConcurrency).To(Equal(uint(25)))
					Expect(consumer.opts.ExecutorsBufferSize).To(Equal(uint(10)))
				})
			})
		})

		Context("When a redis client is not provided", func() {
			It("Panics", func() {
				Expect(func() {
					NewConsumer(&ConsumerOpts{
						Queue: queue,
					})
				}).To(Panic())
			})
		})

		Context("When a queue is not provided", func() {
			It("Panics", func() {
				Expect(func() {
					NewConsumer(&ConsumerOpts{
						Client: client,
					})
				}).To(Panic())
			})
		})
	})

	Describe("Public API", func() {
		var consumer *Consumer

		BeforeEach(func() {
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
			})
		})

		Describe("ConsumeCtx", func() {
			It("Shuts down cleanly when the context is canceled", func(done Done) {
				processErrors := make(chan error)
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					processErrors <- consumer.ConsumeCtx(ctx, func(ctx context.Context, job Job) error {
						return nil
					})
				}()

				time.Sleep(100 * time.Millisecond)
				cancel()
				Expect(<-processErrors).To(BeNil())
				close(done)
			}, 5.0)

			Context("When a job runs longer than ShutdownGracePeriod", func() {
				BeforeEach(func() {
					consumer.opts.ShutdownGracePeriod = 500 * time.Millisecond

					job := &Job{
						ID:      "job-1",
						Attempt: 0,
						Data:    []byte("A number!"),
					}
					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.RPush(queue.activeJobsList, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Returns an error", func(done Done) {
					processErrors := make(chan error)
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						defer func() {
							if r := recover(); r != nil {
								processErrors <- fmt.Errorf("%v", r)
							}
						}()

						processErrors <- consumer.ConsumeCtx(ctx, func(ctx context.Context, job Job) error {
							time.Sleep(10 * time.Second)
							return nil
						})
					}()

					time.Sleep(100 * time.Millisecond)
					cancel()
					Expect(<-processErrors).To(HaveOccurred())
					close(done)
				})
			})
		})

		Describe("Consume", func() {
			It("Shuts down cleanly when the process is killed", func(done Done) {
				processErrors := make(chan error)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					processErrors <- consumer.Consume(func(ctx context.Context, job Job) error {
						return nil
					}, syscall.SIGUSR1)
				}()

				time.Sleep(100 * time.Millisecond)
				syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
				Expect(<-processErrors).To(BeNil())
				close(done)
			}, 5.0)

			Context("When a job runs longer than ShutdownGracePeriod", func() {
				BeforeEach(func() {
					consumer.opts.ShutdownGracePeriod = 500 * time.Millisecond

					job := &Job{
						ID:      "job-1",
						Attempt: 0,
						Data:    []byte("A number!"),
					}
					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.RPush(queue.activeJobsList, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Returns an error", func(done Done) {
					processErrors := make(chan error)
					go func() {
						defer func() {
							if r := recover(); r != nil {
								processErrors <- fmt.Errorf("%v", r)
							}
						}()

						processErrors <- consumer.Consume(func(ctx context.Context, job Job) error {
							return nil
						}, syscall.SIGUSR1)
					}()

					time.Sleep(100 * time.Millisecond)
					syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
					Expect(<-processErrors).To(BeNil())
					close(done)
				})
			})
		})
	})

	Describe("Processing Loops", func() {
		var consumer *Consumer
		var ctx context.Context
		var cancel context.CancelFunc
		var processErrors chan error

		BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			processErrors = make(chan error, 0)
		})

		AfterEach(func(done Done) {
			cancel()
			consumer.processes.Wait()
			Expect(<-processErrors).To(BeNil())
			close(done)
		}, 2.0)

		Describe("runExecutors", func() {
			start := func(handler HandlerFunc) {
				consumer.processes.Add(1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					consumer.runExecutors(ctx, handler)
					processErrors <- nil
				}()
			}

			addJobs := func(jobs []*Job) {
				var err error
				hashData := map[string]interface{}{}
				jobIDs := make([]interface{}, len(jobs))
				for idx, job := range jobs {
					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					jobIDs[idx] = job.ID
					hashData[job.ID] = msg
				}

				err = client.HMSet(queue.jobDataHash, hashData).Err()
				Expect(err).NotTo(HaveOccurred())

				err = client.RPush(queue.activeJobsList, jobIDs...).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			Context("When the context is canceled", func() {
				BeforeEach(func() {
					consumer = NewConsumer(&ConsumerOpts{
						Client:               client,
						Queue:                queue,
						ExecutorsConcurrency: 1,
						ExecutorsBufferSize:  5,
					})
				})

				It("Shuts down cleanly", func() {
					start(func(ctx context.Context, job Job) error {
						return nil
					})

					cancel()
				})

				It("Reenqueues jobs on the queue", func(done Done) {
					stop := make(chan struct{})

					addJobs([]*Job{
						&Job{ID: "job-1"},
						&Job{ID: "job-2"},
						&Job{ID: "job-3"},
						&Job{ID: "job-4"},
						&Job{ID: "job-5"},
						&Job{ID: "job-6"},
						&Job{ID: "job-7"},
						&Job{ID: "job-8"},
						&Job{ID: "job-9"},
						&Job{ID: "job-10"},
					})

					active := make(chan []string)
					activeTicker := time.NewTicker(10 * time.Millisecond)
					go func() {
						for _ = range activeTicker.C {
							activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							active <- activeJobs
						}
					}()

					start(func(ctx context.Context, job Job) error {
						<-stop
						return nil
					})

					// We can buffer five elements in addition to one being processed.
					// So only four elements should remain in the active list.
					Eventually(active).Should(Receive(ConsistOf([]string{
						"job-7",
						"job-8",
						"job-9",
						"job-10",
					})))

					// Cancel the process and wait until the queue is drained.
					cancel()
					Eventually(active).Should(Receive(ConsistOf([]string{
						"job-7",
						"job-8",
						"job-9",
						"job-10",
						"job-2",
						"job-3",
						"job-4",
						"job-5",
						"job-6",
					})))

					// Allow the in-process job to proceed.
					stop <- struct{}{}
					activeTicker.Stop()
					close(done)
				}, 5.0)
			})

			Context("When jobs are on the queue", func() {
				var successful chan string

				var completed chan []string
				var retried chan []string
				var failed chan []string

				var retriedTicker *time.Ticker
				var failedTicker *time.Ticker

				handler := func(ctx context.Context, job Job) error {
					id, err := strconv.Atoi(job.ID)
					if err != nil {
						return err
					}

					if id%2 == 0 {
						panic("No even numbers!")
					}

					successful <- job.ID
					return nil
				}

				BeforeEach(func() {
					consumer = NewConsumer(&ConsumerOpts{
						Client:                client,
						Queue:                 queue,
						ExecutorsPollInterval: 100 * time.Millisecond,
						ExecutorsMaxAttempts:  10,
						ExecutorsConcurrency:  3,
						ExecutorsBufferSize:   3,
					})

					successful = make(chan string)

					completed = make(chan []string)
					go func() {
						jobIDs := []string{}
						for id := range successful {
							jobIDs = append(jobIDs, id)
							completed <- jobIDs
						}
					}()

					retried = make(chan []string)
					retriedTicker = time.NewTicker(100 * time.Millisecond)
					go func() {
						for _ = range retriedTicker.C {
							scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							retried <- scheduledJobs
						}
					}()

					failed = make(chan []string)
					failedTicker = time.NewTicker(100 * time.Millisecond)
					go func() {
						for _ = range failedTicker.C {
							deadJobs, err := client.ZRange(queue.deadJobsSet, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							failed <- deadJobs
						}
					}()
				})

				AfterEach(func() {
					failedTicker.Stop()
					retriedTicker.Stop()

					close(successful)
					close(completed)
					close(retried)
					close(failed)
				})

				It("Processes jobs that are already on the queue when it starts", func(done Done) {
					addJobs([]*Job{
						&Job{ID: "1", Attempt: 9},
						&Job{ID: "2", Attempt: 9},
						&Job{ID: "three", Attempt: 10},
					})

					start(handler)

					Eventually(completed).Should(Receive(ConsistOf([]string{
						"1",
					})))
					Eventually(retried).Should(Receive(ConsistOf([]string{
						"2",
					})))
					Eventually(failed).Should(Receive(ConsistOf([]string{
						"three",
					})))

					close(done)
				})

				It("Processes jobs that are added to the queue after it starts", func(done Done) {
					start(handler)

					addJobs([]*Job{
						&Job{ID: "four", Attempt: 9},
						&Job{ID: "5", Attempt: 10},
						&Job{ID: "6", Attempt: 10},
					})

					Eventually(completed).Should(Receive(ConsistOf([]string{
						"5",
					})))
					Eventually(retried).Should(Receive(ConsistOf([]string{
						"four",
					})))
					Eventually(failed).Should(Receive(ConsistOf([]string{
						"6",
					})))

					close(done)
				})

				It("Continues processing jobs when there are more than it can handle in a single poll", func(done Done) {
					start(handler)

					addJobs([]*Job{
						&Job{ID: "7", Attempt: 9},
						&Job{ID: "8", Attempt: 9},
						&Job{ID: "nine", Attempt: 10},
						&Job{ID: "ten", Attempt: 9},
						&Job{ID: "11", Attempt: 10},
						&Job{ID: "12", Attempt: 10},
						&Job{ID: "13", Attempt: 9},
						&Job{ID: "14", Attempt: 9},
						&Job{ID: "15", Attempt: 9},
						&Job{ID: "17", Attempt: 0},
						&Job{ID: "19", Attempt: 0},
						&Job{ID: "21", Attempt: 0},
					})

					Eventually(completed).Should(Receive(ConsistOf([]string{
						"7",
						"11",
						"13",
						"15",
						"17",
						"19",
						"21",
					})))
					Eventually(retried).Should(Receive(ConsistOf([]string{
						"8",
						"ten",
						"14",
					})))
					Eventually(failed).Should(Receive(ConsistOf([]string{
						"nine",
						"12",
					})))

					close(done)
				}, 5.0)
			})
		})

		Describe("runHeartbeat", func() {
			BeforeEach(func() {
				consumer = NewConsumer(&ConsumerOpts{
					Client: client,
					Queue:  queue,
				})

				// Force a lower heartbeat interval for testing purposes.
				consumer.opts.HeartbeatInterval = 100 * time.Millisecond

				consumer.processes.Add(1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					consumer.runHeartbeat(ctx)
					processErrors <- nil
				}()
			})

			It("Shuts down cleanly when the context is canceled", func() {
				cancel()
			})

			It("Periodically updates the heartbeat", func(done Done) {
				var previouslySeenAt float64
				lastSeenAt := make(chan float64)
				ticker := time.NewTicker(50 * time.Millisecond)
				go func() {
					for _ = range ticker.C {
						score, err := client.ZScore(queue.consumersSet, consumer.inflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						lastSeenAt <- score
					}
				}()

				for i := 0; i < 2; i++ {
					Eventually(lastSeenAt).Should(Receive(BeNumerically(">", previouslySeenAt)))
					previouslySeenAt = <-lastSeenAt
				}

				// Clean up
				ticker.Stop()
				close(done)
			}, 5.0)
		})

		Describe("runScheduler", func() {
			BeforeEach(func() {
				consumer = NewConsumer(&ConsumerOpts{
					Client:                client,
					Queue:                 queue,
					SchedulerPollInterval: 100 * time.Millisecond,
				})

				consumer.processes.Add(1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					consumer.runScheduler(ctx)
					processErrors <- nil
				}()
			})

			It("Shuts down cleanly when the context is canceled", func() {
				cancel()
			})

			Context("When scheduled jobs are ready to be run", func() {
				addJob := func(id string, at time.Time) {
					err := client.ZAdd(queue.scheduledJobsSet, redis.Z{
						Score:  float64(at.Unix()),
						Member: id,
					}).Err()
					Expect(err).NotTo(HaveOccurred())
				}

				BeforeEach(func() {
					for i := 1; i < 5; i++ {
						id := fmt.Sprintf("job_id-%d", i)
						at := time.Now().Add(time.Duration(i) * time.Minute)
						if i <= 2 {
							at = at.Add(-5 * time.Minute)
						}

						addJob(id, at)
					}
				})

				It("Enqueues them", func(done Done) {
					jobIDs := make(chan []string)
					ticker := time.NewTicker(100 * time.Millisecond)
					go func() {
						for _ = range ticker.C {
							activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							jobIDs <- activeJobs
						}
					}()

					// Test that we get only the initial jobs that are schedulable.
					Eventually(jobIDs).Should(Receive(ConsistOf([]string{
						"job_id-1",
						"job_id-2",
					})))

					// Test that we get another job that is added later.
					addJob("another_job", time.Now().Add(time.Millisecond))
					Eventually(jobIDs).Should(Receive(ConsistOf([]string{
						"job_id-1",
						"job_id-2",
						"another_job",
					})))

					// Clean up
					ticker.Stop()
					close(done)
				}, 5.0)
			})
		})

		Describe("runCustodian", func() {
			BeforeEach(func() {
				consumer = NewConsumer(&ConsumerOpts{
					Client:                client,
					Queue:                 queue,
					CustodianPollInterval: 100 * time.Millisecond,
				})

				consumer.processes.Add(1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							processErrors <- fmt.Errorf("%v", r)
						}
					}()

					consumer.runCustodian(ctx)
					processErrors <- nil
				}()
			})

			It("Shuts down cleanly when the context is canceled", func() {
				cancel()
			})

			Context("When there are jobs to be cleaned up", func() {
				var consumers []*Consumer

				createConsumers := func(count int) []*Consumer {
					results := make([]*Consumer, count)
					for i := 0; i < count; i++ {
						results[i] = NewConsumer(&ConsumerOpts{
							Client: client,
							Queue:  queue,
						})
					}
					return results
				}

				registerConsumer := func(consumer *Consumer, lastSeenAt time.Time) {
					err := client.ZAdd(queue.consumersSet, redis.Z{
						Score:  float64(lastSeenAt.Unix()),
						Member: consumer.inflightSet,
					}).Err()
					Expect(err).NotTo(HaveOccurred())
				}

				reserveJob := func(consumer *Consumer, jobID string) {
					err := client.SAdd(consumer.inflightSet, jobID).Err()
					Expect(err).NotTo(HaveOccurred())
				}

				BeforeEach(func() {
					consumers = createConsumers(5)
					for idx, consumer := range consumers {
						jobID := fmt.Sprintf("job_id-%d", idx)
						reserveJob(consumer, jobID)

						lastSeenAt := time.Now()
						if idx < 2 {
							lastSeenAt = lastSeenAt.Add(-consumer.opts.HeartbeatInterval).Add(-consumer.opts.CustodianConsumerTimeout)
						}
						registerConsumer(consumer, lastSeenAt)
					}
				})

				It("Enqueues them", func(done Done) {
					jobIDs := make(chan []string)
					ticker := time.NewTicker(100 * time.Millisecond)
					go func() {
						for _ = range ticker.C {
							activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
							Expect(err).NotTo(HaveOccurred())
							jobIDs <- activeJobs
						}
					}()

					// Test that we get only the initial jobs that are schedulable.
					Eventually(jobIDs).Should(Receive(ConsistOf([]string{
						"job_id-0",
						"job_id-1",
					})))

					// Test that we get another job when another consumer expires.
					lastSeenAt := time.Now().Add(-consumer.opts.HeartbeatInterval).Add(-consumer.opts.CustodianConsumerTimeout)
					registerConsumer(consumers[2], lastSeenAt)
					Eventually(jobIDs).Should(Receive(ConsistOf([]string{
						"job_id-0",
						"job_id-1",
						"job_id-2",
					})))

					// Clean up
					ticker.Stop()
					close(done)
				}, 5.0)
			})
		})
	})

	Describe("Redis operations", func() {
		var consumer *Consumer
		var ctx context.Context

		BeforeEach(func() {
			ctx = context.Background()
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
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

					err = client.HSet(queue.jobDataHash, job.ID, job.Data).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Acknolwedges the job, removing it from Redis", func() {
					acked, err := consumer.ackJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(acked).To(Equal(true))

					err = client.HGet(queue.jobDataHash, job.ID).Err()
					Expect(err).To(Equal(redis.Nil))
				})
			})

			Context("When this consumer does not have ownership of the job", func() {
				BeforeEach(func() {
					var err error

					err = client.HSet(queue.jobDataHash, job.ID, job.Data).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not remove the job data", func() {
					acked, err := consumer.ackJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(acked).To(Equal(false))

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
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

						err := client.ZAdd(queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Enqueues scheduled jobs that are ready to be run", func() {
					count, err := consumer.enqueueScheduledJobs(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(2))

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(2))
					Expect(activeJobs[0]).To(Equal("job_id-1"))
					Expect(activeJobs[1]).To(Equal("job_id-2"))

					scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(2))
					Expect(scheduledJobs[0]).To(Equal("job_id-3"))
					Expect(scheduledJobs[1]).To(Equal("job_id-4"))
				})
			})

			Context("When there are no scheduled jobs ready to run", func() {
				BeforeEach(func() {
					for i := 1; i < 5; i++ {
						id := fmt.Sprintf("job_id-%d", i)
						at := time.Now().Add(time.Duration(i) * time.Hour)

						err := client.ZAdd(queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Does nothing", func() {
					count, err := consumer.enqueueScheduledJobs(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(0))

					scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(4))
					Expect(scheduledJobs[0]).To(Equal("job_id-1"))
					Expect(scheduledJobs[1]).To(Equal("job_id-2"))
					Expect(scheduledJobs[2]).To(Equal("job_id-3"))
					Expect(scheduledJobs[3]).To(Equal("job_id-4"))

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(0))
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

						err := client.ZAdd(queue.scheduledJobsSet, redis.Z{
							Score:  float64(at.Unix()),
							Member: id,
						}).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Enqueues only the maximum number allowed", func() {
					count, err := consumer.enqueueScheduledJobs(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).To(Equal(3))

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(activeJobs)).To(Equal(3))
					Expect(activeJobs[0]).To(Equal("job_id-1"))
					Expect(activeJobs[1]).To(Equal("job_id-2"))
					Expect(activeJobs[2]).To(Equal("job_id-3"))

					scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(1))
					Expect(scheduledJobs[0]).To(Equal("job_id-4"))
				})
			})
		})

		Describe("getJobs", func() {
			Context("When there are jobs in the queue", func() {
				BeforeEach(func() {
					for i := 0; i < 4; i++ {
						var err error

						id := fmt.Sprintf("job_id-%d", i)
						data := []byte(fmt.Sprintf("job_data-%d", i))

						job := &Job{
							ID:      id,
							Attempt: uint(i),
							Data:    data,
						}

						msg, err := job.message()
						Expect(err).NotTo(HaveOccurred())

						err = client.RPush(queue.activeJobsList, id).Err()
						Expect(err).NotTo(HaveOccurred())

						err = client.HSet(queue.jobDataHash, id, msg).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				Context("When count is larger than or equal to the number of jobs in the queue", func() {
					It("Reserves all the jobs from the queue and returns them", func() {
						jobs, err := consumer.getJobs(ctx, 10)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(jobs)).To(Equal(4))

						for idx, job := range jobs {
							Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
							Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
						}

						activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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
							Expect(job.Attempt).To(Equal(uint(idx)))
							Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
						}
					})
				})

				Context("When count is less than the number of jobs in the queue", func() {
					It("Reserves up to count jobs from the queue and returns them", func() {
						jobs, err := consumer.getJobs(ctx, 2)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(jobs)).To(Equal(2))

						for idx, job := range jobs {
							Expect(job.ID).To(Equal(fmt.Sprintf("job_id-%d", idx)))
							Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
						}

						activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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
							Expect(job.Attempt).To(Equal(uint(idx)))
							Expect(string(job.Data)).To(Equal(fmt.Sprintf("job_data-%d", idx)))
						}
					})
				})
			})

			Context("When there are no jobs in the queue", func() {
				It("Does nothing", func() {
					jobs, err := consumer.getJobs(ctx, 10)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(jobs)).To(Equal(0))
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

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Moves the job to the dead set", func() {
					killed, err := consumer.killJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(killed).To(Equal(true))

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(uint(1)))

					deadJobs, err := client.ZRange(queue.deadJobsSet, 0, -1).Result()
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

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not alter the job data", func() {
					killed, err := consumer.killJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(killed).To(Equal(false))

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(uint(0)))

					deadJobs, err := client.ZRange(queue.deadJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(deadJobs).To(BeEmpty())
				})
			})
		})

		Describe("reenqueueActiveJobs", func() {
			var jobs []*Job
			var job_ids []string

			BeforeEach(func() {
				for i := 0; i < 4; i++ {
					id := fmt.Sprintf("job_id-%d", i)
					data := []byte(fmt.Sprintf("job_data-%d", i))
					job := &Job{
						ID:      id,
						Attempt: uint(i),
						Data:    data,
					}

					jobs = append(jobs, job)
					job_ids = append(job_ids, id)

					msg, err := job.message()
					Expect(err).NotTo(HaveOccurred())

					err = client.HSet(queue.jobDataHash, id, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Context("When this consumer owns the jobs", func() {
				BeforeEach(func() {
					for _, job_id := range job_ids {
						err := client.SAdd(consumer.inflightSet, job_id).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("Moves the jobs back to the active list", func() {
					err := consumer.reenqueueActiveJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(ConsistOf(job_ids))

					inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())
				})
			})

			Context("When this consumer does not own some of the jobs", func() {
				BeforeEach(func() {
					for idx, job_id := range job_ids {
						if idx < 2 {
							err := client.SAdd(consumer.inflightSet, job_id).Err()
							Expect(err).NotTo(HaveOccurred())
						}
					}
				})

				It("Moves only the owned jobs back to the active list", func() {
					err := consumer.reenqueueActiveJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(ConsistOf([]string{job_ids[0], job_ids[1]}))

					inflightJobs, err := client.SMembers(consumer.inflightSet).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(inflightJobs).To(BeEmpty())
				})
			})
		})

		Describe("reenqueueOrphanedJobs", func() {
			Context("When there are no registered consumers", func() {
				It("Doesn't do anything", func() {
					count, err := consumer.reenqueueOrphanedJobs(ctx)
					Expect(count).To(Equal(0))

					consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(BeEmpty())

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(BeEmpty())
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

						err := client.ZAdd(queue.consumersSet, redis.Z{
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
					count, err := consumer.reenqueueOrphanedJobs(ctx)
					Expect(count).To(Equal(0))

					consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf(otherInflightSets))

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(activeJobs).To(BeEmpty())

					for i, inflightSet := range otherInflightSets {
						inflightJobs, err := client.SMembers(inflightSet).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(inflightJobs).To(ConsistOf([]string{jobIDs[i]}))
					}
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
							score = time.Now().Add(-consumer.opts.CustodianConsumerTimeout).Add(-consumer.opts.HeartbeatInterval).Add(time.Duration(-i) * time.Minute)
						}

						err := client.ZAdd(queue.consumersSet, redis.Z{
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

					count, err := consumer.reenqueueOrphanedJobs(ctx)
					Expect(count).To(Equal(2))

					consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf(retainedInflightSets))

					activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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

						count, err := consumer.reenqueueOrphanedJobs(ctx)
						Expect(count).To(Equal(3))

						consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(retainedInflightSets))

						activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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

						count, err := consumer.reenqueueOrphanedJobs(ctx)
						Expect(count).To(Equal(1))

						consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(inflightSets))

						activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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

						count, err := consumer.reenqueueOrphanedJobs(ctx)
						Expect(count).To(Equal(1))

						consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(consumers).To(ConsistOf(retainedInflightSets))

						activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
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
					})
				})
			})
		})

		Describe("registerConsumer", func() {
			Context("When the consumer is not registered", func() {
				It("Adds it to the consumers set", func() {
					err := consumer.registerConsumer(ctx)
					Expect(err).NotTo(HaveOccurred())

					consumers, err := client.ZRange(queue.consumersSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(consumers).To(ConsistOf([]string{consumer.inflightSet}))
				})
			})

			Context("When the consumer was previously registered", func() {
				BeforeEach(func() {
					err := client.ZAdd(queue.consumersSet, redis.Z{
						Member: consumer.inflightSet,
						Score:  float64(time.Now().Add(-5 * time.Minute).Unix()),
					}).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Updates it's last seen time", func() {
					err := consumer.registerConsumer(ctx)
					Expect(err).NotTo(HaveOccurred())

					consumers, err := client.ZRangeWithScores(queue.consumersSet, 0, -1).Result()
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

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())

					err = client.SAdd(consumer.inflightSet, job.ID).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Moves the job to the scheduled set at a time determined by the backoff routine", func() {
					retried, err := consumer.retryJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(retried).To(Equal(true))

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(uint(7)))

					scheduledJobs, err := client.ZRangeWithScores(queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(scheduledJobs)).To(Equal(1))
					Expect(scheduledJobs[0].Member).To(Equal(job.ID))
					Expect(scheduledJobs[0].Score).To(BeNumerically(">=", float64(time.Now().Add(60*time.Second).Unix())))
					Expect(scheduledJobs[0].Score).To(BeNumerically("<=", float64(time.Now().Add(70*time.Second).Unix())))

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

					err = client.HSet(queue.jobDataHash, job.ID, msg).Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not alter the job data", func() {
					retried, err := consumer.retryJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(retried).To(Equal(false))

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())

					err = job.fromMessage([]byte(jobData))
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Attempt).To(Equal(uint(6)))

					scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(scheduledJobs).To(BeEmpty())
				})
			})
		})
	})

	Describe("Helpers", func() {
		var consumer *Consumer
		var job *Job
		var ctx context.Context

		BeforeEach(func() {
			ctx = context.Background()
			job = &Job{
				ID:   "TestID",
				Data: []byte("TestData"),
			}
			consumer = NewConsumer(&ConsumerOpts{
				Client: client,
				Queue:  queue,
			})
		})

		Describe("executeJob", func() {
			It("Executes the job using the provided handler function", func() {
				err := consumer.executeJob(ctx, job, func(ctx context.Context, received Job) error {
					Expect(received).To(Equal(*job))
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
			})

			Context("When the handler returns an error", func() {
				It("Bubbles up the error", func() {
					err := consumer.executeJob(ctx, job, func(ctx context.Context, received Job) error {
						Expect(received).To(Equal(*job))
						return fmt.Errorf("I broke!")
					})
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("I broke!"))
				})
			})

			Context("When the handler panics", func() {
				It("Recovers and bubbles up an error", func() {
					err := consumer.executeJob(ctx, job, func(ctx context.Context, received Job) error {
						Expect(received).To(Equal(*job))
						panic("I broke!")
					})
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(fmt.Sprintf("Job %s panicked during execution: I broke!", job.ID)))
				})
			})
		})
	})
})
