package curlyq

import (
	"context"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v7"
	"github.com/gofrs/uuid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer", func() {
	var client *redis.Client
	var queue string
	var server *miniredis.Miniredis

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
		server.Close()
	})

	Describe("NewProducer", func() {
		var producer *Producer

		Context("When provided with valid values", func() {
			BeforeEach(func() {
				producer = NewProducer(&ProducerOpts{
					Client: client,
					Queue:  queue,
				})
			})

			It("Returns a valid producer", func() {
				Expect(producer.client).To(Equal(client))
				Expect(producer.queue.name).To(Equal(queue))
			})

			It("Loads all of the expected Lua scripts", func() {
				Expect(producer.pushJobScript).NotTo(BeNil())
				Expect(producer.scheduleJobScript).NotTo(BeNil())
			})

			It("Applies the correct default options", func() {
				Expect(producer.opts.Logger).To(Equal(&DefaultLogger{}))
			})
		})

		Context("When an Address is provided", func() {
			It("Generates a client based on the Address", func() {
				producer = NewProducer(&ProducerOpts{
					Address: server.Addr(),
					Queue:   queue,
				})

				Expect(producer.client).NotTo(Equal(client))

				err := client.Set("a", "b", 0).Err()
				Expect(err).NotTo(HaveOccurred())

				val, err := producer.client.Get("a").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal("b"))
			})

			Context("When a Client is also provided", func() {
				It("Uses the provided value for Client", func() {
					producer = NewProducer(&ProducerOpts{
						Address: "some.random.address",
						Client:  client,
						Queue:   queue,
					})

					Expect(producer.client).To(Equal(client))
				})
			})
		})

		Context("When a redis client is not provided", func() {
			It("Panics", func() {
				Expect(func() {
					NewProducer(&ProducerOpts{
						Queue: queue,
					})
				}).To(Panic())
			})
		})

		Context("When a queue is not provided", func() {
			It("Panics", func() {
				Expect(func() {
					NewProducer(&ProducerOpts{
						Client: client,
					})
				}).To(Panic())
			})
		})
	})

	Describe("Public API", func() {
		var producer *Producer
		var job Job

		BeforeEach(func() {
			producer = NewProducer(&ProducerOpts{
				Client: client,
				Queue:  queue,
				Logger: &EmptyLogger{},
			})

			job = Job{
				ID:   "TestID",
				Data: []byte("TestData"),
			}
		})

		Describe("PerformAfter", func() {
			var duration time.Duration

			BeforeEach(func() {
				duration = 3 * time.Minute
			})

			It("Creates a new job and schedules it to run after the provided duration", func() {
				id, err := producer.PerformAfter(duration, job)
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(job.ID))

				jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 211, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRangeWithScores(producer.queue.scheduledJobsSet, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(scheduledJobs)).To(Equal(1))
				Expect(scheduledJobs[0].Member).To(Equal(job.ID))

				lowerBound := time.Now().Add(2 * time.Minute).Unix()
				upperBound := time.Now().Add(4 * time.Minute).Unix()
				Expect(scheduledJobs[0].Score).To(BeNumerically(">", float64(lowerBound)))
				Expect(scheduledJobs[0].Score).To(BeNumerically("<", float64(upperBound)))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(producer.queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					_, err := producer.PerformAfter(duration, job)
					Expect(err).To(Equal(ErrJobAlreadyExists{
						Job: job,
					}))

					jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When an ID is not provided", func() {
				BeforeEach(func() {
					job = Job{
						Data: []byte("TestData"),
					}
				})

				It("Generates one", func() {
					id, err := producer.PerformAfter(duration, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.ID).To(Equal(""))

					_, err = uuid.FromString(id)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					_, err := producer.PerformAfter(duration, job)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("PerformAt", func() {
			var moment time.Time

			BeforeEach(func() {
				moment = time.Now().Add(3 * time.Minute)
			})

			It("Creates a new job and schedules it to run after the provided duration", func() {
				id, err := producer.PerformAt(moment, job)
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(job.ID))

				jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 211, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRangeWithScores(producer.queue.scheduledJobsSet, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(scheduledJobs)).To(Equal(1))
				Expect(scheduledJobs[0].Member).To(Equal(job.ID))
				Expect(scheduledJobs[0].Score).To(Equal(float64(moment.Unix())))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(producer.queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					_, err := producer.PerformAt(moment, job)
					Expect(err).To(Equal(ErrJobAlreadyExists{
						Job: job,
					}))

					jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When an ID is not provided", func() {
				BeforeEach(func() {
					job = Job{
						Data: []byte("TestData"),
					}
				})

				It("Generates one", func() {
					id, err := producer.PerformAt(moment, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.ID).To(Equal(""))

					_, err = uuid.FromString(id)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					_, err := producer.PerformAt(moment, job)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("Perform", func() {
			It("Creates a new job and enqueues it", func() {
				id, err := producer.Perform(job)
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(job.ID))

				jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 211, 0, 0, 0, 0, 0, 0, 0, 0}))

				activeJobs, err := client.LRange(producer.queue.activeJobsList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(activeJobs).To(Equal([]string{job.ID}))

				signals, err := client.LRange(producer.queue.signalList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(err).NotTo(HaveOccurred())
				Expect(signals).To(Equal([]string{"1"}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(producer.queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					_, err := producer.Perform(job)
					Expect(err).To(Equal(ErrJobAlreadyExists{
						Job: job,
					}))

					jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When an ID is not provided", func() {
				BeforeEach(func() {
					job = Job{
						Data: []byte("TestData"),
					}
				})

				It("Generates one", func() {
					id, err := producer.Perform(job)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.ID).To(Equal(""))

					_, err = uuid.FromString(id)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					_, err := producer.Perform(job)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})

	Describe("Redis operations", func() {
		var producer *Producer
		var job Job
		var ctx context.Context

		BeforeEach(func() {
			producer = NewProducer(&ProducerOpts{
				Client: client,
				Queue:  queue,
				Logger: &EmptyLogger{},
			})

			ctx = context.Background()
			job = Job{
				ID:   "TestID",
				Data: []byte("TestData"),
			}
		})

		Describe("pushJob", func() {
			It("Creates a new job and enqueues it", func() {
				id, err := producer.pushJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(job.ID))

				jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 211, 0, 0, 0, 0, 0, 0, 0, 0}))

				activeJobs, err := client.LRange(producer.queue.activeJobsList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(activeJobs).To(Equal([]string{job.ID}))

				signals, err := client.LRange(producer.queue.signalList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(signals).To(Equal([]string{"1"}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(producer.queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					_, err := producer.pushJob(ctx, job)
					Expect(err).To(Equal(ErrJobAlreadyExists{
						Job: job,
					}))

					jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))

					signals, err := client.LRange(producer.queue.signalList, 0, -1).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(signals).To(BeEmpty())
				})
			})

			Context("When an ID is not provided", func() {
				BeforeEach(func() {
					job = Job{
						Data: []byte("TestData"),
					}
				})

				It("Generates one", func() {
					id, err := producer.pushJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.ID).To(Equal(""))

					_, err = uuid.FromString(id)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					_, err := producer.pushJob(ctx, job)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("scheduleJob", func() {
			var at time.Time

			BeforeEach(func() {
				at = time.Now()
			})

			It("Creates a new job and schedules it", func() {
				id, err := producer.scheduleJob(ctx, at, job)
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(job.ID))

				jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 211, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRange(producer.queue.scheduledJobsSet, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(scheduledJobs).To(Equal([]string{job.ID}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(producer.queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					_, err := producer.scheduleJob(ctx, at, job)
					Expect(err).To(Equal(ErrJobAlreadyExists{
						Job: job,
					}))

					jobData, err := client.HGet(producer.queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When an ID is not provided", func() {
				BeforeEach(func() {
					job = Job{
						Data: []byte("TestData"),
					}
				})

				It("Generates one", func() {
					id, err := producer.scheduleJob(ctx, at, job)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.ID).To(Equal(""))

					_, err = uuid.FromString(id)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					_, err := producer.scheduleJob(ctx, at, job)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
