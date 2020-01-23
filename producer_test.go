package curlyq

import (
	"context"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer", func() {
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
				Expect(producer.opts.Client).To(Equal(client))
				Expect(producer.opts.Queue).To(Equal(queue))
			})

			It("Loads all of the expected Lua scripts", func() {
				Expect(producer.pushJobScript).NotTo(BeNil())
				Expect(producer.scheduleJobScript).NotTo(BeNil())
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
		var job *Job

		BeforeEach(func() {
			producer = NewProducer(&ProducerOpts{
				Client: client,
				Queue:  queue,
			})

			job = &Job{
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
				err := producer.PerformAfter(job, duration)
				Expect(err).NotTo(HaveOccurred())

				jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRangeWithScores(queue.scheduledJobsSet, 0, -1).Result()
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
					err := client.HSet(queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					err := producer.PerformAfter(job, duration)
					Expect(err).NotTo(HaveOccurred())

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					err := producer.PerformAfter(job, duration)
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
				err := producer.PerformAt(job, moment)
				Expect(err).NotTo(HaveOccurred())

				jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRangeWithScores(queue.scheduledJobsSet, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(scheduledJobs)).To(Equal(1))
				Expect(scheduledJobs[0].Member).To(Equal(job.ID))
				Expect(scheduledJobs[0].Score).To(Equal(float64(moment.Unix())))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					err := producer.PerformAt(job, moment)
					Expect(err).NotTo(HaveOccurred())

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					err := producer.PerformAt(job, moment)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("PerformNow", func() {
			It("Creates a new job and schedules it to run after the provided duration", func() {
				err := producer.PerformNow(job)
				Expect(err).NotTo(HaveOccurred())

				jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 0}))

				activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(activeJobs).To(Equal([]string{job.ID}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					err := producer.PerformNow(job)
					Expect(err).NotTo(HaveOccurred())

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					err := producer.PerformNow(job)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})

	Describe("Redis operations", func() {
		Describe("pushJob", func() {
			var producer *Producer
			var job *Job
			var ctx context.Context

			BeforeEach(func() {
				producer = NewProducer(&ProducerOpts{
					Client: client,
					Queue:  queue,
				})

				ctx = context.Background()
				job = &Job{
					ID:   "TestID",
					Data: []byte("TestData"),
				}
			})

			It("Creates a new job and enqueues it", func() {
				err := producer.pushJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 0}))

				activeJobs, err := client.LRange(queue.activeJobsList, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(activeJobs).To(Equal([]string{job.ID}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					err := producer.pushJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					err := producer.pushJob(ctx, job)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Describe("scheduleJob", func() {
			var producer *Producer
			var job *Job
			var ctx context.Context
			var at time.Time

			BeforeEach(func() {
				producer = NewProducer(&ProducerOpts{
					Client: client,
					Queue:  queue,
				})

				ctx = context.Background()
				job = &Job{
					ID:   "TestID",
					Data: []byte("TestData"),
				}
				at = time.Now()
			})

			It("Creates a new job and schedules it", func() {
				err := producer.scheduleJob(ctx, job, at)
				Expect(err).NotTo(HaveOccurred())

				jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(jobData)).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 0}))

				scheduledJobs, err := client.ZRange(queue.scheduledJobsSet, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(scheduledJobs).To(Equal([]string{job.ID}))
			})

			Context("When a job with that ID already exists", func() {
				BeforeEach(func() {
					err := client.HSet(queue.jobDataHash, job.ID, "Preexisting Data").Err()
					Expect(err).NotTo(HaveOccurred())
				})

				It("Does not overwrite the existing job data", func() {
					err := producer.scheduleJob(ctx, job, at)
					Expect(err).NotTo(HaveOccurred())

					jobData, err := client.HGet(queue.jobDataHash, job.ID).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(jobData).To(Equal("Preexisting Data"))
				})
			})

			Context("When redis is unavailable", func() {
				BeforeEach(func() {
					server.Close()
				})

				It("Returns an error", func() {
					err := producer.scheduleJob(ctx, job, at)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
