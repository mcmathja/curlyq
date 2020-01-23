package curlyq

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queue", func() {
	Describe("NewQueue", func() {
		var queue *Queue

		Context("When provided with a name", func() {
			BeforeEach(func() {
				queue = NewQueue(&QueueOpts{
					Name: "name",
				})
			})

			It("Sets the correct path values", func() {
				Expect(queue.activeJobsList).To(Equal("curlyq:name:active"))
				Expect(queue.consumersSet).To(Equal("curlyq:name:consumers"))
				Expect(queue.deadJobsSet).To(Equal("curlyq:name:dead"))
				Expect(queue.inflightJobsPrefix).To(Equal("curlyq:name:inflight"))
				Expect(queue.jobDataHash).To(Equal("curlyq:name:data"))
				Expect(queue.scheduledJobsSet).To(Equal("curlyq:name:scheduled"))
			})
		})

		Context("When provided with a name and a prefix", func() {
			BeforeEach(func() {
				queue = NewQueue(&QueueOpts{
					Name:   "name",
					Prefix: "prefix",
				})
			})

			It("Sets the correct path values", func() {
				Expect(queue.activeJobsList).To(Equal("prefix:curlyq:name:active"))
				Expect(queue.consumersSet).To(Equal("prefix:curlyq:name:consumers"))
				Expect(queue.deadJobsSet).To(Equal("prefix:curlyq:name:dead"))
				Expect(queue.inflightJobsPrefix).To(Equal("prefix:curlyq:name:inflight"))
				Expect(queue.jobDataHash).To(Equal("prefix:curlyq:name:data"))
				Expect(queue.scheduledJobsSet).To(Equal("prefix:curlyq:name:scheduled"))
			})
		})

		Context("When not provided with a name", func() {
			It("Panics", func() {
				Expect(func() {
					NewQueue(&QueueOpts{})
				}).To(Panic())
			})
		})
	})
})
