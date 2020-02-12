package curlyq

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queue", func() {
	Describe("NewQueue", func() {
		var q *queue

		Context("When provided with a name", func() {
			BeforeEach(func() {
				q = newQueue(&queueOpts{
					Name: "name",
				})
			})

			It("Sets the correct path values", func() {
				Expect(q.activeJobsList).To(Equal("name:active"))
				Expect(q.consumersSet).To(Equal("name:consumers"))
				Expect(q.deadJobsSet).To(Equal("name:dead"))
				Expect(q.inflightJobsPrefix).To(Equal("name:inflight"))
				Expect(q.jobDataHash).To(Equal("name:data"))
				Expect(q.scheduledJobsSet).To(Equal("name:scheduled"))
				Expect(q.signalList).To(Equal("name:signal"))
			})
		})

		Context("When not provided with a name", func() {
			It("Panics", func() {
				Expect(func() {
					newQueue(&queueOpts{})
				}).To(Panic())
			})
		})
	})
})
