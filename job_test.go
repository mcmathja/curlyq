package curlyq

import (
	"github.com/gofrs/uuid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Job", func() {
	var job *Job

	Describe("message", func() {
		BeforeEach(func() {
			job = &Job{
				ID:      "TestID",
				Attempt: 1,
				Data:    []byte("TestData"),
			}
		})

		It("Encodes the job data correctly", func() {
			msg, err := job.message()
			Expect(err).NotTo(HaveOccurred())
			Expect(msg).To(Equal([]byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 196, 8, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 1}))
		})

		Context("When an ID is not provided", func() {
			BeforeEach(func() {
				job = &Job{
					Data: []byte("Test"),
				}
			})

			It("Sets it to a random UUID", func() {
				job.message()
				Expect(job.ID).NotTo(Equal(""))

				_, err := uuid.FromString(job.ID)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("When an ID is provided", func() {
			BeforeEach(func() {
				job = &Job{
					Data: []byte("Test"),
					ID:   "TestID",
				}
			})

			It("Does not change it", func() {
				job.message()
				Expect(job.ID).To(Equal("TestID"))
			})
		})
	})

	Describe("FromMessage", func() {
		BeforeEach(func() {
			job = &Job{}
			Expect(job.ID).To(Equal(""))
			Expect(job.Data).To(BeNil())
			Expect(job.Attempt).To(Equal(uint(0)))
		})

		It("Decodes the job data correctly", func() {
			bytes := []byte{131, 162, 73, 68, 166, 84, 101, 115, 116, 73, 68, 164, 68, 97, 116, 97, 168, 84, 101, 115, 116, 68, 97, 116, 97, 167, 65, 116, 116, 101, 109, 112, 116, 207, 0, 0, 0, 0, 0, 0, 0, 1}
			err := job.fromMessage(bytes)
			Expect(err).NotTo(HaveOccurred())
			Expect(job.ID).To(Equal("TestID"))
			Expect(job.Attempt).To(Equal(uint(1)))
			Expect(string(job.Data)).To(Equal("TestData"))
		})
	})
})
