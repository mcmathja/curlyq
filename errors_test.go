package curlyq

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Errors", func() {
	wrappedErr := errors.New("Wrapped error description")
	job := Job{
		ID: "test-job",
	}

	Describe("ErrFailedToRetryJob", func() {
		It("Wraps an error and a job", func() {
			err := ErrFailedToRetryJob{
				Err: wrappedErr,
				Job: job,
			}

			expectedMsg := "Failed to retry job test-job: Wrapped error description"
			Expect(err.Error()).To(Equal(expectedMsg))
		})
	})

	Describe("ErrFailedToKillJob", func() {
		It("Wraps an error and a job", func() {
			err := ErrFailedToKillJob{
				Err: wrappedErr,
				Job: job,
			}

			expectedMsg := "Failed to kill job test-job: Wrapped error description"
			Expect(err.Error()).To(Equal(expectedMsg))
		})
	})

	Describe("ErrFailedToAckJob", func() {
		It("Wraps an error and a job", func() {
			err := ErrFailedToAckJob{
				Err: wrappedErr,
				Job: job,
			}

			expectedMsg := "Failed to acknowledge job test-job: Wrapped error description"
			Expect(err.Error()).To(Equal(expectedMsg))
		})
	})
})
