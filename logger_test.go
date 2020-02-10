package curlyq

import (
	"bytes"
	"log"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Logger", func() {
	var logger Logger
	var output *bytes.Buffer

	msg := "Test message"
	paramOne := "The Answer"
	paramTwo := "Forty-Two"

	BeforeEach(func() {
		output = bytes.NewBuffer([]byte{})
		log.SetOutput(output)
	})

	AfterEach(func() {
		log.SetOutput(os.Stderr)
	})

	Describe("DefaultLogger", func() {
		BeforeEach(func() {
			logger = &DefaultLogger{}
		})

		Describe("Debug", func() {
			It("Does not write to the standard log", func() {
				logger.Debug(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(Equal(""))
			})
		})

		Describe("Info", func() {
			It("Writes to the standard log", func() {
				logger.Info(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})

		Describe("Warn", func() {
			It("Writes to the standard log", func() {
				logger.Warn(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})

		Describe("Error", func() {
			It("Writes to the standard log", func() {
				logger.Error(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})
	})

	Describe("EmptyLogger", func() {
		BeforeEach(func() {
			logger = &EmptyLogger{}
		})

		Describe("Debug", func() {
			It("Does not write to the standard log", func() {
				logger.Debug(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(Equal(""))
			})
		})

		Describe("Info", func() {
			It("Writes to the standard log", func() {
				logger.Info(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(Equal(""))
			})
		})

		Describe("Warn", func() {
			It("Writes to the standard log", func() {
				logger.Warn(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(Equal(""))
			})
		})

		Describe("Error", func() {
			It("Writes to the standard log", func() {
				logger.Error(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(Equal(""))
			})
		})
	})

	Describe("LoudLogger", func() {
		BeforeEach(func() {
			logger = &LoudLogger{}
		})

		Describe("Debug", func() {
			It("Writes to the standard log", func() {
				logger.Debug(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})

		Describe("Info", func() {
			It("Writes to the standard log", func() {
				logger.Info(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})

		Describe("Warn", func() {
			It("Writes to the standard log", func() {
				logger.Warn(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})

		Describe("Error", func() {
			It("Writes to the standard log", func() {
				logger.Error(msg, paramOne, paramTwo)

				result := output.String()
				Expect(result).To(ContainSubstring(msg))
				Expect(result).To(ContainSubstring(paramOne))
				Expect(result).To(ContainSubstring(paramTwo))
			})
		})
	})
})
