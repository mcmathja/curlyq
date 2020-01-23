package curlyq

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCurlyq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "curlyq")
}
