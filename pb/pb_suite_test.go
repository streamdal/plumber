package pb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pb Suite")
}
