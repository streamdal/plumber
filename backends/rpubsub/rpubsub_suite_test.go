package rpubsub_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRpubsub(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rpubsub Suite")
}
