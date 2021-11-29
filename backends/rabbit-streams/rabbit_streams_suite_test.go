package rabbit_streams_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRabbitStreams(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RabbitStreams Suite")
}
