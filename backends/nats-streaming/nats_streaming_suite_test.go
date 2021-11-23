package nats_streaming_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestNatsStreaming(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NatsStreaming Suite")
}
