package gcppubsub_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGcpPubsub(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GcpPubsub Suite")
}
