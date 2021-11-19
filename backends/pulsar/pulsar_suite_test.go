package pulsar_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPulsar(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pulsar Suite")
}
