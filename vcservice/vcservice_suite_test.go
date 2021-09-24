package vcservice_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestVcservice(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Vcservice Suite")
}
