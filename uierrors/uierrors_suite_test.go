package uierrors_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestUierrors(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Uierrors Suite")
}
