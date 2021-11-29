package rstreams_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRstreams(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rstreams Suite")
}
