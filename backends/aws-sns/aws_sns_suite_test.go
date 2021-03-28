package awssns_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwsSns(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AwsSns Suite")
}
