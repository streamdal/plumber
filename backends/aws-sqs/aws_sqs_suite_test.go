package awssqs_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwsSqs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AwsSqs Suite")
}
