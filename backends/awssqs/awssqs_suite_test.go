package awssqs_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwssqs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Awssqs Suite")
}
