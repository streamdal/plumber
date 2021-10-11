package awssns_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwssns(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Awssns Suite")
}
