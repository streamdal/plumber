package awskinesis_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwskinesis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Awskinesis Suite")
}
