package github_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGithub(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Github Suite")
}
