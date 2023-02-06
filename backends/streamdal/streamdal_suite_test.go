package streamdal

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

func TestAPISuite(t *testing.T) {
	logrus.SetLevel(logrus.FatalLevel)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Streamdal Test Suite")
}
