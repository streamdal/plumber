package rabbitmq

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/app"
)

var _ = Describe("Read", func() {
	Context("Read", func() {
		var (
			a *cli.App
		)

		BeforeEach(func() {
			a = app.Setup()
		})

		It("happy path: read plain message", func() {
			err := a.Run([]string{"read message"})
			Expect(err).ToNot(HaveOccurred())
		})

		It("happy path: read protobuf message", func() {

		})

		It("happy path: convert (decode) output message from base64", func() {

		})

		It("happy path: gunzip output message", func() {

		})

		It("error: passing bad args", func() {

		})

		It("error: missing address", func() {

		})

		It("error: bad protobuf option combo", func() {

		})

		It("error: invalid root protobuf message", func() {

		})

		It("error: invalid protobuf dir", func() {

		})

		It("error: no .proto files", func() {

		})

		It("error: unable to connect to rabbit", func() {

		})

		It("error: unable to decode protobuf message", func() {

		})

		It("error: unable to base64 convert (decode) output message", func() {

		})

		It("error: unable to gunzip output message", func() {

		})
	})
})
