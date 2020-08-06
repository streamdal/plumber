package rabbitmq

import (
	. "github.com/onsi/ginkgo"
)

var _ = Describe("Read", func() {
	Context("Read", func() {
		var ()

		BeforeEach(func() {
		})

		It("happy path: read plain message", func() {
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
