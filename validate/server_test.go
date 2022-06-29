package validate

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Validate Server", func() {

	Context("ConnectionOptionsForServer", func() {
		It("validates missing conn options", func() {
			err := ConnectionOptionsForServer(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnectionOptions))
		})

		It("validates missing name", func() {
			err := ConnectionOptionsForServer(&opts.ConnectionOptions{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnName))
		})

		It("validates missing bus config", func() {
			err := ConnectionOptionsForServer(&opts.ConnectionOptions{
				Name: "testing",
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnectionType))
		})
	})

	Context("RelayOptionsForServer", func() {
		It("validates missing conn options", func() {
			err := RelayOptionsForServer(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingRelayOptions))
		})

		It("validates missing collection token", func() {
			err := RelayOptionsForServer(&opts.RelayOptions{
				CollectionToken: "",
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCollectionToken))
		})

		It("validates missing connection ID", func() {
			err := RelayOptionsForServer(&opts.RelayOptions{
				ConnectionId:    "",
				CollectionToken: "TOKEN",
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnectionId))
		})

		It("falls back to the default GRPCCollectorAddress and GRPCTimeout if none is given", func() {
			relayOptions := &opts.RelayOptions{
				XBatchshGrpcAddress: "",
				CollectionToken:     "TOKEN",
				ConnectionId:        "ConnId",
			}
			err := RelayOptionsForServer(relayOptions)

			Expect(err).ToNot(HaveOccurred())
			Expect(relayOptions.XBatchshGrpcAddress).To(Equal("grpc-collector.batch.sh:9000"))
			Expect(relayOptions.XBatchshGrpcTimeoutSeconds).To(BeEquivalentTo(5))
		})

	})
})
