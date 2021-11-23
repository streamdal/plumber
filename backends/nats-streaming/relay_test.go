package nats_streaming

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/types"
)

var _ = Describe("Nats Streaming Backend", func() {
	Context("Relay", func() {
		It("returns not implemented error", func() {
			err := (&NatsStreaming{}).Relay(nil, nil, nil, nil)
			Expect(err).To(MatchError(types.NotImplementedErr))
		})
	})
})
