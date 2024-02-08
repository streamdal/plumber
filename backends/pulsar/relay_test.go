package pulsar

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/streamdal/plumber/types"
)

var _ = Describe("Pulsar", func() {
	Context("Relay", func() {
		It("returns not implemented error", func() {
			p := &Pulsar{}

			err := p.Relay(context.Background(), nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(types.NotImplementedErr))
		})
	})
})
