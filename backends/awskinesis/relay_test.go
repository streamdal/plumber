package awskinesis

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/types"
)

var _ = Describe("AWS Kinesis Backend", func() {
	Context("Relay", func() {
		It("returns not implemented error", func() {
			err := (&Kinesis{}).Relay(nil, nil, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(types.NotImplementedErr))
		})
	})
})
