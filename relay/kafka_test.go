package relay

import (
	"encoding/binary"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	skafka "github.com/segmentio/kafka-go"
)

var _ = Describe("Relay", func() {
	Context("convertKafkaHeaders", func() {
		It("handles values which cannot be converted to utf8 string", func() {
			var ts [8]byte
			binary.LittleEndian.PutUint64(ts[:], uint64(1640101168))

			headers := []skafka.Header{
				{Key: "key", Value: ts[:]},
			}

			converted := convertKafkaHeaders(headers)
			Expect(len(converted)).To(Equal(1))
			Expect(converted[0].Value).To(Equal("MPXBYQAAAAA="))
		})
	})
})
