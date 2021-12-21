package kafka

import (
	"encoding/binary"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
)

var _ = Describe("Kafka", func() {
	Context("getAuthenticationMechanism", func() {
		It("Returns nil when no username/password is specified", func() {
			opts := &args.KafkaConn{
				SaslUsername: "",
				SaslPassword: "",
			}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).To(BeNil())
		})

		It("Returns SCRAM mechanism", func() {
			opts := &args.KafkaConn{
				SaslUsername: "testing",
				SaslPassword: "hunter2",
				SaslType:     args.SASLType_SCRAM,
			}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
			Expect(m.Name()).To(Equal("SCRAM-SHA-512"))
		})

		It("Returns Plain mechanism", func() {
			opts := &args.KafkaConn{
				SaslUsername: "testing",
				SaslPassword: "hunter2",
				SaslType:     args.SASLType_PLAIN,
			}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
			Expect(m.Name()).To(Equal("PLAIN"))
		})
	})

	Context("convertKafkaHeaders", func() {
		It("handles values which cannot be converted to utf8 string", func() {
			var ts [8]byte
			binary.LittleEndian.PutUint64(ts[:], uint64(1640101168))

			headers := []skafka.Header{
				{Key: "key", Value: ts[:]},
			}

			converted := convertKafkaHeadersToProto(headers)
			Expect(len(converted)).To(Equal(1))
			Expect(converted[0].Value).To(Equal("MPXBYQAAAAA="))
		})
	})
})
