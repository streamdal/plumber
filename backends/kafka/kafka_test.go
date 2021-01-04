package kafka

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/cli"
)

var _ = Describe("Kafka", func() {
	Context("getAuthenticationMechanism", func() {
		It("Returns nil when no username/password is specified", func() {
			opts := &cli.Options{Kafka: &cli.KafkaOptions{
				Username: "",
				Password: "",
			}}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).To(BeNil())
		})

		It("Returns SCRAM mechanism", func() {
			opts := &cli.Options{Kafka: &cli.KafkaOptions{
				Username:           "testing",
				Password:           "hunter2",
				AuthenticationType: "scram",
			}}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
			Expect(m.Name()).To(Equal("SCRAM-SHA-512"))
		})

		It("Returns Plain mechanism", func() {
			opts := &cli.Options{Kafka: &cli.KafkaOptions{
				Username:           "testing",
				Password:           "hunter2",
				AuthenticationType: "plain",
			}}

			m, err := getAuthenticationMechanism(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
			Expect(m.Name()).To(Equal("PLAIN"))
		})
	})
})
