package kafka

//import (
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//
//	"github.com/batchcorp/plumber/options"
//)
//
//var _ = Describe("Kafka", func() {
//	Context("getAuthenticationMechanism", func() {
//		It("Returns nil when no username/password is specified", func() {
//			opts := &options.Options{Kafka: &options.KafkaOptions{
//				Username: "",
//				Password: "",
//			}}
//
//			m, err := getAuthenticationMechanism(opts)
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(m).To(BeNil())
//		})
//
//		It("Returns SCRAM mechanism", func() {
//			opts := &options.Options{Kafka: &options.KafkaOptions{
//				Username:           "testing",
//				Password:           "hunter2",
//				AuthenticationType: "scram",
//			}}
//
//			m, err := getAuthenticationMechanism(opts)
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(m).ToNot(BeNil())
//			Expect(m.Name()).To(Equal("SCRAM-SHA-512"))
//		})
//
//		It("Returns Plain mechanism", func() {
//			opts := &options.Options{Kafka: &options.KafkaOptions{
//				Username:           "testing",
//				Password:           "hunter2",
//				AuthenticationType: "plain",
//			}}
//
//			m, err := getAuthenticationMechanism(opts)
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(m).ToNot(BeNil())
//			Expect(m.Name()).To(Equal("PLAIN"))
//		})
//	})
//})
