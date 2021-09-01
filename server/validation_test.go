package server

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/conns"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

var _ = Describe("Validation", func() {
	Context("validateConnection", func() {
		It("validates missing conn", func() {
			err := validateConnection(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnection))
		})

		It("validates missing name", func() {
			err := validateConnection(&protos.Connection{
				Conn: &protos.Connection_Kafka{
					Kafka: &conns.Kafka{},
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnName))
		})

		It("validates missing bus config", func() {
			err := validateConnection(&protos.Connection{
				Name: "testing",
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnectionType))
		})
	})

	Context("validateConnectionKafka", func() {
		It("validates missing broker address", func() {
			k := &conns.Kafka{
				Address:  nil,
				SaslType: conns.SASLType_NONE,
			}

			err := validateConnectionKafka(k)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingAddress))
		})

		It("validates missing username", func() {
			k := &conns.Kafka{
				Address:      []string{"1.2.3.4:9200"},
				SaslType:     conns.SASLType_PLAIN,
				SaslPassword: "foo",
			}

			err := validateConnectionKafka(k)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingUsername))
		})

		It("validates missing password", func() {
			k := &conns.Kafka{
				Address:      []string{"1.2.3.4:9200"},
				SaslType:     conns.SASLType_PLAIN,
				SaslUsername: "foo",
			}

			err := validateConnectionKafka(k)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingPassword))
		})
	})

	Context("validateRead", func() {
		It("validates missing read", func() {
			err := validateRead(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingRead))
		})

		It("validates missing read", func() {
			read := &protos.Read{
				Name:          "testing",
				ConnectionId:  "",
				ReadOptions:   &protos.ReadOptions{},
				SampleOptions: &protos.SampleOptions{},
				DecodeOptions: &encoding.Options{},
			}

			err := validateRead(read)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConnectionID))
		})

		It("validates missing read", func() {
			read := &protos.Read{
				Name:          "testing",
				ConnectionId:  uuid.NewV4().String(),
				ReadOptions:   &protos.ReadOptions{},
				SampleOptions: &protos.SampleOptions{},
			}

			err := validateRead(read)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingReadType))
		})
	})

	Context("validateArgsKafka", func() {
		It("validate missing kafka args", func() {
			err := validateArgsKafka(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingKafkaArgs))
		})

		It("validates missing topics", func() {
			cfg := &args.Kafka{
				Topics: nil,
			}

			err := validateArgsKafka(cfg)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingTopic))
		})

		It("validates missing consumer group name", func() {
			cfg := &args.Kafka{
				Topics:           []string{"testing"},
				UseConsumerGroup: true,
			}

			err := validateArgsKafka(cfg)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingConsumerGroupName))
		})

		It("passes validation", func() {
			cfg := &args.Kafka{
				Topics: []string{"testing"},
			}

			err := validateArgsKafka(cfg)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("validateDecodeOptions", func() {
		It("returns nil when passed nil", func() {
			err := validateDecodeOptions(nil)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("validateDecodeOptionsProtobuf", func() {
		It("returns nil when passed nil", func() {
			err := validateDecodeOptionsProtobuf(nil)
			Expect(err).ToNot(HaveOccurred())
		})

		It("validates missing root type", func() {
			opts := &encoding.Protobuf{
				ZipArchive: []byte(`1`),
			}

			err := validateDecodeOptionsProtobuf(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingRootType))
		})

		It("validates missing zip archive", func() {
			opts := &encoding.Protobuf{
				RootType: "events.Message",
			}

			err := validateDecodeOptionsProtobuf(opts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingZipArchive))
		})
	})

	Context("validateDecodeOptionsAvro", func() {
		It("returns nil when passed nil", func() {
			err := validateDecodeOptionsAvro(nil)
			Expect(err).ToNot(HaveOccurred())
		})

		It("validates missing schema", func() {
			err := validateDecodeOptionsAvro(&encoding.Avro{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingAVROSchema))
		})

		It("passes validation", func() {
			err := validateDecodeOptionsAvro(&encoding.Avro{Schema: []byte(`1`)})
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("validateService", func() {
		It("errors when passed nil", func() {
			err := validateService(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingService))
		})

		It("validates missing name", func() {
			svc := &protos.Service{
				Name:    "",
				OwnerId: uuid.NewV4().String(),
			}

			err := validateService(svc)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingName))
		})

		// TODO: uncomment when I figure out what owner should be
		//It("validates missing owner ID", func() {
		//	svc := &protos.Service{
		//		Name:    "testing",
		//		OwnerId: "",
		//	}
		//
		//	err := validateService(svc)
		//	Expect(err).To(HaveOccurred())
		//	Expect(err).To(Equal(ErrMissingOwner))
		//})

		It("validates repo url", func() {
			svc := &protos.Service{
				Name:    "testing",
				OwnerId: uuid.NewV4().String(),
				RepoUrl: "foo",
			}

			err := validateService(svc)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidRepoURL))
		})
	})
})
