package validate

import (
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

var _ = Describe("Validate CLI", func() {

	Context("ProtoBuffOptionsForCLI", func() {
		It("validates protobuf directory or Protobuf file descriptor set", func() {
			err := ProtobufOptionsForCLI([]string{}, "", "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one '--protobuf-dirs' or --protobuf-descriptor-set required when type is set to 'protobuf'"))
		})

		It("validates root message for Protobuf", func() {
			err := ProtobufOptionsForCLI([]string{"directory"}, "", "fdsFile")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("'--protobuf-root-message' required when type is set to 'protobuf'"))
		})

		It("validates existence of protobuf directory if set", func() {
			err := ProtobufOptionsForCLI([]string{"directory"}, "rootMessage", "fdsFile")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("--protobuf-dir validation error(s)"))
		})
	})

	Context("ReadOptionsForCLI", func() {
		It("validates missing read options", func() {
			err := ReadOptionsForCLI(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingReadOptions))
		})

		It("validates missing CLI options", func() {
			err := ReadOptionsForCLI(&opts.ReadOptions{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCLIOptions))
		})
	})

	Context("RelayOptionsForCLI", func() {
		It("validates missing relay options", func() {
			err := RelayOptionsForCLI(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyRelayOpts))
		})

		It("validates missing CLI options", func() {
			err := RelayOptionsForCLI(&opts.RelayOptions{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCLIOptions))
		})
	})

	Context("WriteOptionsForCLI", func() {
		It("validates missing write options", func() {
			err := WriteOptionsForCLI(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyWriteOpts))
		})

		It("validates missing CLI options", func() {
			err := WriteOptionsForCLI(&opts.WriteOptions{
				XCliOptions: nil,
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingCLIOptions))
		})

		It("validates input or input file are specified ", func() {
			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile:  "",
					InputStdin: []string{},
				},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be specified"))
		})

		It("validates that only one of input or input file are specified", func() {
			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "an input record",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: "a file",
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot both be set"))
		})

		It("validates the existence of the input file if specified", func() {
			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: "a file that does not exist",
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
		})

		It("validates Protobuf encodings if set", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
					//InputStdin: tempFile.ReadAt(0, 0),
				},
				EncodeOptions: &encoding.EncodeOptions{
					EncodeType:       encoding.EncodeType_ENCODE_TYPE_JSONPB,
					ProtobufSettings: nil,
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("protobuf settings cannot be unset if encode type is set to jsonpb"))
		})

		It("validates protobuf root message", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
					//InputStdin: tempFile.ReadAt(0, 0),
				},
				EncodeOptions: &encoding.EncodeOptions{
					EncodeType: encoding.EncodeType_ENCODE_TYPE_JSONPB,
					ProtobufSettings: &encoding.ProtobufSettings{
						ProtobufRootMessage: "",
					},
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("protobuf root message must be set if encode type is set to jsonpb"))
		})

		It("validates root director or Protobuf descriptor set if running in Protobuf mode", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
					//InputStdin: tempFile.ReadAt(0, 0),
				},

				EncodeOptions: &encoding.EncodeOptions{
					EncodeType: encoding.EncodeType_ENCODE_TYPE_JSONPB,
					ProtobufSettings: &encoding.ProtobufSettings{
						ProtobufRootMessage: "",
					},
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("protobuf root message must be set if encode type is set to jsonpb"))
		})

		It("validates root director or Protobuf descriptor set if running in Protobuf mode", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
				},

				EncodeOptions: &encoding.EncodeOptions{
					EncodeType: encoding.EncodeType_ENCODE_TYPE_JSONPB,
					ProtobufSettings: &encoding.ProtobufSettings{
						ProtobufRootMessage:   "writeOptionCLI",
						ProtobufDirs:          []string{},
						ProtobufDescriptorSet: "",
					},
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("either a protobuf directory or a descriptor set file must be specified if encode type is set to jsonpb"))
		})

		It("validates Avro encoding options are set if the encoding type is set to Avro", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
				},

				EncodeOptions: &encoding.EncodeOptions{
					EncodeType: encoding.EncodeType_ENCODE_TYPE_AVRO,
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("avro settings cannot be nil if encode type is set to avro"))
		})

		It("validates Avro schema if Avro encoding is selected", func() {
			tempFile, _ := ioutil.TempFile("", "writeOptionsCLI")

			err := WriteOptionsForCLI(&opts.WriteOptions{
				Record: &records.WriteRecord{
					Input: "",
				},
				XCliOptions: &opts.WriteCLIOptions{
					InputFile: tempFile.Name(),
				},

				EncodeOptions: &encoding.EncodeOptions{
					EncodeType:   encoding.EncodeType_ENCODE_TYPE_AVRO,
					AvroSettings: &encoding.AvroSettings{AvroSchemaFile: ""},
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("avro schema file must be specified if encode type is set to avro"))
		})

	})

})
