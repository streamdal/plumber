package server

import (
	"archive/zip"
	"context"
	"encoding/base64"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/github/githubfakes"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

const (
	// schemas/fakes/sample-protos/batchcorp-schemas-9789dfg70s980fdsfs/test/sample-message.proto
	GithubZipFile = "UEsDBBQAAAAAAKqDEFMAAAAAAAAAAAAAAAAlACAAYmF0Y2hjb3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL1VUD" +
		"QAHoMoaYaHKGmGgyhphdXgLAAEE9QEAAAQUAAAAUEsDBBQAAAAAAI+DEFMAAAAAAAAAAAAAAAAqACAAYmF0Y2hjb3JwLXNjaG" +
		"VtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL3Rlc3QvVVQNAAduyhphoMoaYaDKGmF1eAsAAQT1AQAABBQAAABQSwMEFAAIAAgATHU" +
		"0UQAAAAAAAAAATgAAAD4AIABiYXRjaGNvcnAtc2NoZW1hcy05Nzg5ZGZnNzBzOTgwZmRzZnMvdGVzdC9zYW1wbGUtbWVzc2Fn" +
		"ZS5wcm90b1VUDQAHkaJnX2/KGmFuyhphdXgLAAEE9QEAAAQUAAAAK67MK0msULBVUCooyi/JN1ay5uIqSEzOTkxPVShOzC3IS" +
		"QUK5KYWF4MEfKF0NZcCEBSXFGXmpSukJJYkAvUbWnPVcgEAUEsHCK9KTetFAAAATgAAAFBLAQIUAxQAAAAAAKqDEFMAAAAAAA" +
		"AAAAAAAAAlACAAAAAAAAAAAADtQQAAAABiYXRjaGNvcnAtc2NoZW1hcy05Nzg5ZGZnNzBzOTgwZmRzZnMvVVQNAAegyhphoco" +
		"aYaDKGmF1eAsAAQT1AQAABBQAAABQSwECFAMUAAAAAACPgxBTAAAAAAAAAAAAAAAAKgAgAAAAAAAAAAAA7UFjAAAAYmF0Y2hj" +
		"b3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL3Rlc3QvVVQNAAduyhphoMoaYaDKGmF1eAsAAQT1AQAABBQAAABQSwECF" +
		"AMUAAgACABMdTRRr0pN60UAAABOAAAAPgAgAAAAAAAAAAAApIHLAAAAYmF0Y2hjb3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MG" +
		"Zkc2ZzL3Rlc3Qvc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ19vyhphbsoaYXV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAwA" +
		"DAHcBAACcAQAAAAA="

	// schemas/fakes/sample-protos/sample-message.proto
	LocalZipfile = "UEsDBBQACAAIAEx1NFEAAAAAAAAAAE4AAAAUACAAc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ1/BlflgkaJnX" +
		"3V4CwABBPUBAAAEFAAAACuuzCtJrFCwVVAqKMovyTdWsubiKkhMzk5MT1UoTswtyEkFCuSmFheDBHyhdDWXAhAUlxRl5qUrp" +
		"CSWJAL1G1pz1XIBAFBLBwivSk3rRQAAAE4AAABQSwECFAMUAAgACABMdTRRr0pN60UAAABOAAAAFAAgAAAAAAAAAAAApIEAA" +
		"AAAc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ1/BlflgkaJnX3V4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAGIAAACnA" +
		"AAAAAA="

	AvroSchema = "ewogICJ0eXBlIjogInJlY29yZCIsCiAgIm5hbWVzcGFjZSI6ICJjb20uZXhhbXBsZSIsCiAgIm5hbWUiOiAiQ29tcG" +
		"FuaWVzIiwKICAiZmllbGRzIjogWwogICAgeyAibmFtZSI6ICJjb21wYW55IiwgInR5cGUiOiAic3RyaW5nIiB9CiAgXQp9"

	JSONSchema = "ewogICIkaWQiOiAiaHR0cHM6Ly9leGFtcGxlLmNvbS9wZXJzb24uc2NoZW1hLmpzb24iLAogICIkc2NoZW1hIjog" +
		"Imh0dHBzOi8vanNvbi1zY2hlbWEub3JnL2RyYWZ0LzIwMjAtMTIvc2NoZW1hIiwKICAidGl0bGUiOiAiUGVyc29uIiwKICAid" +
		"HlwZSI6ICJvYmplY3QiLAogICJwcm9wZXJ0aWVzIjogewogICAgImZpcnN0TmFtZSI6IHsKICAgICAgInR5cGUiOiAic3RyaW" +
		"5nIiwKICAgICAgImRlc2NyaXB0aW9uIjogIlRoZSBwZXJzb24ncyBmaXJzdCBuYW1lLiIKICAgIH0sCiAgICAibGFzdE5hbWU" +
		"iOiB7CiAgICAgICJ0eXBlIjogInN0cmluZyIsCiAgICAgICJkZXNjcmlwdGlvbiI6ICJUaGUgcGVyc29uJ3MgbGFzdCBuYW1l" +
		"LiIKICAgIH0sCiAgICAiYWdlIjogewogICAgICAiZGVzY3JpcHRpb24iOiAiQWdlIGluIHllYXJzIHdoaWNoIG11c3QgYmUgZ" +
		"XF1YWwgdG8gb3IgZ3JlYXRlciB0aGFuIHplcm8uIiwKICAgICAgInR5cGUiOiAiaW50ZWdlciIsCiAgICAgICJtaW5pbXVtIj" +
		"ogMAogICAgfQogIH0KfQo"
)

var _ = Describe("Schema Import", func() {
	Context("importLocal", func() {
		It("returns error on unknown encoding type", func() {
			p := &Server{}

			_, err := p.importLocal(&protos.ImportLocalRequest{
				Auth: &common.Auth{Token: "batchcorp"},
				Name: "testing",
				Type: 99,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown schema type"))
		})
	})

	Context("importLocalProtobuf", func() {
		It("returns error on zip file parsing failure", func() {
			req := &protos.ImportLocalRequest{
				Auth:         &common.Auth{Token: "batchcorp"},
				Name:         "testing",
				Type:         0,
				FileName:     "testing.zip",
				FileContents: []byte(`1`),
				Settings: &protos.ImportLocalRequest_ProtobufSettings{
					ProtobufSettings: &encoding.ProtobufSettings{
						ProtobufRootMessage: "events.Message",
					},
				},
			}

			_, err := importLocalProtobuf(req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(zip.ErrFormat.Error()))
		})

		It("parses schema", func() {

			zipData, err := base64.StdEncoding.DecodeString(LocalZipfile)
			Expect(err).ToNot(HaveOccurred())

			req := &protos.ImportLocalRequest{
				Auth:         &common.Auth{Token: "batchcorp"},
				Name:         "testing",
				Type:         protos.SchemaType_SCHEMA_TYPE_PROTOBUF,
				FileName:     "testing.zip",
				FileContents: zipData,
				Settings: &protos.ImportLocalRequest_ProtobufSettings{
					ProtobufSettings: &encoding.ProtobufSettings{
						ProtobufRootMessage: "sample.Message",
					},
				},
			}

			schema, err := importLocalProtobuf(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(schema.GetProtobufSettings().XMessageDescriptor).ToNot(BeEmpty())
			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
		})
	})

	Context("importGithubProtobuf", func() {
		It("parses schema", func() {

			zipData, err := base64.StdEncoding.DecodeString(GithubZipFile)
			Expect(err).ToNot(HaveOccurred())

			fakeGithub := &githubfakes.FakeIGithub{}
			fakeGithub.GetRepoArchiveStub = func(context.Context, string, string) ([]byte, error) {
				return zipData, nil
			}

			s := &Server{
				GithubService:    fakeGithub,
				PersistentConfig: &config.Config{},
			}

			req := &protos.ImportGithubRequest{
				Auth: &common.Auth{Token: "batchcorp"},
				Name: "testing",
				Type: protos.SchemaType_SCHEMA_TYPE_PROTOBUF,
				Settings: &protos.ImportGithubRequest_ProtobufSettings{
					ProtobufSettings: &encoding.ProtobufSettings{
						XProtobufRootDir:    "test",
						ProtobufRootMessage: "sample.Message",
					},
				},
			}

			schema, err := s.importGithubProtobuf(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(schema.GetProtobufSettings().XMessageDescriptor).ToNot(BeEmpty())
			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
		})
	})

	Context("importGithubAvro", func() {
		It("returns a schema", func() {
			fakeGithub := &githubfakes.FakeIGithub{}
			fakeGithub.GetRepoFileStub = func(context.Context, string, string) ([]byte, string, error) {
				data, _ := base64.StdEncoding.DecodeString(AvroSchema)
				return data, "test.avsc", nil
			}

			s := &Server{
				GithubService:    fakeGithub,
				PersistentConfig: &config.Config{},
			}

			req := &protos.ImportGithubRequest{
				Auth:      &common.Auth{Token: "batchcorp"},
				Name:      "testing",
				Type:      protos.SchemaType_SCHEMA_TYPE_AVRO,
				GithubUrl: "https://github.com/batchcorp/avro-test/test.avsc",
			}

			schema, err := s.importGithubAvro(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(schema.GetAvroSettings().Schema).ToNot(BeEmpty())
			Expect(schema.Files["test.avsc"]).ToNot(BeEmpty())
		})
	})

	Context("importGithubJSONSchema", func() {
		It("returns a schema", func() {
			fakeGithub := &githubfakes.FakeIGithub{}
			fakeGithub.GetRepoFileStub = func(context.Context, string, string) ([]byte, string, error) {
				data, _ := base64.StdEncoding.DecodeString(JSONSchema)
				return data, "test.json", nil
			}

			s := &Server{
				GithubService:    fakeGithub,
				PersistentConfig: &config.Config{},
			}

			req := &protos.ImportGithubRequest{
				Auth:      &common.Auth{Token: "batchcorp"},
				Name:      "testing",
				Type:      protos.SchemaType_SCHEMA_TYPE_JSONSCHEMA,
				GithubUrl: "https://github.com/batchcorp/jsonschema-test/test.json",
			}

			schema, err := s.importGithubJSONSchema(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			Expect(schema.GetJsonSchemaSettings().Schema).ToNot(BeEmpty())
			Expect(schema.Files["test.json"]).ToNot(BeEmpty())
		})
	})

	Context("importGithub", func() {
		It("returns error on invalid schema type", func() {
			p := &Server{
				GithubService:    &githubfakes.FakeIGithub{},
				PersistentConfig: &config.Config{},
			}

			_, err := p.importGithub(context.Background(), &protos.ImportGithubRequest{
				Type: protos.SchemaType_SCHEMA_TYPE_UNSET,
			})

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrInvalidGithubSchemaType))
		})

		It("returns a schema", func() {
			fakeGithub := &githubfakes.FakeIGithub{}
			fakeGithub.GetRepoArchiveStub = func(context.Context, string, string) ([]byte, error) {
				return base64.StdEncoding.DecodeString(GithubZipFile)
			}

			p := &Server{
				PersistentConfig: &config.Config{},
				GithubService:    fakeGithub,
			}

			schema, err := p.importGithub(context.Background(), &protos.ImportGithubRequest{
				Type: protos.SchemaType_SCHEMA_TYPE_PROTOBUF,
				Settings: &protos.ImportGithubRequest_ProtobufSettings{
					ProtobufSettings: &encoding.ProtobufSettings{
						XProtobufRootDir:    "test",
						ProtobufRootMessage: "sample.Message",
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(schema.GetProtobufSettings().XMessageDescriptor).ToNot(BeEmpty())
			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
		})
	})
})
