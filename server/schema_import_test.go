package server

//import (
//	"archive/zip"
//	"context"
//	"encoding/base64"
//
//	"github.com/batchcorp/plumber/config"
//
//	"github.com/batchcorp/plumber/github/githubfakes"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//
//	"github.com/batchcorp/plumber-schemas/build/go/protos"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
//)
//
//const (
//	// schemas/fakes/sample-protos/batchcorp-schemas-9789dfg70s980fdsfs/test/sample-message.proto
//	GithubZipFile = "UEsDBBQAAAAAAKqDEFMAAAAAAAAAAAAAAAAlACAAYmF0Y2hjb3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL1VUD" +
//		"QAHoMoaYaHKGmGgyhphdXgLAAEE9QEAAAQUAAAAUEsDBBQAAAAAAI+DEFMAAAAAAAAAAAAAAAAqACAAYmF0Y2hjb3JwLXNjaG" +
//		"VtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL3Rlc3QvVVQNAAduyhphoMoaYaDKGmF1eAsAAQT1AQAABBQAAABQSwMEFAAIAAgATHU" +
//		"0UQAAAAAAAAAATgAAAD4AIABiYXRjaGNvcnAtc2NoZW1hcy05Nzg5ZGZnNzBzOTgwZmRzZnMvdGVzdC9zYW1wbGUtbWVzc2Fn" +
//		"ZS5wcm90b1VUDQAHkaJnX2/KGmFuyhphdXgLAAEE9QEAAAQUAAAAK67MK0msULBVUCooyi/JN1ay5uIqSEzOTkxPVShOzC3IS" +
//		"QUK5KYWF4MEfKF0NZcCEBSXFGXmpSukJJYkAvUbWnPVcgEAUEsHCK9KTetFAAAATgAAAFBLAQIUAxQAAAAAAKqDEFMAAAAAAA" +
//		"AAAAAAAAAlACAAAAAAAAAAAADtQQAAAABiYXRjaGNvcnAtc2NoZW1hcy05Nzg5ZGZnNzBzOTgwZmRzZnMvVVQNAAegyhphoco" +
//		"aYaDKGmF1eAsAAQT1AQAABBQAAABQSwECFAMUAAAAAACPgxBTAAAAAAAAAAAAAAAAKgAgAAAAAAAAAAAA7UFjAAAAYmF0Y2hj" +
//		"b3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MGZkc2ZzL3Rlc3QvVVQNAAduyhphoMoaYaDKGmF1eAsAAQT1AQAABBQAAABQSwECF" +
//		"AMUAAgACABMdTRRr0pN60UAAABOAAAAPgAgAAAAAAAAAAAApIHLAAAAYmF0Y2hjb3JwLXNjaGVtYXMtOTc4OWRmZzcwczk4MG" +
//		"Zkc2ZzL3Rlc3Qvc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ19vyhphbsoaYXV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAwA" +
//		"DAHcBAACcAQAAAAA="
//
//	// schemas/fakes/sample-protos/sample-message.proto
//	LocalZipfile = "UEsDBBQACAAIAEx1NFEAAAAAAAAAAE4AAAAUACAAc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ1/BlflgkaJnX" +
//		"3V4CwABBPUBAAAEFAAAACuuzCtJrFCwVVAqKMovyTdWsubiKkhMzk5MT1UoTswtyEkFCuSmFheDBHyhdDWXAhAUlxRl5qUrp" +
//		"CSWJAL1G1pz1XIBAFBLBwivSk3rRQAAAE4AAABQSwECFAMUAAgACABMdTRRr0pN60UAAABOAAAAFAAgAAAAAAAAAAAApIEAA" +
//		"AAAc2FtcGxlLW1lc3NhZ2UucHJvdG9VVA0AB5GiZ1/BlflgkaJnX3V4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAGIAAACnA" +
//		"AAAAAA="
//)
//
//var _ = Describe("Schema Import", func() {
//	Context("importLocal", func() {
//		It("returns error on unknown encoding type", func() {
//			p := &Server{}
//
//			_, err := p.importLocal(&protos.ImportLocalRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Name: "testing",
//				Type: 99,
//			})
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring("unknown encoding type"))
//		})
//	})
//
//	Context("importLocalProtobuf", func() {
//		It("returns error on zip file parsing failure", func() {
//			req := &protos.ImportLocalRequest{
//				Auth:       &common.Auth{Token: "batchcorp"},
//				Name:       "testing",
//				Type:       0,
//				ZipArchive: []byte(`1`),
//				RootType:   "events.Message",
//			}
//
//			_, err := importLocalProtobuf(req)
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(zip.ErrFormat.Error()))
//		})
//
//		It("parses schema", func() {
//
//			zipData, err := base64.StdEncoding.DecodeString(LocalZipfile)
//			Expect(err).ToNot(HaveOccurred())
//
//			req := &protos.ImportLocalRequest{
//				Auth:       &common.Auth{Token: "batchcorp"},
//				Name:       "testing",
//				Type:       encoding.Type_PROTOBUF,
//				ZipArchive: zipData,
//				RootType:   "sample.Message",
//			}
//
//			schema, err := importLocalProtobuf(req)
//			Expect(err).ToNot(HaveOccurred())
//			Expect(schema.MessageDescriptor).ToNot(BeEmpty())
//			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
//		})
//	})
//
//	Context("importGithubProtobuf", func() {
//		It("parses schema", func() {
//
//			zipData, err := base64.StdEncoding.DecodeString(GithubZipFile)
//			Expect(err).ToNot(HaveOccurred())
//
//			req := &protos.ImportGithubRequest{
//				Auth:     &common.Auth{Token: "batchcorp"},
//				Name:     "testing",
//				Type:     encoding.Type_PROTOBUF,
//				RootType: "sample.Message",
//				RootDir:  "test",
//			}
//
//			schema, err := importGithubProtobuf(zipData, req)
//			Expect(err).ToNot(HaveOccurred())
//			Expect(schema.MessageDescriptor).ToNot(BeEmpty())
//			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
//		})
//	})
//
//	Context("importGithub", func() {
//		It("returns error on invalid schema type", func() {
//			p := &Server{
//				GithubService:    &githubfakes.FakeIGithub{},
//				PersistentConfig: &config.Config{},
//			}
//
//			_, err := p.importGithub(context.Background(), &protos.ImportGithubRequest{
//				Type: encoding.Type_JSON,
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err).To(Equal(ErrInvalidGithubSchemaType))
//		})
//
//		It("returns a schema", func() {
//			fakeGithub := &githubfakes.FakeIGithub{}
//			fakeGithub.GetRepoArchiveStub = func(context.Context, string, string) ([]byte, error) {
//				return base64.StdEncoding.DecodeString(GithubZipFile)
//			}
//
//			p := &Server{
//				PersistentConfig: &config.Config{},
//				GithubService:    fakeGithub,
//			}
//
//			schema, err := p.importGithub(context.Background(), &protos.ImportGithubRequest{
//				Type:     encoding.Type_PROTOBUF,
//				RootType: "sample.Message",
//				RootDir:  "test",
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(schema.MessageDescriptor).ToNot(BeEmpty())
//			Expect(schema.Files["sample-message.proto"]).ToNot(BeEmpty())
//		})
//	})
//})
