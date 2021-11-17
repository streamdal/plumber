package awssns

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/awssns/types/typesfakes"
)

var _ = Describe("AWS SNS Backend", func() {
	defer GinkgoRecover()

	Context("validateWriteOptions", func() {
		It("Returns error on missing --topic flag", func() {
			writeOpts := &opts.WriteOptions{
				Awssns: &opts.WriteGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "",
					},
				},
			}

			err := validateWriteOptions(writeOpts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingTopicARN))
		})

		It("Returns error on invalid ARN", func() {
			writeOpts := &opts.WriteOptions{
				Awssns: &opts.WriteGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "invalid arn",
					},
				},
			}

			err := validateWriteOptions(writeOpts)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("'invalid arn' is not a valid ARN"))
		})

		It("Returns nil on valid config", func() {
			writeOpts := &opts.WriteOptions{
				Awssns: &opts.WriteGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "arn:aws:sns:us-east-2:123456789012:MyTopic",
					},
				},
			}

			err := validateWriteOptions(writeOpts)

			Expect(err).To(BeNil())
		})
	})

	Context("Write", func() {
		It("Returns error on failure to publish", func() {
			expectedErr := errors.New("fake error")
			fakeSNS := &typesfakes.FakeISNSAPI{}
			fakeSNS.PublishStub = func(*sns.PublishInput) (*sns.PublishOutput, error) {
				return nil, expectedErr
			}

			errorCh := make(chan *records.ErrorRecord, 1)

			writeOpts := &opts.WriteOptions{
				Awssns: &opts.WriteGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "arn:aws:sns:us-east-2:123456789012:MyTopic",
					},
				},
			}

			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Awssns{
					Awssns: &args.AWSSNSConn{},
				},
			}

			a := &AWSSNS{
				connOpts: connOpts,
				Service:  fakeSNS,
			}

			writeRecord := &records.WriteRecord{
				Input: "fake message",
			}

			err := a.Write(context.Background(), writeOpts, errorCh, writeRecord)

			// errorCh send is inside a goroutine, sleep shortly to allow it to receive
			time.Sleep(time.Millisecond * 200)

			Expect(err).ToNot(HaveOccurred())
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))
			Expect(errorCh).Should(Receive())
		})

		It("Succeeds", func() {
			fakeSNS := &typesfakes.FakeISNSAPI{}
			fakeSNS.PublishStub = func(*sns.PublishInput) (*sns.PublishOutput, error) {
				return &sns.PublishOutput{MessageId: aws.String("testing")}, nil
			}

			writeOpts := &opts.WriteOptions{
				Awssns: &opts.WriteGroupAWSSNSOptions{
					Args: &args.AWSSNSWriteArgs{
						Topic: "arn:aws:sns:us-east-2:123456789012:MyTopic",
					},
				},
			}

			connOpts := &opts.ConnectionOptions{
				Conn: &opts.ConnectionOptions_Awssns{
					Awssns: &args.AWSSNSConn{},
				},
			}

			a := &AWSSNS{
				connOpts: connOpts,
				Service:  fakeSNS,
				log:      logrus.NewEntry(logrus.New()),
			}

			writeRecord := &records.WriteRecord{
				Input: "fake message",
			}

			err := a.Write(context.Background(), writeOpts, nil, writeRecord)

			Expect(err).To(BeNil())
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))
		})
	})
})
