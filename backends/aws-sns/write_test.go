package awssns

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/batchcorp/plumber/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/aws-sns/types/typesfakes"
	"github.com/batchcorp/plumber/options"
)

var _ = Describe("AWS SNS Backend", func() {
	defer GinkgoRecover()

	Context("validateWriteOptions", func() {
		It("Returns error on missing --topic flag", func() {
			opts := &options.Options{
				AWSSNS: &options.AWSSNSOptions{},
			}

			err := validateOpts(opts)

			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(errMissingTopicARN))
		})

		It("Returns error on invalid ARN", func() {
			opts := &options.Options{
				AWSSNS: &options.AWSSNSOptions{
					TopicArn: "invalid arn",
				},
			}

			err := validateOpts(opts)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("'invalid arn' is not a valid ARN"))
		})

		It("Returns nil on valid config", func() {

			opts := &options.Options{
				AWSSNS: &options.AWSSNSOptions{
					TopicArn: "arn:aws:sns:us-east-2:123456789012:MyTopic",
				},
			}

			err := validateOpts(opts)

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

			opts := &options.Options{
				AWSSNS: &options.AWSSNSOptions{
					TopicArn: "arn:aws:sns:us-east-2:123456789012:MyTopic",
				},
			}

			a := &AWSSNS{
				Options: opts,
				service: fakeSNS,
			}

			err := a.Write(context.Background(), nil, &types.WriteMessage{
				Value: []byte(`fake message`),
			})

			Expect(err).To(HaveOccurred())
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))
			Expect(err.Error()).To(Equal("could not publish message to SNS: " + expectedErr.Error()))
		})

		It("Succeeds", func() {

			fakeSNS := &typesfakes.FakeISNSAPI{}
			fakeSNS.PublishStub = func(*sns.PublishInput) (*sns.PublishOutput, error) {
				return &sns.PublishOutput{MessageId: aws.String("testing")}, nil
			}

			opts := &options.Options{
				AWSSNS: &options.AWSSNSOptions{
					TopicArn: "arn:aws:sns:us-east-2:123456789012:MyTopic",
				},
			}

			a := &AWSSNS{
				Options: opts,
				service: fakeSNS,
				log:     logrus.NewEntry(logrus.New()),
			}

			err := a.Write(context.Background(), nil, &types.WriteMessage{
				Value: []byte(`fake message`),
			})

			Expect(err).To(BeNil())
			Expect(fakeSNS.PublishCallCount()).To(Equal(1))
		})
	})
})
