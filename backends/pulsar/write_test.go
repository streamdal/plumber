package pulsar

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/apache/pulsar-client-go/pulsar"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/pulsar/pulsarfakes"
)

var _ = Describe("Pulsar Backend", func() {

	writeOpts := &opts.WriteOptions{
		Pulsar: &opts.WriteGroupPulsarOptions{
			Args: &args.PulsarWriteArgs{Topic: "testing"},
		},
	}
	Context("Write", func() {
		It("returns error when producer fails to create", func() {
			testErr := errors.New("test err")

			fakeClient := &pulsarfakes.FakeClient{}
			fakeClient.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				return nil, testErr
			}

			p := &Pulsar{client: fakeClient}

			err := p.Write(context.Background(), writeOpts, nil, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(testErr.Error()))
		})

		It("sends a message to error channel", func() {
			fakeClient := &pulsarfakes.FakeClient{}
			fakeClient.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				fakeProducer := &pulsarfakes.FakeProducer{}
				fakeProducer.SendStub = func(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
					return nil, errors.New("test err")
				}
				return fakeProducer, nil
			}

			p := &Pulsar{
				client: fakeClient,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errChan := make(chan *records.ErrorRecord, 1)

			msgs := &records.WriteRecord{Input: "test"}
			p.Write(context.Background(), writeOpts, errChan, msgs)

			Expect(errChan).To(Receive())
		})

		It("publishes the message", func() {
			fakeClient := &pulsarfakes.FakeClient{}
			fakeClient.CreateProducerStub = func(pulsar.ProducerOptions) (pulsar.Producer, error) {
				fakeProducer := &pulsarfakes.FakeProducer{}
				fakeProducer.SendStub = func(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
					return nil, errors.New("test err")
				}
				return fakeProducer, nil
			}

			p := &Pulsar{
				client: fakeClient,
				log:    logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
			}

			errChan := make(chan *records.ErrorRecord, 1)

			msgs := &records.WriteRecord{Input: "test"}

			err := p.Write(context.Background(), writeOpts, errChan, msgs)
			Expect(err).To(BeNil())
			Expect(errChan).To(Receive())
		})
	})
})
