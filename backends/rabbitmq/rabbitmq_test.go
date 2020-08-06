package rabbitmq

import (
	"fmt"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

var (
	amqpAddress      = "amqp://localhost"
	testExchangeName = "plumber-exchange"
	testRoutingKey   = "#"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var _ = Describe("Shared", func() {
	Context("connect", func() {
		var (
			rabbitCh      *amqp.Channel
			testQueueName string
		)

		BeforeEach(func() {
			testQueueName = fmt.Sprintf("plumber-queue-%d", rand.Int())

			var setupErr error

			rabbitCh, setupErr = setupRabbit(amqpAddress, testExchangeName, testQueueName)

			Expect(setupErr).ToNot(HaveOccurred())
			Expect(rabbitCh).ToNot(BeNil())
		})

		It("happy path: returns an amqp channel", func() {
			testBody := []byte(fmt.Sprintf("test-body-contents-%d", rand.Int()))

			opts := &Options{
				Action:          "foo",
				Address:         amqpAddress,
				QueueName:       "should-not-exist",
				ExchangeName:    testExchangeName,
				RoutingKey:      testRoutingKey,
				QueueDurable:    false,
				QueueAutoDelete: true,
				QueueExclusive:  true,
			}

			ch, err := connect(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(ch).ToNot(BeNil())

			// Ensure we're able to use the channel for publishing
			err = ch.Publish(testExchangeName, "messages", false, false,
				amqp.Publishing{
					Body: testBody,
				})

			Expect(err).ToNot(HaveOccurred())

			// Wait a little for rabbit to copy the message to the queue
			time.Sleep(50 * time.Millisecond)

			// Expect to receive it
			msg, ok, err := rabbitCh.Get(testQueueName, true)

			Expect(ok).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
			Expect(msg.Body).To(Equal(testBody))

			// Verify that we didn't create the queue
			_, ok2, err := rabbitCh.Get("should-not-exist", true)

			Expect(ok2).To(BeFalse())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("NOT_FOUND - no queue"))
		})

		It("happy path: with 'read' action, declares queue and binds it to exchange w/ routing key", func() {
			// This test doesn't need to use the separate rabbit channel
			testBody := []byte(fmt.Sprintf("test-body-contents-%d", rand.Int()))

			opts := &Options{
				Action:          "read",
				Address:         amqpAddress,
				QueueName:       "should-exist",
				ExchangeName:    testExchangeName,
				RoutingKey:      testRoutingKey,
				QueueDurable:    false,
				QueueAutoDelete: true,
				QueueExclusive:  true,
			}

			ch, err := connect(opts)

			Expect(err).ToNot(HaveOccurred())
			Expect(ch).ToNot(BeNil())

			// Write something to the exchange
			err = ch.Publish(testExchangeName, "messages", false, false,
				amqp.Publishing{
					Body: testBody,
				})

			Expect(err).ToNot(HaveOccurred())

			// Wait a little for rabbit to copy the message to the queue
			time.Sleep(50 * time.Millisecond)

			// Expect to receive it
			msg, ok, err := ch.Get("should-exist", true)

			Expect(ok).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
			Expect(msg.Body).To(Equal(testBody))
		})

		It("error: will error when unable to dial destination host", func() {
			ch, err := connect(&Options{
				Address: "bad-address",
			})

			Expect(err).To(HaveOccurred())
			Expect(ch).To(BeNil())
		})
	})
})

func setupRabbit(address, exchange, queue string) (*amqp.Channel, error) {
	ac, err := amqp.Dial(address)
	if err != nil {
		return nil, errors.Wrap(err, "unable to dial amqp address")
	}

	ch, err := ac.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if err := ch.ExchangeDeclare(exchange, "topic", false, false, false, false, nil); err != nil {
		return nil, errors.Wrap(err, "unable to declare exchange")
	}

	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return nil, errors.Wrap(err, "unable to declare queue")
	}

	if err := ch.QueueBind(queue, "#", exchange, false, nil); err != nil {
		return nil, errors.Wrap(err, "unable to bind queue")
	}

	return ch, nil
}
