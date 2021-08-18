package rabbitmq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/options"
	ptypes "github.com/batchcorp/plumber/types"
)

var (
	amqpAddress      = "amqp://localhost"
	testExchangeName = "plumber-exchange"
	testRoutingKey   = "#"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var _ = Describe("RabbitMQ", func() {

	Context("Read/Write", func() {
		var (
			rabbitCh      *amqp.Channel
			testQueueName string
		)

		BeforeEach(func() {
			testQueueName = fmt.Sprintf("plumber-queue-%d", time.Now().UnixNano())

			var setupErr error

			rabbitCh, setupErr = setupRabbit(amqpAddress, testExchangeName, testQueueName)

			Expect(setupErr).ToNot(HaveOccurred())
			Expect(rabbitCh).ToNot(BeNil())
		})

		It("happy path: writes to, and reads from a queue", func() {
			// This test doesn't need to use the separate rabbit channel
			testBody := []byte(fmt.Sprintf("test-body-contents-%d", rand.Int()))

			opts := &options.Options{
				Action: "write",
				Read:   &options.ReadOptions{},
				Rabbit: &options.RabbitOptions{
					Address:             amqpAddress,
					Exchange:            testExchangeName,
					RoutingKey:          testRoutingKey,
					ReadQueue:           testQueueName,
					ReadQueueDurable:    true,
					ReadQueueAutoDelete: false,
					ReadQueueExclusive:  false,
				},
			}

			r := &RabbitMQ{
				Options: opts,
				log:     logrus.WithField("pkg", "rabbitmq_test.go"),
			}

			errorCh := make(chan *ptypes.ErrorMessage, 1)

			// Write something to the exchange
			err := r.Write(context.Background(), errorCh, &ptypes.WriteMessage{Value: testBody})

			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).ShouldNot(Receive())

			// Wait a little for rabbit to copy the message to the queue
			time.Sleep(50 * time.Millisecond)

			// New connection with consumer mode
			r.Options.Action = "read"

			msgCh := make(chan *ptypes.ReadMessage, 1)

			// Expect to receive it
			err = r.Read(context.Background(), msgCh, errorCh)
			//time.Sleep(time.Second)
			Expect(err).ToNot(HaveOccurred())
			Expect(errorCh).ShouldNot(Receive())
			Expect(msgCh).Should(Receive())
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

func startCapture(outC chan string) (*os.File, *os.File) {
	old := os.Stdout
	rf, wf, err := os.Pipe()
	Expect(err).ToNot(HaveOccurred())

	os.Stdout = wf

	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, rf)
		outC <- buf.String()
	}()

	return wf, old
}

func endCapture(wf, oldStdout *os.File, outC chan string) string {
	wf.Close()
	os.Stdout = oldStdout
	out := <-outC
	return out
}
