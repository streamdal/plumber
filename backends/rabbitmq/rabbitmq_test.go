package rabbitmq

import (
	"bytes"
	"context"
	"fmt"
	"github.com/batchcorp/rabbit"
	"github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/cli"
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
			testQueueName = fmt.Sprintf("plumber-queue-%d", rand.Int())

			var setupErr error

			rabbitCh, setupErr = setupRabbit(amqpAddress, testExchangeName, testQueueName)

			Expect(setupErr).ToNot(HaveOccurred())
			Expect(rabbitCh).ToNot(BeNil())
		})

		It("happy path: writes to, and reads from a queue", func() {
			outC := make(chan string)
			wf, oldStdout := startCapture(outC)

			// This test doesn't need to use the separate rabbit channel
			testBody := []byte(fmt.Sprintf("test-body-contents-%d", rand.Int()))

			opts := &cli.Options{
				Action: "read",
				Rabbit: &cli.RabbitOptions{
					Address:             amqpAddress,
					Exchange:            testExchangeName,
					RoutingKey:          testRoutingKey,
					ReadQueue:           testQueueName,
					ReadQueueDurable:    false,
					ReadQueueAutoDelete: true,
					ReadQueueExclusive:  true,
				},
			}

			rmq, err := rabbit.New(&rabbit.Options{
				URL:            amqpAddress,
				QueueName:      testQueueName,
				ExchangeName:   testExchangeName,
				RoutingKey:     testRoutingKey,
				QueueExclusive: opts.Rabbit.ReadQueueExclusive,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(rmq).ToNot(BeNil())

			r := &RabbitMQ{
				Options:  opts,
				Consumer: rmq,
				MsgDesc:  nil,
				log:      logrus.WithField("pkg", "rabbitmq_test.go"),
			}

			// Write something to the exchange
			err = r.Write(context.Background(), testBody)

			Expect(err).ToNot(HaveOccurred())

			// Wait a little for rabbit to copy the message to the queue
			time.Sleep(50 * time.Millisecond)

			// Expect to receive it
			err = r.Read()

			// End capturing stdout and assert that it contains our expected message
			out := endCapture(wf, oldStdout, outC)

			Expect(err).ToNot(HaveOccurred())
			Expect(out).To(ContainSubstring(string(testBody)))
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
