/*
Package nsq is the official Go package for NSQ (http://nsq.io/).

It provides high-level Consumer and Producer types as well as low-level
functions to communicate over the NSQ protocol.

Consumer

Consuming messages from NSQ can be done by creating an instance of a Consumer and supplying it a handler.

	type myMessageHandler struct {}

	// HandleMessage implements the Handler interface.
	func (h *myMessageHandler) HandleMessage(m *nsq.Message) error {
		if len(m.Body) == 0 {
			// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
			return nil
		}

		err := processMessage(m.Body)

		// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
		return err
	}

	func main() {
		// Instantiate a consumer that will subscribe to the provided channel.
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer("topic", "channel", config)
		if err != nil {
			log.Fatal(err)
		}

		// Set the Handler for messages received by this Consumer. Can be called multiple times.
		// See also AddConcurrentHandlers.
		consumer.AddHandler(&myMessageHandler{})

		// Use nsqlookupd to discover nsqd instances.
		// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
		err = consumer.ConnectToNSQLookupd("localhost:4161")
		if err != nil {
			log.Fatal(err)
		}

		// Gracefully stop the consumer.
		consumer.Stop()
	}

Producer

Producing messages can be done by creating an instance of a Producer.

	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Fatal(err)
	}

	messageBody := []byte("hello")
	topicName := "topic"

	// Synchronously publish a single message to the specified topic.
	// Messages can also be sent asynchronously and/or in batches.
	err = p.Publish(topicName, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	// Gracefully stop the producer.
	producer.Stop()

*/
package nsq
