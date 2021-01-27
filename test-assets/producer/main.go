package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	countFlag     = flag.Int("c", 100, "number of batched writes to perform (0 == forever)")
	batchSizeFlag = flag.Int("b", 100, "number of messages to batch on write")
	addrFlag      = flag.String("a", "localhost:9092", "kafka address")
	topicFlag     = flag.String("t", "test", "kafka topic to write to")

	messagesWritten int
)

func init() {
	flag.Parse()
}

func main() {
	w := &kafka.Writer{
		Addr:     kafka.TCP(*addrFlag),
		Topic:    *topicFlag,
		Balancer: &kafka.LeastBytes{},
	}

	if *countFlag == 0 {
		go report()
	}

	var iterations int

	for {
		messages := generateKafkaMessages(*batchSizeFlag)

		err := w.WriteMessages(context.Background(), messages...)
		if err != nil {
			logrus.Fatalf("failed to write messages: %s", err)
		}

		messagesWritten += len(messages)

		if *countFlag == 0 {
			continue
		}

		iterations += 1

		if iterations >= *countFlag && *countFlag != 0 {
			if err := w.Close(); err != nil {
				log.Fatal("failed to close writer:", err)
			}

			break
		}
	}

	logrus.Infof("Wrote %d messages", messagesWritten)
}

func report() {
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		logrus.Infof("Wrote %d messages", messagesWritten)
	}
}

func generateKafkaMessages(num int) []kafka.Message {
	messages := make([]kafka.Message, 0)

	for i := 0; i != num; i++ {
		messages = append(messages, kafka.Message{
			Value: []byte(`{"producer":"test"}`),
		})
	}

	return messages
}
