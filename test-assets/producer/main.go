package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
)

var (
	countFlag      = flag.Int("c", 100, "number of batched writes to perform (0 == forever)")
	batchSizeFlag  = flag.Int("b", 100, "number of messages to batch on write")
	addrFlag       = flag.String("a", "localhost:9092", "kafka address")
	topicFlag      = flag.String("t", "test", "kafka topic to write to")
	authMethodFlag = flag.String("auth-mode", "", "auth mode")
	authUserFlag   = flag.String("auth-user", "", "auth user")
	authPassFlag   = flag.String("auth-pass", "", "auth password")
	insecureFlag   = flag.Bool("insecure", false, "skip tls verification")

	messagesWritten int
)

func init() {
	flag.Parse()
}

func main() {
	auth, err := getAuthenticationMechanism(*authMethodFlag, *authUserFlag, *authPassFlag)
	if err != nil {
		log.Fatalf("unable to get auth mechanism: %s", err)
	}

	transport := &kafka.Transport{
		SASL: auth,
	}

	if *insecureFlag {
		transport.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	w := &kafka.Writer{
		Addr:      kafka.TCP(*addrFlag),
		Topic:     *topicFlag,
		Balancer:  &kafka.LeastBytes{},
		Transport: transport,
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

func getAuthenticationMechanism(authType, user, pass string) (sasl.Mechanism, error) {
	if user == "" {
		return nil, nil
	}

	if pass == "" {
		return nil, errors.New("password must be set if username is set")
	}

	switch authType {
	case "scram":
		return scram.Mechanism(scram.SHA512, user, pass)
	default:
		return plain.Mechanism{
			Username: user,
			Password: pass,
		}, nil
	}
}

func report() {
	ticker := time.NewTicker(5 * time.Second)

	var previousValue int
	var messagesThisTime int
	var rate float64

	for range ticker.C {
		messagesThisTime = messagesWritten - previousValue
		rate = float64(messagesThisTime / 5)

		fmt.Printf("Wrote %d messages [Total: %d] [Rate: %f/s]\n",
			messagesThisTime, messagesWritten, rate)
		previousValue = messagesWritten
	}
}

func generateKafkaMessages(num int) []kafka.Message {
	messages := make([]kafka.Message, 0)

	for i := 0; i != num; i++ {
		messages = append(messages, kafka.Message{
			Value: []byte(fmt.Sprintf(`{"producer":"test-%d"}`, i)),
		})
	}

	return messages
}
