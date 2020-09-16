package relay

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
)

type Relay struct {
	RelayCh chan interface{}
	log     *logrus.Entry
}

func New(token, gRPCAddress string, relayCh chan interface{}) (*Relay, error) {
	// TODO: Verify address

	// TODO: Verify token

	// TODO: Set JSON formatter

	return &Relay{
		RelayCh: relayCh,
		log:     logrus.WithField("pkg", "relay"),
	}, nil
}

func (r *Relay) Run() {
	r.log.Debug("Relayer started")

	for {
		msg := <-r.RelayCh

		switch v := msg.(type) {
		case *sqs.Message:
			fmt.Printf("Received an SQS message: %v\n", v)
		default:
			fmt.Println("Received unknown message type: ", v)
		}
	}

	r.log.Debug("Relayer is exiting")
}
