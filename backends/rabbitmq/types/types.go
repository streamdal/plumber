package types

import (
	"github.com/streadway/amqp"
)

type RelayMessage struct {
	Value   *amqp.Delivery
	Options *RelayMessageOptions
}

type RelayMessageOptions struct {

}
