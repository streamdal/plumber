// Package rabbit is a simple streadway/amqp wrapper library that comes with:
//
// * Auto-reconnect support
//
// * Context support
//
// * Helpers for consuming once or forever and publishing
//
// The library is used internally at https://batch.sh where it powers most of
// the platform's backend services.
//
// For an example, refer to the README.md.
package rabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	// How long to wait before attempting to reconnect to a rabbit server
	DefaultRetryReconnectSec = 60
)

// IRabbit is the interface that the `rabbit` library implements. It's here as
// convenience.
type IRabbit interface {
	Consume(ctx context.Context, errChan chan *ConsumeError, f func(msg amqp.Delivery) error)
	ConsumeOnce(ctx context.Context, runFunc func(msg amqp.Delivery) error) error
	Publish(ctx context.Context, routingKey string, payload []byte) error
	Stop() error
}

// Rabbit struct that is instantiated via `New()`. You should not instantiate
// this struct by hand (unless you have a really good reason to do so).
type Rabbit struct {
	Conn                    *amqp.Connection
	ConsumerDeliveryChannel <-chan amqp.Delivery
	ConsumerRWMutex         *sync.RWMutex
	NotifyCloseChan         chan *amqp.Error
	ProducerServerChannel   *amqp.Channel
	ProducerRWMutex         *sync.RWMutex
	ConsumeLooper           director.Looper
	Options                 *Options

	ctx    context.Context
	cancel func()
	log    *logrus.Entry
}

// Options determines how the `rabbit` library will behave and should be passed
// in to rabbit via `New()`. Many of the options are optional (and will fall
// back to sane defaults).
type Options struct {
	// Required; format "amqp://user:pass@host:port"
	URL string

	// If left empty, server will auto generate queue name
	QueueName string

	// Required
	ExchangeName string

	// Whether to declare/create exchange on connect
	ExchangeDeclare bool

	// Required if declaring queue (valid: direct, fanout, topic, headers)
	ExchangeType string

	// Whether exchange should survive/persist server restarts
	ExchangeDurable bool

	// Whether to delete exchange when its no longer used; used only if ExchangeDeclare set to true
	ExchangeAutoDelete bool

	// Used as either routing (publish) or binding key (consume)
	RoutingKey string

	// https://godoc.org/github.com/streadway/amqp#Channel.Qos
	// Leave unset if no QoS preferences
	QosPrefetchCount int
	QosPrefetchSize  int

	// How long to wait before we retry connecting to a server (after disconnect)
	RetryReconnectSec int

	// Whether queue should survive/persist server restarts (and there are no remaining bindings)
	QueueDurable bool

	// Whether consumer should be the sole consumer of the queue; used only if
	// QueueDeclare set to true
	QueueExclusive bool

	// Whether to delete queue on consumer disconnect; used only if QueueDeclare set to true
	QueueAutoDelete bool

	// Whether to declare/create queue on connect; used only if QueueDeclare set to true
	QueueDeclare bool
}

// ConsumeError will be passed down the error channel if/when `f()` func runs
// into an error during `Consume()`.
type ConsumeError struct {
	Message *amqp.Delivery
	Error   error
}

// New is used for instantiating the library.
func New(opts *Options) (*Rabbit, error) {
	if err := ValidateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}

	ac, err := amqp.Dial(opts.URL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to dial server")
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Rabbit{
		Conn:            ac,
		ConsumerRWMutex: &sync.RWMutex{},
		NotifyCloseChan: make(chan *amqp.Error),
		ProducerRWMutex: &sync.RWMutex{},
		ConsumeLooper:   director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		Options:         opts,

		ctx:    ctx,
		cancel: cancel,
		log:    logrus.WithField("pkg", "rabbit"),
	}

	if err := r.newConsumerChannel(); err != nil {
		return nil, errors.Wrap(err, "unable to get initial delivery channel")
	}

	ac.NotifyClose(r.NotifyCloseChan)

	// Launch connection watcher/reconnect
	go r.watchNotifyClose()

	return r, nil
}

// ValidateOptions validates various combinations of options.
func ValidateOptions(opts *Options) error {
	if opts == nil {
		return errors.New("Options cannot be nil")
	}

	if opts.URL == "" {
		return errors.New("URL cannot be empty")
	}

	if opts.ExchangeDeclare {
		if opts.ExchangeType == "" {
			return errors.New("ExchangeType cannot be empty if ExchangeDeclare set to true")
		}
	}

	if opts.ExchangeName == "" {
		return errors.New("ExchangeName cannot be empty")
	}

	if opts.RoutingKey == "" {
		return errors.New("RoutingKey cannot be empty")
	}

	if opts.RetryReconnectSec == 0 {
		opts.RetryReconnectSec = DefaultRetryReconnectSec
	}

	return nil
}

// Consume consumes messages from the configured queue (`Options.QueueName`) and
// executes `f` for every received message.
//
// `Consume()` will block until it is stopped either via the passed in `ctx` OR
// by calling `Stop()`
//
// It is also possible to see the errors that `f()` runs into by passing in an
// error channel (`chan *ConsumeError`).
//
// Both `ctx` and `errChan` can be `nil`.
//
// If the server goes away, `Consume` will automatically attempt to reconnect.
// Subsequent reconnect attempts will sleep/wait for `DefaultRetryReconnectSec`
// between attempts.
func (r *Rabbit) Consume(ctx context.Context, errChan chan *ConsumeError, f func(msg amqp.Delivery) error) {
	if ctx == nil {
		ctx = context.Background()
	}

	r.log.Debug("waiting for messages from rabbit ...")

	var quit bool

	r.ConsumeLooper.Loop(func() error {
		// This is needed to prevent context flood in case .Quit() wasn't picked
		// up quickly enough by director
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		select {
		case msg := <-r.delivery():
			if err := f(msg); err != nil {
				r.log.Debugf("error during consume: %s", err)

				if errChan != nil {
					// Write in a goroutine in case error channel is not consumed fast enough
					go func() {
						errChan <- &ConsumeError{
							Message: &msg,
							Error:   err,
						}
					}()
				}
			}
		case <-ctx.Done():
			r.log.Warning("stopped via context")
			r.ConsumeLooper.Quit()
			quit = true
		case <-r.ctx.Done():
			r.log.Warning("stopped via Stop()")
			r.ConsumeLooper.Quit()
			quit = true
		}

		return nil
	})

	r.log.Debug("Consume finished - exiting")
}

// ConsumeOnce will consume exactly one message from the configured queue,
// execute `runFunc()` on the message and return.
//
// Same as with `Consume()`, you can pass in a context to cancel `ConsumeOnce()`
// or run `Stop()`.
func (r *Rabbit) ConsumeOnce(ctx context.Context, runFunc func(msg amqp.Delivery) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	r.log.Debug("waiting for a single message from rabbit ...")

	select {
	case msg := <-r.delivery():
		if err := runFunc(msg); err != nil {
			return err
		}
	case <-ctx.Done():
		r.log.Warning("stopped via context")
		return nil
	case <-r.ctx.Done():
		r.log.Warning("stopped via Stop()")
		return nil
	}

	r.log.Debug("ConsumeOnce finished - exiting")

	return nil
}

// Publish publishes one message to the configured exchange, using the specified
// routing key.
//
// NOTE: Context semantics are not implemented.
//
// TODO: Implement ctx usage
func (r *Rabbit) Publish(ctx context.Context, routingKey string, body []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Is this the first time we're publishing?
	if r.ProducerServerChannel == nil {
		ch, err := r.newServerChannel()
		if err != nil {
			return errors.Wrap(err, "unable to create server channel")
		}

		r.ProducerRWMutex.Lock()
		r.ProducerServerChannel = ch
		r.ProducerRWMutex.Unlock()
	}

	r.ProducerRWMutex.RLock()
	defer r.ProducerRWMutex.RUnlock()

	if err := r.ProducerServerChannel.Publish(r.Options.ExchangeName, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return err
	}

	return nil
}

// Stop stops an in-progress `Consume()` or `ConsumeOnce()`.
func (r *Rabbit) Stop() error {
	r.cancel()
	return nil
}

func (r *Rabbit) watchNotifyClose() {
	// TODO: Use a looper here
	for {
		closeErr := <-r.NotifyCloseChan

		r.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", closeErr)

		// Acquire mutex to pause all consumers while we reconnect AND prevent
		// access to the channel map
		r.ConsumerRWMutex.Lock()

		var attempts int

		for {
			attempts++

			if err := r.reconnect(); err != nil {
				r.log.Warningf("unable to complete reconnect: %s; retrying in %d", err, r.Options.RetryReconnectSec)
				time.Sleep(time.Duration(r.Options.RetryReconnectSec) * time.Second)
				continue
			}

			r.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// Create and set a new notify close channel (since old one gets closed)
		r.NotifyCloseChan = make(chan *amqp.Error, 0)
		r.Conn.NotifyClose(r.NotifyCloseChan)

		// Update channel
		if err := r.newConsumerChannel(); err != nil {
			logrus.Errorf("unable to set new channel: %s", err)

			// TODO: This is super shitty. Should address this.
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}

		// Unlock so that consumers can begin reading messages from a new channel
		r.ConsumerRWMutex.Unlock()

		r.log.Debug("watchNotifyClose has completed successfully")
	}
}

func (r *Rabbit) newServerChannel() (*amqp.Channel, error) {
	if r.Conn == nil {
		return nil, errors.New("r.Conn is nil - did this get instantiated correctly? bug?")
	}

	ch, err := r.Conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if err := ch.Qos(r.Options.QosPrefetchCount, r.Options.QosPrefetchSize, false); err != nil {
		return nil, errors.Wrap(err, "unable to set qos policy")
	}

	if r.Options.ExchangeDeclare {
		if err := ch.ExchangeDeclare(
			r.Options.ExchangeName,
			r.Options.ExchangeType,
			r.Options.ExchangeDurable,
			r.Options.ExchangeAutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to declare exchange")
		}
	}

	if r.Options.QueueDeclare {
		if _, err := ch.QueueDeclare(
			r.Options.QueueName,
			r.Options.QueueDurable,
			r.Options.QueueAutoDelete,
			r.Options.QueueExclusive,
			false,
			nil,
		); err != nil {
			return nil, err
		}
	}

	if err := ch.QueueBind(
		r.Options.QueueName,
		r.Options.RoutingKey,
		r.Options.ExchangeName,
		false,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "unable to bind queue")
	}

	return ch, nil
}

func (r *Rabbit) newConsumerChannel() error {
	serverChannel, err := r.newServerChannel()
	if err != nil {
		return errors.Wrap(err, "unable to create new server channel")
	}

	deliveryChannel, err := serverChannel.Consume(r.Options.QueueName,
		"",
		false,
		r.Options.QueueExclusive,
		false,
		false,
		nil,
	)

	if err != nil {
		return errors.Wrap(err, "unable to create delivery channel")
	}

	r.ConsumerDeliveryChannel = deliveryChannel

	return nil
}

func (r *Rabbit) reconnect() error {
	ac, err := amqp.Dial(r.Options.URL)
	if err != nil {
		return err
	}

	r.Conn = ac

	return nil
}

func (r *Rabbit) delivery() <-chan amqp.Delivery {
	// Acquire lock (in case we are reconnecting and channels are being swapped)
	r.ConsumerRWMutex.RLock()
	defer r.ConsumerRWMutex.RUnlock()

	return r.ConsumerDeliveryChannel
}
