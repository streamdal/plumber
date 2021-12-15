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
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

const (
	// DefaultRetryReconnectSec determines how long to wait before attempting
	// to reconnect to a rabbit server
	DefaultRetryReconnectSec = 60

	// Both means that the client is acting as both a consumer and a producer.
	Both Mode = 0
	// Consumer means that the client is acting as a consumer.
	Consumer Mode = 1
	// Producer means that the client is acting as a producer.
	Producer Mode = 2
)

var (
	// ErrShutdown will be returned if the underlying connection has already
	// been closed (ie. if you Close()'d and then tried to Publish())
	ErrShutdown = errors.New("connection has been shutdown")

	// DefaultConsumerTag is used for identifying consumer
	DefaultConsumerTag = "c-rabbit-" + uuid.NewV4().String()[0:8]

	// DefaultAppID is used for identifying the producer
	DefaultAppID = "p-rabbit-" + uuid.NewV4().String()[0:8]
)

// IRabbit is the interface that the `rabbit` library implements. It's here as
// convenience.
type IRabbit interface {
	Consume(ctx context.Context, errChan chan *ConsumeError, f func(msg amqp.Delivery) error)
	ConsumeOnce(ctx context.Context, runFunc func(msg amqp.Delivery) error) error
	Publish(ctx context.Context, routingKey string, payload []byte) error
	Stop() error
	Close() error
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

	shutdown bool
	ctx      context.Context
	cancel   func()
	log      Logger
}

// Mode is the type used to represent whether the RabbitMQ
// cliens is acting as a consumer, a producer, or both.
type Mode int

// Binding represents the information needed to bind a queue to
// an Exchange.
type Binding struct {
	// Required
	ExchangeName string

	// Bind a queue to one or more routing keys
	BindingKeys []string

	// Whether to declare/create exchange on connect
	ExchangeDeclare bool

	// Required if declaring queue (valid: direct, fanout, topic, headers)
	ExchangeType string

	// Whether exchange should survive/persist server restarts
	ExchangeDurable bool

	// Whether to delete exchange when its no longer used; used only if ExchangeDeclare set to true
	ExchangeAutoDelete bool
}

// Options determines how the `rabbit` library will behave and should be passed
// in to rabbit via `New()`. Many of the options are optional (and will fall
// back to sane defaults).
type Options struct {
	// Required; format "amqp://user:pass@host:port"
	URLs []string

	// In what mode does the library operate (Both, Consumer, Producer)
	Mode Mode

	// If left empty, server will auto generate queue name
	QueueName string

	// Bindings is the set of information need to bind a queue to one or
	// more exchanges, specifying one or more binding (routing) keys.
	Bindings []Binding

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

	// Additional arguements to pass to the queue declaration or binding
	// https://github.com/batchcorp/plumber/issues/210
	QueueArgs map[string]interface{}

	// Whether to automatically acknowledge consumed message(s)
	AutoAck bool

	// Used for identifying consumer
	ConsumerTag string

	// Used as a property to identify producer
	AppID string

	// Use TLS
	UseTLS bool

	// Skip cert verification (only applies if UseTLS is true)
	SkipVerifyTLS bool

	// Log is the (optional) logger to use for writing out log messages.
	Log Logger
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

	var ac *amqp.Connection
	var err error

	// try all available URLs in a loop and quit as soon as it
	// can successfully establish a connection to one of them
	for _, url := range opts.URLs {
		if opts.UseTLS {
			tlsConfig := &tls.Config{}

			if opts.SkipVerifyTLS {
				tlsConfig.InsecureSkipVerify = true
			}

			ac, err = amqp.DialTLS(url, tlsConfig)
		} else {
			ac, err = amqp.Dial(url)
		}

		if err == nil {
			// yes, we made it!
			break
		}
	}

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
		log:    opts.Log,
	}

	if opts.Mode != Producer {
		if err := r.newConsumerChannel(); err != nil {
			return nil, errors.Wrap(err, "unable to get initial delivery channel")
		}
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

	validURL := false
	for _, url := range opts.URLs {
		if len(url) > 0 {
			validURL = true
			break
		}
	}

	if !validURL {
		return errors.New("At least one non-empty URL must be provided")
	}

	if len(opts.Bindings) == 0 {
		return errors.New("At least one Exchange must be specified")
	}

	if err := validateBindings(opts); err != nil {
		return errors.Wrap(err, "binding validation failed")
	}

	applyDefaults(opts)

	if err := validMode(opts.Mode); err != nil {
		return err
	}

	return nil
}

func validateBindings(opts *Options) error {
	if opts.Mode == Producer || opts.Mode == Both {
		if len(opts.Bindings) > 1 {
			return errors.New("Exactly one Exchange must be specified when publishing messages")
		}
	}

	for _, binding := range opts.Bindings {
		if binding.ExchangeDeclare {
			if binding.ExchangeType == "" {
				return errors.New("ExchangeType cannot be empty if ExchangeDeclare set to true")
			}
		}
		if binding.ExchangeName == "" {
			return errors.New("ExchangeName cannot be empty")
		}

		// BindingKeys are only needed if Consumer or Both
		if opts.Mode != Producer {
			if len(binding.BindingKeys) < 1 {
				return errors.New("At least one BindingKeys must be specified")
			}
		}
	}

	return nil
}

func applyDefaults(opts *Options) {
	if opts == nil {
		return
	}

	if opts.RetryReconnectSec == 0 {
		opts.RetryReconnectSec = DefaultRetryReconnectSec
	}

	if opts.AppID == "" {
		opts.AppID = DefaultAppID
	}

	if opts.ConsumerTag == "" {
		opts.ConsumerTag = DefaultConsumerTag
	}

	if opts.Log == nil {
		opts.Log = &NoOpLogger{}
	}

	if opts.QueueArgs == nil {
		opts.QueueArgs = make(map[string]interface{})
	}
}

func validMode(mode Mode) error {
	validModes := []Mode{Both, Producer, Consumer}

	var found bool

	for _, validMode := range validModes {
		if validMode == mode {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("invalid mode '%d'", mode)
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
	if r.shutdown {
		r.log.Error(ErrShutdown)
		return
	}

	if r.Options.Mode == Producer {
		r.log.Error("unable to Consume() - library is configured in Producer mode")
		return
	}

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
			r.log.Warn("stopped via context")
			r.ConsumeLooper.Quit()
			quit = true
		case <-r.ctx.Done():
			r.log.Warn("stopped via Stop()")
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
	if r.shutdown {
		return ErrShutdown
	}

	if r.Options.Mode == Producer {
		return errors.New("unable to ConsumeOnce - library is configured in Producer mode")
	}

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
		r.log.Warn("stopped via context")
		return nil
	case <-r.ctx.Done():
		r.log.Warn("stopped via Stop()")
		return nil
	}

	r.log.Debug("ConsumeOnce finished - exiting")

	return nil
}

// Publish publishes one message to the configured exchange, using the specified
// routing key.
func (r *Rabbit) Publish(ctx context.Context, routingKey string, body []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if r.shutdown {
		return ErrShutdown
	}

	if r.Options.Mode == Consumer {
		return errors.New("unable to Publish - library is configured in Consumer mode")
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

	// Create channels for error and done signals
	chanErr := make(chan error)
	chanDone := make(chan struct{})
	go func() {
		if err := r.ProducerServerChannel.Publish(r.Options.Bindings[0].ExchangeName, routingKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
			AppId:        r.Options.AppID,
		}); err != nil {
			// Signal there is an error
			chanErr <- err
		}

		// Signal we are done
		chanDone <- struct{}{}
	}()

	select {
	case <-chanDone:
		// We did it!
		return nil
	case err := <-chanErr:
		return errors.Wrap(err, "failed to publish message")
	case <-ctx.Done():
		r.log.Warn("stopped via context")
		err := r.ProducerServerChannel.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close producer channel")
		}
		return errors.New("context cancelled")
	}
}

// Stop stops an in-progress `Consume()` or `ConsumeOnce()`.
func (r *Rabbit) Stop() error {
	r.cancel()
	return nil
}

// Close stops any active Consume and closes the amqp connection (and channels using the conn)
//
// You should re-instantiate the rabbit lib once this is called.
func (r *Rabbit) Close() error {
	r.cancel()

	if err := r.Conn.Close(); err != nil {
		return fmt.Errorf("unable to close amqp connection: %s", err)
	}

	r.shutdown = true

	return nil
}

func (r *Rabbit) watchNotifyClose() {
	// TODO: Use a looper here
	for {
		closeErr := <-r.NotifyCloseChan

		r.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", closeErr)

		// Acquire mutex to pause all consumers/producers while we reconnect AND prevent
		// access to the channel map
		r.ConsumerRWMutex.Lock()
		r.ProducerRWMutex.Lock()

		var attempts int

		for {
			attempts++
			if err := r.reconnect(); err != nil {
				r.log.Warnf("unable to complete reconnect: %s; retrying in %d", err, r.Options.RetryReconnectSec)
				time.Sleep(time.Duration(r.Options.RetryReconnectSec) * time.Second)
				continue
			}
			r.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// Create and set a new notify close channel (since old one gets shutdown)
		r.NotifyCloseChan = make(chan *amqp.Error, 0)
		r.Conn.NotifyClose(r.NotifyCloseChan)

		// Update channel
		if r.Options.Mode == Producer {
			serverChannel, err := r.newServerChannel()
			if err != nil {
				r.log.Errorf("unable to set new channel: %s", err)
				panic(fmt.Sprintf("unable to set new channel: %s", err))
			}

			r.ProducerServerChannel = serverChannel
		} else {
			if err := r.newConsumerChannel(); err != nil {
				r.log.Errorf("unable to set new channel: %s", err)

				// TODO: This is super shitty. Should address this.
				panic(fmt.Sprintf("unable to set new channel: %s", err))
			}
		}

		// Unlock so that consumers/producers can begin reading messages from a new channel
		r.ConsumerRWMutex.Unlock()
		r.ProducerRWMutex.Unlock()
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

	// Only declare queue if in Both or Consumer mode
	if r.Options.Mode != Producer {
		if r.Options.QueueDeclare {
			if _, err := ch.QueueDeclare(
				r.Options.QueueName,
				r.Options.QueueDurable,
				r.Options.QueueAutoDelete,
				r.Options.QueueExclusive,
				false,
				r.Options.QueueArgs,
			); err != nil {
				return nil, err
			}
		}
	}

	for _, binding := range r.Options.Bindings {
		if binding.ExchangeDeclare {
			if err := ch.ExchangeDeclare(
				binding.ExchangeName,
				binding.ExchangeType,
				binding.ExchangeDurable,
				binding.ExchangeAutoDelete,
				false,
				false,
				nil,
			); err != nil {
				return nil, errors.Wrap(err, "unable to declare exchange")
			}
		}

		// Only bind queue if in Both or Consumer mode
		if r.Options.Mode != Producer {
			for _, bindingKey := range binding.BindingKeys {
				if err := ch.QueueBind(
					r.Options.QueueName,
					bindingKey,
					binding.ExchangeName,
					false,
					r.Options.QueueArgs,
				); err != nil {
					return nil, errors.Wrap(err, "unable to bind queue")
				}
			}
		}
	}

	return ch, nil
}

func (r *Rabbit) newConsumerChannel() error {
	serverChannel, err := r.newServerChannel()
	if err != nil {
		return errors.Wrap(err, "unable to create new server channel")
	}

	deliveryChannel, err := serverChannel.Consume(
		r.Options.QueueName,
		r.Options.ConsumerTag,
		r.Options.AutoAck,
		r.Options.QueueExclusive,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create delivery channel")
	}

	r.ProducerServerChannel = serverChannel
	r.ConsumerDeliveryChannel = deliveryChannel

	return nil
}

func (r *Rabbit) reconnect() error {
	var ac *amqp.Connection
	var err error

	// try all available URLs in a loop and quit as soon as it
	// can successfully establish a connection to one of them
	for _, url := range r.Options.URLs {
		if r.Options.UseTLS {
			tlsConfig := &tls.Config{}

			if r.Options.SkipVerifyTLS {
				tlsConfig.InsecureSkipVerify = true
			}

			ac, err = amqp.DialTLS(url, tlsConfig)
		} else {
			ac, err = amqp.Dial(url)
		}

		if err == nil {
			// yes, we made it!
			break
		}
	}

	if err != nil {
		return errors.Wrap(err, "all servers failed on reconnect")
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
