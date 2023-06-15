package natty

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func (n *Natty) Publish(ctx context.Context, subject string, value []byte) {
	span, ctx := tracer.StartSpanFromContext(ctx, "natty.Publish")
	defer span.Finish()

	n.getPublisherBySubject(subject).batch(ctx, subject, value)
}

// DeletePublisher will stop the batch publisher goroutine and remove the
// publisher from the shared publisher map.
//
// It is safe to call this if a publisher for the topic does not exist.
//
// Returns bool which indicate if publisher exists.
func (n *Natty) DeletePublisher(ctx context.Context, topic string) bool {
	n.publisherMutex.RLock()
	publisher, ok := n.publisherMap[topic]
	n.publisherMutex.RUnlock()

	if !ok {
		n.log.Debugf("publisher for topic '%s' not found", topic)
		return false
	}

	n.log.Debugf("found existing publisher in cache for topic '%s' - closing and removing", topic)

	// Stop batch publisher goroutine
	publisher.PublisherCancel()

	n.publisherMutex.Lock()
	delete(n.publisherMap, topic)
	n.publisherMutex.Unlock()

	return true
}

func (n *Natty) getPublisherBySubject(subject string) *Publisher {
	n.publisherMutex.Lock()
	defer n.publisherMutex.Unlock()

	p, ok := n.publisherMap[subject]
	if !ok {
		n.log.Debugf("creating new publisher goroutine for subject '%s'", subject)

		p = n.newPublisher(subject)
		n.publisherMap[subject] = p
	}

	return p
}

func (n *Natty) newPublisher(subject string) *Publisher {
	ctx, cancel := context.WithCancel(context.Background())
	publisher := &Publisher{
		Subject:                subject,
		QueueMutex:             &sync.RWMutex{},
		Queue:                  make([]*message, 0),
		looper:                 director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		PublisherContext:       ctx,
		PublisherCancel:        cancel,
		ErrorCh:                n.PublishErrorCh,
		Natty:                  n,
		ServiceShutdownContext: n.ServiceShutdownContext,
		IdleTimeout:            n.WorkerIdleTimeout,
		log:                    n.log,
	}

	go publisher.runBatchPublisher(ctx)

	return publisher
}

func (p *Publisher) writeMessagesBatch(ctx context.Context, msgs []*message) error {
	p.log.Debugf("creating a batch for %d messages", len(msgs))

	js, err := p.Natty.nc.JetStream(nats.PublishAsyncMaxPending(p.Natty.PublishBatchSize), nats.Context(ctx))
	if err != nil {
		return errors.Wrap(err, "unable to create JetStream context")
	}

	batches := buildBatch(msgs, p.Natty.PublishBatchSize)

	// TODO: how to handle retry?
	for _, batch := range batches {
		for _, msg := range batch {
			if _, err := js.PublishAsync(msg.Subject, msg.Value); err != nil {
				err = errors.Wrap(err, "unable to publish message")
				p.writeError(err)
			}
		}

		select {
		case <-js.PublishAsyncComplete():
			p.log.Debugf("Successfully published '%d' messages", len(msgs))
			return nil
		case <-time.After(p.Natty.PublishTimeout):
			msg := fmt.Errorf("timed out waiting for message acknowledgement of '%d' messages for '%s'", len(batch), p.Subject)
			p.writeError(msg)
		}
	}

	return nil
}

func (p *Publisher) writeError(err error) {
	p.log.Error(err)

	if p.ErrorCh == nil {
		return
	}

	go func() {
		// Writing in goroutine in case channel is blocked
		select {
		case p.ErrorCh <- &PublishError{
			Subject: p.Subject,
			Message: err,
		}:
		default:
			p.log.Warnf("publish error channel is full; discarding error")
		}
	}()
}

func (p *Publisher) batch(_ context.Context, subject string, value []byte) {
	p.QueueMutex.Lock()
	defer p.QueueMutex.Unlock()

	p.Queue = append(p.Queue, &message{
		Subject: subject,
		Value:   value,
	})
}

func buildBatch(slice []*message, entriesPerBatch int) [][]*message {
	batch := make([][]*message, 0)

	if len(slice) < entriesPerBatch {
		return append(batch, slice)
	}

	// How many iterations should we have?
	iterations := len(slice) / entriesPerBatch

	// We're operating in ints - we need the remainder
	remainder := len(slice) % entriesPerBatch

	var startIndex int
	nextIndex := entriesPerBatch

	for i := 0; i != iterations; i++ {
		batch = append(batch, slice[startIndex:nextIndex])

		startIndex = nextIndex
		nextIndex = nextIndex + entriesPerBatch
	}

	if remainder != 0 {
		batch = append(batch, slice[startIndex:])
	}

	return batch
}

func (p *Publisher) runBatchPublisher(ctx context.Context) {
	var quit bool

	p.log.Debugf("publisher id '%s' starting", p.Subject)

	lastArrivedAt := time.Now()

	p.looper.Loop(func() error {
		span, ctx := tracer.StartSpanFromContext(ctx, "natty.publisher.runBatchPublisher")
		defer span.Finish()

		p.QueueMutex.RLock()
		remaining := len(p.Queue)
		p.QueueMutex.RUnlock()

		if quit && remaining == 0 {
			p.Natty.DeletePublisher(ctx, p.Subject)
			// empty queue, sleep for a bit and then loop again to check for new messages
			time.Sleep(time.Millisecond * 100)
			return nil
		}

		// Should we shutdown?
		select {
		case <-ctx.Done(): // DeletePublisher context
			p.log.Debugf("publisher id '%s' received notice to quit", p.Subject)
			quit = true

		case <-p.ServiceShutdownContext.Done():
			p.log.Debugf("publisher id '%s' received app shutdown signal, waiting for batch to be empty", p.Subject)
			quit = true
			p.looper.Quit()
		default:
			// NOOP
		}

		// No reason to keep goroutines running forever
		if remaining == 0 && time.Since(lastArrivedAt) > p.IdleTimeout {
			p.log.Debugf("idle timeout reached (%s); exiting", p.IdleTimeout)

			p.Natty.DeletePublisher(ctx, p.Subject)
			return nil
		}

		if remaining == 0 {
			// Queue is empty, nothing to do
			return nil
		}

		p.QueueMutex.Lock()
		tmpQueue := make([]*message, len(p.Queue))
		copy(tmpQueue, p.Queue)
		p.Queue = make([]*message, 0)
		p.QueueMutex.Unlock()

		lastArrivedAt = time.Now()

		if err := p.writeMessagesBatch(ctx, tmpQueue); err != nil {
			p.log.Error(err)
		}

		return nil
	})

	p.log.Debugf("publisher id '%s' exiting", p.Subject)
}
