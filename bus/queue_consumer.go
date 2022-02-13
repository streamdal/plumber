package bus

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// Returning an error will cause the error to be bubbled up to natty which will
// log the error and push it to the error channel (if set).
func (b *Bus) queueCallback(ctx context.Context, natsMsg *nats.Msg) error {
	defer natsMsg.Ack()

	llog := b.log.WithField("method", "queueCallback")

	if natsMsg == nil {
		return errors.New("response cannot be nil")
	}

	msg := &Message{}

	if err := json.Unmarshal(natsMsg.Data, msg); err != nil {
		llog.Errorf("unable to unmarshal NATS message on subj '%s': %s", natsMsg.Subject, err)
		return nil
	}

	if msg.EmittedBy == b.config.ServerOptions.NodeId {
		llog.Debugf("ignoring message emitted by self")
		return nil
	}

	llog.Debugf("received message on subj '%s'. Contents: %s", natsMsg.Subject, string(natsMsg.Data))

	// Add actions here that the consumer should respond to
	switch msg.Action {
	default:
		llog.Debugf("unrecognized action '%s' in msg on subj '%s' - skipping", msg.Action, natsMsg.Subject)
	}

	return nil
}
