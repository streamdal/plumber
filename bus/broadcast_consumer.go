package bus

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// Returning an error will cause the error to be bubbled up to natty which will
// log the error and push it to the error channel (if set).
func (b *Bus) broadcastCallback(ctx context.Context, natsMsg *nats.Msg) error {
	llog := b.log.WithField("method", "broadcastCallback")

	defer natsMsg.Ack()

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

	var err error

	// Add actions here that the consumer should respond to
	switch msg.Action {

	// Connection
	case CreateConnection:
		err = b.doCreateConnection(ctx, msg)
	case UpdateConnection:
		err = b.doUpdateConnection(ctx, msg)
	case DeleteConnection:
		err = b.doDeleteConnection(ctx, msg)

	// Relay
	case CreateRelay:
		err = b.doCreateRelay(ctx, msg)
	case UpdateRelay:
		err = b.doUpdateRelay(ctx, msg)
	case DeleteRelay:
		err = b.doDeleteRelay(ctx, msg)
	case StopRelay:
		err = b.doStopRelay(ctx, msg)
	case ResumeRelay:
		err = b.doResumeRelay(ctx, msg)

	// Dynamic
	case CreateDynamic:
		err = b.doCreateDynamic(ctx, msg)
	case UpdateDynamic:
		err = b.doUpdateDynamic(ctx, msg)
	case DeleteDynamic:
		err = b.doDeleteDynamic(ctx, msg)
	case StopDynamic:
		err = b.doStopDynamic(ctx, msg)
	case ResumeDynamic:
		err = b.doResumeDynamic(ctx, msg)

	default:
		llog.Debugf("unrecognized action '%s' in msg on subj '%s' - skipping", msg.Action, natsMsg.Subject)
	}

	if err != nil {

	}

	return nil
}
