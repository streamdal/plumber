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

	// Tunnel
	case CreateTunnel:
		err = b.doCreateTunnel(ctx, msg)
	case UpdateTunnel:
		err = b.doUpdateTunnel(ctx, msg)
	case DeleteTunnel:
		err = b.doDeleteTunnel(ctx, msg)
	case StopTunnel:
		err = b.doStopTunnel(ctx, msg)
	case ResumeTunnel:
		err = b.doResumeTunnel(ctx, msg)

	// RuleSet
	case CreateRuleSet:
		err = b.doCreateRuleSet(ctx, msg)
	case UpdateRuleSet:
		err = b.doUpdateRuleSet(ctx, msg)
	case DeleteRuleSet:
		err = b.doDeleteRuleSet(ctx, msg)

	default:
		llog.Debugf("unrecognized action '%s' in msg on subj '%s' - skipping", msg.Action, natsMsg.Subject)
	}

	if err != nil {

	}

	return nil
}
