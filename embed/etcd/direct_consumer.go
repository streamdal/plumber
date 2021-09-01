package etcd

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *Etcd) handleDirectWatchResponse(ctx context.Context, resp *clientv3.WatchResponse) error {
	if resp == nil {
		return errors.New("response cannot be nil")
	}

	for _, v := range resp.Events {
		// We only care about creations
		if v.Type != clientv3.EventTypePut {
			continue
		}

		msg := &Message{}

		if err := json.Unmarshal(v.Kv.Value, msg); err != nil {
			e.log.Errorf("unable to unmarshal etcd key '%s' to message: %s", string(v.Kv.Key), err)
			continue
		}

		var err error

		// Add actions here that the consumer should respond to
		switch msg.Action {
		default:
			e.log.Debugf("unrecognized action '%s' for key '%s' - skipping", msg.Action, string(v.Kv.Key))
		}

		if err != nil {
			e.log.Errorf("unable to complete '%s' action for key '%s': %s",
				msg.Action, string(v.Kv.Key), err)
		}
	}

	return nil
}
