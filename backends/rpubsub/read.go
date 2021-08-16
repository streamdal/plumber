package rpubsub

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func (r *Redis) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	ps := r.client.Subscribe(ctx, r.Options.RedisPubSub.Channels...)
	defer ps.Unsubscribe(ctx)

	r.log.Info("Listening for message(s) ...")

	count := 1

	for {
		msg, err := ps.ReceiveMessage(ctx)
		if err != nil {
			util.WriteError(r.log, errorChan, errors.Wrap(err, "error receiving message"))
			return err
		}

		resultsChan <- &types.ReadMessage{
			Value: []byte(msg.Payload),
			Metadata: map[string]interface{}{
				"pattern": msg.Pattern,
				"channel": msg.Channel,
			},
			ReceivedAt: time.Now().UTC(),
			Num:        count,
			Raw:        msg,
		}

		count++

		if !r.Options.Read.Follow {
			break
		}
	}

	r.log.Debug("exiting")

	return nil
}
