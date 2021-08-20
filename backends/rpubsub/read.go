package rpubsub

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
)

func (r *Redis) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	ps := r.client.Subscribe(ctx, r.Options.RedisPubSub.Channels...)
	defer ps.Unsubscribe(ctx)

	r.log.Info("Listening for message(s) ...")

	count := 1

	msgCh := ps.Channel()

	for {
		select {
		case msg := <-msgCh:
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
				return nil
			}
		case <-ctx.Done():
			return nil
		default:
			// NOOP
		}
	}

	r.log.Debug("exiting")

	return nil
}
