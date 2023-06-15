package bus

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/server/types"
)

func (b *Bus) doCounter(ctx context.Context, msg *Message) error {
	counter := &types.Counter{}
	if err := json.Unmarshal(msg.Data, &counter); err != nil {
		return errors.Wrap(err, "unable to unmarshal message into types.Counter")
	}

	b.log.Infof("Received counter message: %+v", counter)

	b.config.Actions.Counter(ctx, counter)

	return nil
}
