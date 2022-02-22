package plumber

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/pkg/errors"
)

func (p *Plumber) HandleGetRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetRelay(ctx, &protos.GetRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		RelayId: p.CLIOptions.Manage.Get.Relay.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": "no such relay id"})
		return nil
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleGetAllRelaysCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetAllRelays(ctx, &protos.GetAllRelaysRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
	})

	if err != nil {
		return errors.Wrap(err, "failed to get all relays")
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleResumeRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.ResumeRelay(ctx, &protos.ResumeRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		RelayId: p.CLIOptions.Manage.Resume.Relay.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleStopRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.StopRelay(ctx, &protos.StopRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		RelayId: p.CLIOptions.Manage.Resume.Relay.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleDeleteRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.DeleteRelay(ctx, &protos.DeleteRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		RelayId: p.CLIOptions.Manage.Resume.Relay.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

// TODO: Implement
func (p *Plumber) HandleCreateRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	fmt.Println("create relay")

	return nil
}
