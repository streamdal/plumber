package plumber

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/pkg/errors"
)

func (p *Plumber) HandleGetTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetTunnel(ctx, &protos.GetTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		TunnelId: p.CLIOptions.Manage.Get.Tunnel.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": "no such tunnel id"})
		return nil
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleGetAllTunnelsCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetAllTunnels(ctx, &protos.GetAllTunnelsRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
	})

	if err != nil {
		return errors.Wrap(err, "failed to get all tunnels")
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

// TODO: Implement
func (p *Plumber) HandleCreateTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	return nil
}

func (p *Plumber) HandleDeleteTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.DeleteTunnel(ctx, &protos.DeleteTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		TunnelId: p.CLIOptions.Manage.Delete.Tunnel.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleStopTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.StopTunnel(ctx, &protos.StopTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		TunnelId: p.CLIOptions.Manage.Delete.Tunnel.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleResumeTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.ResumeTunnel(ctx, &protos.ResumeTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		TunnelId: p.CLIOptions.Manage.Delete.Tunnel.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}
