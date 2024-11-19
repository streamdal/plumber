package plumber

import (
	"context"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"
)

func (p *Plumber) HandleGetTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetTunnel(ctx, &protos.GetTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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

func (p *Plumber) HandleCreateTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	// Create tunnel options from CLI opts
	tunnelOpts, err := generateTunnelOptionsForManageCreate(p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "failed to generate tunnel options")
	}

	resp, err := client.CreateTunnel(ctx, &protos.CreateTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		Opts: tunnelOpts,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	p.displayProtobuf(resp)

	return nil
}

func (p *Plumber) HandleDeleteTunnelCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.DeleteTunnel(ctx, &protos.DeleteTunnelRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		TunnelId: p.CLIOptions.Manage.Stop.Tunnel.Id,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		TunnelId: p.CLIOptions.Manage.Resume.Tunnel.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func generateTunnelOptionsForManageCreate(cliOpts *opts.CLIOptions) (*opts.TunnelOptions, error) {
	tunnelOpts := &opts.TunnelOptions{
		ApiToken:            cliOpts.Manage.Create.Tunnel.TunnelToken,
		ConnectionId:        cliOpts.Manage.Create.Tunnel.ConnectionId,
		XGrpcAddress:        cliOpts.Manage.Create.Tunnel.XTunnelAddress,
		XGrpcTimeoutSeconds: cliOpts.Manage.Create.Tunnel.XTunnelTimeoutSeconds,
		XGrpcInsecure:       cliOpts.Manage.Create.Tunnel.XTunnelInsecure,
		Name:                cliOpts.Manage.Create.Tunnel.Name,
	}

	// We need to assign the CLI opts to the correct backend field in the request.
	// As in, cliOpts.Manage.Create.Tunnel.Kafka needs to be assigned to tunnelOpts.Kafka
	// (if kafka was specified). To do this, we will rely on a helper func that
	// is generated via code-gen in plumber-schemas.

	// Some backends have a dash, remove it; all further normalization will be
	// taken care of by the Merge function.
	backendName := strings.Replace(cliOpts.Global.XBackend, "-", "", -1)

	tunnelOpts.Kafka = &opts.TunnelGroupKafkaOptions{Args: cliOpts.Manage.Create.Tunnel.Kafka}

	if err := opts.MergeTunnelOptions(backendName, tunnelOpts, cliOpts.Manage.Create.Tunnel); err != nil {
		return nil, errors.Wrap(err, "unable to merge relay options")
	}

	return tunnelOpts, nil
}
