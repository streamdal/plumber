package plumber

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"
)

func (p *Plumber) HandleGetConnectionCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetConnection(ctx, &protos.GetConnectionRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		ConnectionId: p.CLIOptions.Manage.Get.Connection.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleGetAllConnectionsCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetAllConnections(ctx, &protos.GetAllConnectionsRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleDeleteConnectionCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.DeleteConnection(ctx, &protos.DeleteConnectionRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		ConnectionId: p.CLIOptions.Manage.Delete.Connection.Id,
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
func (p *Plumber) HandleCreateConnectionCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.CreateConnection(ctx, &protos.CreateConnectionRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ServerToken,
		},
		Options: &opts.ConnectionOptions{
			Name:  p.CLIOptions.Manage.Create.Connection.Name,
			Notes: p.CLIOptions.Manage.Create.Connection.Notes,
			Conn:  nil, // TODO: How to create this object dynamically? We do this elsewhere...
		},
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	p.displayProtobuf(resp)

	return nil
}
