package plumber

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/mcuadros/go-lookup"
	"github.com/pkg/errors"
)

func (p *Plumber) HandleGetConnectionCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetConnection(ctx, &protos.GetConnectionRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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

func (p *Plumber) HandleCreateConnectionCmd(ctx context.Context, client protos.PlumberServerClient) error {
	// Create conn from CLI options
	connOpts, err := generateConnOptionsForManageCreate(p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "failed to generate connection options")
	}

	resp, err := client.CreateConnection(ctx, &protos.CreateConnectionRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		Options: connOpts,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	p.displayProtobuf(resp)

	return nil
}

func generateConnOptionsForManageCreate(cliOpts *opts.CLIOptions) (*opts.ConnectionOptions, error) {
	connOpts := &opts.ConnectionOptions{}

	if cliOpts.Manage.Create.Connection.Name != "" {
		connOpts.Name = cliOpts.Manage.Create.Connection.Name
	}

	if cliOpts.Manage.Create.Connection.Notes != "" {
		connOpts.Notes = cliOpts.Manage.Create.Connection.Notes
	}

	// We know that the backend we are interested in was selected via .XBackend
	// Some backends have a dash, remove it
	backendName := strings.Replace(cliOpts.Global.XBackend, "-", "", -1)

	// We are looking for the individual conn located at: cfg.$action.$backendName.XConn
	lookupStrings := []string{"manage", "create", "connection", backendName}

	backendInterface, err := lookup.LookupI(cliOpts, lookupStrings...)
	if err != nil {
		return nil, fmt.Errorf("unable to lookup connection info for backendName '%s': %s",
			cliOpts.Global.XBackend, err)
	}

	conn, ok := opts.GenerateConnOpts(backendName, backendInterface.Interface())
	if !ok {
		return nil, errors.New("unable to generate connection options via proto func")
	}

	connOpts.Conn = conn

	return connOpts, nil

}
