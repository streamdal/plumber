package plumber

import (
	"context"
	"strings"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"

	"github.com/streamdal/plumber/validate"
)

func (p *Plumber) HandleGetRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	resp, err := client.GetRelay(ctx, &protos.GetRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		RelayId: p.CLIOptions.Manage.Stop.Relay.Id,
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
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		RelayId: p.CLIOptions.Manage.Delete.Relay.Id,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
	}

	if err := p.displayProtobuf(resp); err != nil {
		return errors.Wrap(err, "failed to display response")
	}

	return nil
}

func (p *Plumber) HandleCreateRelayCmd(ctx context.Context, client protos.PlumberServerClient) error {
	// Just in case
	if p.CLIOptions == nil || p.CLIOptions.Manage == nil || p.CLIOptions.Manage.Create == nil || p.CLIOptions.Manage.Create.Relay == nil {
		return errors.New("unable to create relay: missing CLI options")
	}

	if err := validate.ManageCreateRelayCmd(p.CLIOptions.Manage.Create.Relay); err != nil {
		return errors.Wrap(err, "unable to validate manage create relay options")
	}

	// Create relay options from CLI opts
	relayOpts, err := generateRelayOptionsForManageCreate(p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "failed to generate relay options")
	}

	resp, err := client.CreateRelay(ctx, &protos.CreateRelayRequest{
		Auth: &common.Auth{
			Token: p.CLIOptions.Manage.GlobalOptions.ManageToken,
		},
		Opts: relayOpts,
	})

	if err != nil {
		p.displayJSON(map[string]string{"error": err.Error()})
		return nil
	}

	p.displayProtobuf(resp)

	return nil
}

func generateRelayOptionsForManageCreate(cliOpts *opts.CLIOptions) (*opts.RelayOptions, error) {
	relayOpts := &opts.RelayOptions{
		CollectionToken:              cliOpts.Manage.Create.Relay.CollectionToken,
		BatchSize:                    cliOpts.Manage.Create.Relay.BatchSize,
		BatchMaxRetry:                cliOpts.Manage.Create.Relay.BatchMaxRetry,
		ConnectionId:                 cliOpts.Manage.Create.Relay.ConnectionId,
		NumWorkers:                   cliOpts.Manage.Create.Relay.NumWorkers,
		StreamdalIntegrationOptions:  cliOpts.Manage.Create.Relay.StreamdalIntegrationOptions,
		XStreamdalGrpcAddress:        cliOpts.Manage.Create.Relay.StreamdalGrpcAddress,
		XStreamdalGrpcDisableTls:     cliOpts.Manage.Create.Relay.StreamdalGrpcDisableTls,
		XStreamdalGrpcTimeoutSeconds: cliOpts.Manage.Create.Relay.StreamdalGrpcTimeoutSeconds,
	}

	// We need to assign the CLI opts to the correct backend field in the request.
	// As in, cliOpts.Manage.Create.Relay.Kafka needs to be assigned to relayOpts.Kafka
	// (if kafka was specified). To do this, we will rely on a helper func that
	// is generated via code-gen in plumber-schemas.

	// Some backends have a dash, remove it; all further normalization will be
	// taken care of by the Merge function.
	backendName := strings.Replace(cliOpts.Global.XBackend, "-", "", -1)

	if err := opts.MergeRelayOptions(backendName, relayOpts, cliOpts.Manage.Create.Relay); err != nil {
		return nil, errors.Wrap(err, "unable to merge relay options")
	}

	return relayOpts, nil
}
