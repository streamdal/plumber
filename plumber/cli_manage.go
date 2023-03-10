package plumber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/hokaccha/go-prettyjson"
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/batchcorp/natty"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (p *Plumber) HandleManageCmd() error {
	if len(p.KongCtx.Args) < 3 {
		return fmt.Errorf("unexpected number of CLI args: %d", len(p.KongCtx.Args))
	}

	// Setup gRPC conn + client
	var opts []grpc.DialOption

	if p.CLIOptions.Manage.GlobalOptions.ManageUseTls {
		tlsConfig, err := natty.GenerateTLSConfig(
			p.CLIOptions.Manage.GlobalOptions.ManageTlsCaFile,
			p.CLIOptions.Manage.GlobalOptions.ManageTlsCertFile,
			p.CLIOptions.Manage.GlobalOptions.ManageTlsKeyFile,
			p.CLIOptions.Manage.GlobalOptions.ManageInsecureTls, // TODO: Protos should be renamed to skip verify
		)

		if err != nil {
			return errors.Wrap(err, "failed to generate TLS config")
		}

		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	} else {
		// grpc.WithInsecure() is a required opt to disable use of TLS
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(p.CLIOptions.Manage.GlobalOptions.ManageAddress, opts...)
	if err != nil {
		return errors.Wrap(err, "failed to dial gRPC server")
	}

	timeout := time.Second * time.Duration(p.CLIOptions.Manage.GlobalOptions.ManageTimeoutSeconds)

	ctx, _ := context.WithTimeout(context.Background(), timeout)

	client := protos.NewPlumberServerClient(conn)

	cmd := p.CLIOptions.Global.XCommands[1] + " " + p.CLIOptions.Global.XCommands[2]

	switch cmd {
	// Get
	case "get connection":
		if p.CLIOptions.Manage.Get.Connection.Id == "" {
			err = p.HandleGetAllConnectionsCmd(ctx, client)
		} else {
			err = p.HandleGetConnectionCmd(ctx, client)
		}
	//case "get relay":
	//	if p.CLIOptions.Manage.Get.Relay.Id == "" {
	//		err = p.HandleGetAllRelaysCmd(ctx, client)
	//	} else {
	//		err = p.HandleGetRelayCmd(ctx, client)
	//	}
	case "get tunnel":
		if p.CLIOptions.Manage.Get.Tunnel.Id == "" {
			err = p.HandleGetAllTunnelsCmd(ctx, client)
		} else {
			err = p.HandleGetTunnelCmd(ctx, client)
		}

	// Create
	case "create connection":
		err = p.HandleCreateConnectionCmd(ctx, client)
	//case "create relay":
	//	err = p.HandleCreateRelayCmd(ctx, client)
	case "create tunnel":
		err = p.HandleCreateTunnelCmd(ctx, client)

	// Delete
	case "delete connection":
		err = p.HandleDeleteConnectionCmd(ctx, client)
	//case "delete relay":
	//	err = p.HandleDeleteRelayCmd(ctx, client)
	case "delete tunnel":
		err = p.HandleDeleteTunnelCmd(ctx, client)

	//// Stop
	//case "stop relay":
	//	err = p.HandleStopRelayCmd(ctx, client)
	case "stop tunnel":
		err = p.HandleStopTunnelCmd(ctx, client)
	//
	//// Resume
	//case "resume relay":
	//	err = p.HandleResumeRelayCmd(ctx, client)
	case "resume tunnel":
		err = p.HandleResumeTunnelCmd(ctx, client)
	default:
		return fmt.Errorf("unrecognized command: '%s'", cmd)
	}

	if err != nil {
		return errors.Wrapf(err, "error handling command '%s'", cmd)
	}

	return nil
}

func (p *Plumber) displayJSON(input map[string]string) {
	data, err := json.Marshal(input)
	if err != nil {
		p.log.Errorf("failed to marshal JSON: %s", err)
		return
	}

	if !p.CLIOptions.Manage.GlobalOptions.DisablePretty {
		colorized, err := prettyjson.Format(data)
		if err != nil {
			p.log.Errorf("failed to colorize JSON: %s", err)
			return
		}

		data = colorized
	}

	fmt.Println(string(data))
}

func (p *Plumber) EnqueueManage(event posthog.Capture) {
	if event.Properties == nil {
		event.Properties = make(map[string]interface{})
	}

	if _, ok := event.Properties["use_tls"]; !ok {
		event.Properties["use_tls"] = p.CLIOptions.Manage.GlobalOptions.ManageUseTls
	}

	if _, ok := event.Properties["insecure_tls"]; !ok {
		event.Properties["insecure_tls"] = p.CLIOptions.Manage.GlobalOptions.ManageInsecureTls
	}

	if _, ok := event.Properties["disable_pretty"]; !ok {
		event.Properties["disable_pretty"] = p.CLIOptions.Manage.GlobalOptions.DisablePretty
	}

	event.Properties["default_manage_token"] = false

	if p.CLIOptions.Manage.GlobalOptions.ManageToken == "streamdal" {
		event.Properties["default_manage_token"] = true
	}

	event.Properties["default_manage_address"] = false

	if p.CLIOptions.Manage.GlobalOptions.ManageAddress == "localhost:9090" {
		event.Properties["default_manage_address"] = true
	}

	p.Telemetry.Enqueue(event)
}

func (p *Plumber) displayProtobuf(msg proto.Message) error {
	marshaller := jsonpb.Marshaler{
		Indent: "  ",
	}

	var buf bytes.Buffer

	if err := marshaller.Marshal(&buf, msg); err != nil {
		return errors.Wrap(err, "failed to marshal response")
	}

	output := buf.Bytes()

	if !p.CLIOptions.Manage.GlobalOptions.DisablePretty {
		colorized, err := prettyjson.Format(buf.Bytes())
		if err != nil {
			return errors.Wrap(err, "unable to colorize response")
		}

		output = colorized
	}

	fmt.Println(string(output))

	return nil
}
