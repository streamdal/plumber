package plumber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/hokaccha/go-prettyjson"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func (p *Plumber) HandleManageCmd() error {
	if len(p.KongCtx.Args) < 3 {
		return fmt.Errorf("unexpected number of CLI args: %d", len(p.KongCtx.Args))
	}

	// Setup gRPC conn + client
	var opts []grpc.DialOption

	if p.CLIOptions.Manage.GlobalOptions.ServerUseTls {
		// TODO: Need to be able to set certs + double check insecure skip verify
	} else {
		// grpc.WithInsecure() is a required opt to disable use of TLS
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(p.CLIOptions.Manage.GlobalOptions.ServerAddress, opts...)
	if err != nil {
		return errors.Wrap(err, "failed to dial gRPC server")
	}

	// TODO: Should allow timeout to be specified via CLI
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)

	client := protos.NewPlumberServerClient(conn)

	cmd := p.KongCtx.Args[1] + " " + p.KongCtx.Args[2]

	switch cmd {
	// Get
	case "get connection":
		if p.CLIOptions.Manage.Get.Connection.Id == "" {
			err = p.HandleGetAllConnectionsCmd(ctx, client)
		} else {
			err = p.HandleGetConnectionCmd(ctx, client)
		}
	case "get relay":
		if p.CLIOptions.Manage.Get.Relay.Id == "" {
			err = p.HandleGetAllRelaysCmd(ctx, client)
		} else {
			err = p.HandleGetRelayCmd(ctx, client)
		}
	case "get tunnel":
		if p.CLIOptions.Manage.Get.Tunnel.Id == "" {
			err = p.HandleGetAllTunnelsCmd(ctx, client)
		} else {
			err = p.HandleGetTunnelCmd(ctx, client)
		}

	// Create
	case "create connection":
		err = p.HandleCreateConnectionCmd(ctx, client)
	case "create relay":
		err = p.HandleCreateRelayCmd(ctx, client)
	case "create tunnel":
		err = p.HandleCreateTunnelCmd(ctx, client)

	// Delete
	case "delete connection":
		err = p.HandleDeleteConnectionCmd(ctx, client)
	case "delete relay":
		err = p.HandleDeleteRelayCmd(ctx, client)
	case "delete tunnel":
		err = p.HandleDeleteTunnelCmd(ctx, client)

	// Stop
	case "stop relay":
		err = p.HandleStopRelayCmd(ctx, client)
	case "stop tunnel":
		err = p.HandleStopTunnelCmd(ctx, client)

	// Resume
	case "resume relay":
		err = p.HandleResumeRelayCmd(ctx, client)
	case "resume tunnel":
		err = p.HandleResumeTunnelCmd(ctx, client)
	default:
		return fmt.Errorf("unrecognized comand: %s", p.KongCtx.Args[2])
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

	// TODO: Add --pretty to CLI protos
	if true {
		colorized, err := prettyjson.Format(data)
		if err != nil {
			p.log.Errorf("failed to colorize JSON: %s", err)
			return
		}

		data = colorized
	}

	fmt.Println(string(data))
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

	// TODO: Add pretty to CLI opts
	if true {
		colorized, err := prettyjson.Format(buf.Bytes())
		if err != nil {
			return errors.Wrap(err, "unable to colorize response")
		}

		output = colorized
	}

	fmt.Println(string(output))

	return nil
}
