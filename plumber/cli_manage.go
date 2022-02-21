package plumber

import (
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
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
		if p.CLIOptions.Manage.GlobalOptions.ServerInsecureTls {
			opts = append(opts, grpc.WithInsecure())
		}
	}

	conn, err := grpc.Dial(p.CLIOptions.Manage.GlobalOptions.ServerAddress, opts...)
	if err != nil {
		return errors.Wrap(err, "failed to dial gRPC server")
	}

	client := protos.NewPlumberServerClient(conn)

	cmd := p.KongCtx.Args[1] + " " + p.KongCtx.Args[2]

	switch cmd {
	// Get
	case "get connection":
		if p.CLIOptions.Manage.Get.Connection.Id == "" {
			err = p.HandleGetAllConnectionsCmd(client)
		} else {
			err = p.HandleGetConnectionCmd(client)
		}
	case "get relay":
		if p.CLIOptions.Manage.Get.Relay.Id == "" {
			err = p.HandleGetAllRelaysCmd(client)
		} else {
			err = p.HandleGetRelayCmd(client)
		}
	case "get tunnel":
		if p.CLIOptions.Manage.Get.Tunnel.Id == "" {
			err = p.HandleGetAllTunnelsCmd(client)
		} else {
			err = p.HandleGetTunnelCmd(client)
		}

	// Create
	case "create connection":
		err = p.HandleCreateConnectionCmd(client)
	case "create relay":
		err = p.HandleCreateRelayCmd(client)
	case "create tunnel":
		err = p.HandleCreateTunnelCmd(client)

	// Delete
	case "delete connection":
		err = p.HandleDeleteConnectionCmd(client)
	case "delete relay":
		err = p.HandleDeleteRelayCmd(client)
	case "delete tunnel":
		err = p.HandleDeleteTunnelCmd(client)

	// Stop
	case "stop relay":
		err = p.HandleStopRelayCmd(client)
	case "stop tunnel":
		err = p.HandleStopTunnelCmd(client)

	// Resume
	case "resume relay":
		err = p.HandleResumeRelayCmd(client)
	case "resume tunnel":
		err = p.HandleResumeTunnelCmd(client)
	default:
		return fmt.Errorf("unrecognized comand: %s", p.KongCtx.Args[2])
	}

	if err != nil {
		return errors.Wrapf(err, "error handling command '%s'", cmd)
	}

	return nil
}

////////////////// GET //////////////////

func (p *Plumber) HandleGetConnectionCmd(client protos.PlumberServerClient) error {
	fmt.Println("get connection")
	return nil
}

func (p *Plumber) HandleGetRelayCmd(client protos.PlumberServerClient) error {
	fmt.Println("get relay")

	return nil
}

func (p *Plumber) HandleGetTunnelCmd(client protos.PlumberServerClient) error {
	fmt.Println("get tunnel")

	return nil
}

func (p *Plumber) HandleGetAllConnectionsCmd(client protos.PlumberServerClient) error {
	fmt.Println("get all connections")
	return nil
}

func (p *Plumber) HandleGetAllRelaysCmd(client protos.PlumberServerClient) error {
	fmt.Println("get all relays")

	return nil
}

func (p *Plumber) HandleGetAllTunnelsCmd(client protos.PlumberServerClient) error {
	fmt.Println("get all tunnels")

	return nil
}

////////////////// CREATE //////////////////

func (p *Plumber) HandleCreateConnectionCmd(client protos.PlumberServerClient) error {
	fmt.Println("create connection")

	return nil
}

func (p *Plumber) HandleCreateRelayCmd(client protos.PlumberServerClient) error {
	fmt.Println("create relay")

	return nil
}

func (p *Plumber) HandleCreateTunnelCmd(client protos.PlumberServerClient) error {
	fmt.Println("create tunnel")

	return nil
}

/////////////////// DELETE //////////////////

func (p *Plumber) HandleDeleteConnectionCmd(client protos.PlumberServerClient) error {
	fmt.Println("delete connection")

	return nil
}

func (p *Plumber) HandleDeleteRelayCmd(client protos.PlumberServerClient) error {
	fmt.Println("delete relay")

	return nil
}

func (p *Plumber) HandleDeleteTunnelCmd(client protos.PlumberServerClient) error {
	fmt.Println("delete tunnel")

	return nil
}

////////////////// STOP //////////////////

func (p *Plumber) HandleStopRelayCmd(client protos.PlumberServerClient) error {
	fmt.Println("stop relay")

	return nil
}

func (p *Plumber) HandleStopTunnelCmd(client protos.PlumberServerClient) error {
	fmt.Println("stop tunnel")

	return nil
}

//////////////////// RESUME //////////////////

func (p *Plumber) HandleResumeRelayCmd(client protos.PlumberServerClient) error {
	fmt.Println("resume relay")

	return nil
}

func (p *Plumber) HandleResumeTunnelCmd(client protos.PlumberServerClient) error {
	fmt.Println("resume tunnel")

	return nil
}
