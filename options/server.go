package options

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	DefaultNodeID = "plumber1"
)

func HandleServerFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("listen-address", "gRPC listen address and port. Ex: localhost:9000").
		Default("localhost:9090").
		StringVar(&opts.Server.ListenAddress)

	cmd.Flag("token", "Plumber authentication token").
		Default("batchcorp").
		StringVar(&opts.Server.AuthToken)

	cmd.Flag("node-id", "Unique node ID for this plumber instance").
		Default(DefaultNodeID).
		StringVar(&opts.Server.NodeID)

	cmd.Flag("initial-cluster", "Comma separated list of key=value pairs containing ALL addresses of plumber peers").
		Default(DefaultNodeID + "=http://127.0.0.1:2380").
		StringVar(&opts.Server.InitialCluster)

	cmd.Flag("advertise-peer-url", "Plumber embedded etcd server address that will be advertised to plumber peers").
		Default("http://127.0.0.1:2380").
		URLVar(&opts.Server.AdvertisePeerURL)

	cmd.Flag("advertise-client-url", "Plumber embedded etcd client address that will be advertised to plumber peers").
		Default("http://127.0.0.1:2379").
		URLVar(&opts.Server.AdvertiseClientURL)

	cmd.Flag("listener-peer-url", "Plumber embedded etcd server address that we will listen on").
		Default("http://127.0.0.1:2380").
		URLVar(&opts.Server.ListenerPeerURL)

	cmd.Flag("listener-client-url", "Plumber embedded etcd client address that we will listen on").
		Default("http://127.0.0.1:2379").
		URLVar(&opts.Server.ListenerClientURL)

	cmd.Flag("peer-token", "Auth token used between plumber peers (must be the same everywhere)").
		Default("secret").
		StringVar(&opts.Server.PeerToken)

	cmd.Flag("debug", "Enable debug logging").
		BoolVar(&opts.Debug)
}
