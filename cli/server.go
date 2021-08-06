package cli

import (
	"net/url"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	DefaultNodeID = "plumber1"
)

type ServerOptions struct {
	NodeID        string
	ListenAddress string
	AuthToken     string

	///////////// Embedded etcd settings /////////////////

	// InitialCluster should contain comma separated list of key=value pairs
	// of host:port entries for ALL peers in the cluster.
	// Example: server1=http://192.168.1.10:2380,server2=http://192.168.1.11:2380,server3=http://192.168.1.12:2380
	//
	// IMPORTANT: The list should include _this_ instance's address.
	InitialCluster string

	// AdvertisePeerURL contains the address of _this_ plumber instance's etcd server interface
	// This is usually something like http://local-ip:2380
	AdvertisePeerURL *url.URL

	// AdvertisePeerURL contains the address of _this_ plumber instance's etcd client interface
	// This is usually something like http://local-ip:2379
	AdvertiseClientURL *url.URL

	// ListenerPeerURL contains the address that _this_ plumber instance's etcd server should listen on
	// This is usually something like http://local-ip:2380
	ListenerPeerURL *url.URL

	// ListenerClientURL contains the address that _this_ plumber instance's etcd client should listen on
	// This is usually something like http://local-ip:2379
	ListenerClientURL *url.URL

	// PeerToken is the token that ALL cluster members should use/share. If this token
	// does not match on one of the plumber instances - it won't be able to join the cluster.
	PeerToken string
}

func HandleServerFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("listen-address", "Listen address and port. Ex: localhost:8080").
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
