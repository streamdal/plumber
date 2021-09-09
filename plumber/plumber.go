package plumber

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alecthomas/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/validate"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/printer"
)

var (
	ErrMissingShutdownCtx      = errors.New("ServiceShutdownCtx cannot be nil")
	ErrMissingMainShutdownFunc = errors.New("MainShutdownFunc cannot be nil")
	ErrMissingMainContext      = errors.New("MainContext cannot be nil")
	ErrMissingOptions          = errors.New("CLIOptions cannot be nil")
)

// Config contains configurable options for instantiating a new Plumber
type Config struct {
	PersistentConfig   *config.Config
	ServiceShutdownCtx context.Context
	MainShutdownFunc   context.CancelFunc
	MainShutdownCtx    context.Context
	CLIOptions         *protos.CLIOptions
	KongCtx            *kong.Context
}

type Plumber struct {
	*Config

	Etcd    *etcd.Etcd
	RelayCh chan interface{}

	cliMD      *desc.MessageDescriptor
	cliConnCfg *protos.ConnectionConfig
	log        *logrus.Entry
}

// New instantiates a properly configured instance of Plumber or a config error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	p := &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		log:     logrus.WithField("pkg", "plumber"),
	}

	if ActionUsesBackend(cfg.CLIOptions.Global.XAction) {
		// Used only when using plumber in CLI mode
		md, err := GenerateMessageDescriptor(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
		}

		connCfg, err := GenerateConnectionConfig(cfg.CLIOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate connection config")
		}

		p.cliMD = md
		p.cliConnCfg = connCfg
	}

	return p, nil
}

// ActionUsesBackend checks the action string to determine if a backend will
// need to be utilized. We need such a check in order to determine if we will
// need to create a connection config or find message descriptors when running
// plumber in CLI mode.
func ActionUsesBackend(action string) bool {
	switch action {
	case "read":
		return true
	case "relay":
		return true
	case "write":
		return true
	case "lag":
		return true
	}

	return false
}

func GenerateConnectionConfig(cfg *protos.CLIOptions) (*protos.ConnectionConfig, error) {
	if cfg == nil {
		return nil, errors.New("cli options config cannot be nil")
	}

	connCfg := &protos.ConnectionConfig{
		Name:  "plumber-cli",
		Notes: "Generated via plumber-cli",
	}

	// We are looking for the individual conn located at: cfg.CLIOptions.$action.ReadOpts.$backendName.XConn
	lookupStrings := []string{
		cfg.Global.XAction,
		cfg.Global.XAction + "Opts",
		cfg.Global.XBackend,
		"XConn",
	}

	var value reflect.Value
	var err error

	value, err = LookupI(cfg, lookupStrings...)
	if err != nil {
		return nil, fmt.Errorf("unable to lookup connection info for backend '%s': %s",
			cfg.Global.XBackend, err)
	}

	switch value.Interface().(type) {
	case args.KafkaConn:
		connCfg.Conn = &protos.ConnectionConfig_Kafka{value.Interface().(*args.KafkaConn)}
	default:
		return nil, fmt.Errorf("unsupported backend type '%s'", cfg.Global.XBackend)
	}

	// TODO: This is where we're at. Need to spell out each of the types unfortunately :(

	// If it's a read, look under cfg.CLIOptions.Read.ReadOpts.$backendName.XConn
	// If it's a write, look under cfg.CLIOptions.Write.WriteOpts.$backendName.XConn
	// If it's a relay, look under cfg.CLIOptions.Relay.RelayOpts.$backendName.XConn
	// etc.

	//connCfg := &protos.ConnectionConfig{
	//	Name:  "plumber-cli",
	//	Notes: "Connection config generated via plumber",
	//}
	//
	//connCfg.Conn = &protos.ConnectionConfig_Kafka{
	//	Kafka: cfg.Read.ReadOpts.Kafka.XConn,
	//}

	return nil, nil
}

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch p.CLIOptions.Global.XAction {
	case "server":
		err = p.RunServer() // DONE
	case "batch":
		err = p.HandleBatchCmd() // DONE
	case "read":
		err = p.HandleReadCmd() // TODO: Am here
	case "write":
		err = p.HandleWriteCmd()
	case "relay":
		printer.PrintRelayOptions(p.CLIOptions)
		err = p.HandleRelayCmd()
	case "dynamic":
		err = p.HandleDynamicCmd()
	case "lag":
		err = p.HandleLagCmd()
	// TODO: should github cmd be in plumber?
	//case "github":
	//	err = p.HandleGithubCmd()
	default:
		logrus.Fatalf("unrecognized command: %s", p.CLIOptions.Global.XAction)
	}

	if err != nil {
		logrus.Fatalf("Unable to complete command: %s", err)
	}
}

func GenerateMessageDescriptor(cliOpts *protos.CLIOptions) (*desc.MessageDescriptor, error) {
	if cliOpts.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_JSONPB ||
		cliOpts.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {

		logrus.Debug("attempting to find decoding protobuf descriptors")

		pbDirs := cliOpts.Read.DecodeOptions.ProtobufSettings.ProtobufDirs
		pbRootMessage := cliOpts.Read.DecodeOptions.ProtobufSettings.ProtobufRootMessage

		if err := validate.ProtobufOptions(pbDirs, pbRootMessage); err != nil {
			return nil, errors.Wrap(err, "unable to validate protobuf settings for decode")
		}

		md, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find root message descriptor for decode")
		}

		return md, nil
	}

	if cliOpts.Write.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
		logrus.Debug("attempting to find encoding protobuf descriptors")

		pbDirs := cliOpts.Write.EncodeOptions.ProtobufSettings.ProtobufDirs
		pbRootMessage := cliOpts.Write.EncodeOptions.ProtobufSettings.ProtobufRootMessage

		if err := validate.ProtobufOptions(pbDirs, pbRootMessage); err != nil {
			return nil, errors.Wrap(err, "unable to validate protobuf settings for encode")
		}

		md, err := pb.FindMessageDescriptor(pbDirs, pbRootMessage)
		if err != nil {
			return nil, errors.Wrap(err, "unable to find root message descriptor for encode")
		}

		return md, nil
	}

	return nil, nil
}

// validateConfig ensures all correct values for Config are passed
func validateConfig(cfg *Config) error {
	if cfg.ServiceShutdownCtx == nil {
		return ErrMissingShutdownCtx
	}

	if cfg.CLIOptions == nil {
		return ErrMissingOptions
	}

	if cfg.MainShutdownCtx == nil {
		return ErrMissingMainContext
	}

	if cfg.MainShutdownFunc == nil {
		return ErrMissingMainShutdownFunc
	}

	return nil
}
