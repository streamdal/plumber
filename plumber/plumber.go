package plumber

import (
	"context"

	"github.com/alecthomas/kong"
	"github.com/batchcorp/plumber-schemas/build/go/protos"
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
	cliMD   *desc.MessageDescriptor
	log     *logrus.Entry
}

// New instantiates a properly configured instance of Plumber or a config error
func New(cfg *Config) (*Plumber, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	// Used only when using plumber in CLI mode
	md, err := GenerateMessageDescriptor(cfg.CLIOptions)
	if err != nil {
		return nil, errors.Wrap(err, "unable to populate protobuf message descriptors")
	}

	return &Plumber{
		Config:  cfg,
		RelayCh: make(chan interface{}, 1),
		cliMD:   md,
		log:     logrus.WithField("pkg", "plumber"),
	}, nil
}

// Run is the main entrypoint to the plumber application
func (p *Plumber) Run() {
	var err error

	switch p.CLIOptions.Global.XAction {
	case "server":
		err = p.RunServer()
	case "batch":
		err = p.HandleBatchCmd() // TODO: This is next
	case "read":
		err = p.HandleReadCmd()
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
	if cliOpts.Global.XAction != "read" && cliOpts.Global.XAction != "write" && cliOpts.Global.XAction != "relay" {
		// No need to generate md's if not read, write or relay
		return nil, nil
	}

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
