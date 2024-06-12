// Package streamdal is a library that allows running of Client data pipelines against data
// This package is designed to be included in golang message bus libraries. The only public
// method is Process() which is used to run pipelines against data.
//
// Use of this package requires a running instance of a streamdal server©.
// The server can be downloaded at https://github.com/streamdal/streamdal/tree/main/apps/server
//
// The following environment variables must be set:
// - STREAMDAL_URL: The address of the Client server
// - STREAMDAL_TOKEN: The token to use when connecting to the Client server
// - STREAMDAL_SERVICE_NAME: The name of the service to identify it in the streamdal console
//
// Optional parameters:
// - STREAMDAL_DRY_RUN: If true, rule hits will only be logged, no failure modes will be ran
package streamdal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/tetratelabs/wazero"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"

	"github.com/streamdal/streamdal/sdks/go/hostfunc"
	"github.com/streamdal/streamdal/sdks/go/kv"
	"github.com/streamdal/streamdal/sdks/go/logger"
	"github.com/streamdal/streamdal/sdks/go/metrics"
	"github.com/streamdal/streamdal/sdks/go/server"
	"github.com/streamdal/streamdal/sdks/go/types"
)

// OperationType is used to indicate if the operation is a consumer or a producer
type OperationType int

// ClientType is used to indicate if this library is being used by a shim or directly (as an SDK)
type ClientType int

// SDKMode is how Process() will handle messages
// ModeSync = Process() will block until the pipeline is complete
// ModeAsync = Process() will return immediately and the pipeline will run in the background
type SDKMode int

// ProcessResponse is the response struct from a Process() call
type ProcessResponse protos.SDKResponse

// WazeroExecutionMode is used to indicate how the SDK will execute wasm modules
type WazeroExecutionMode int

const (
	// DefaultPipelineTimeoutDurationStr is the default timeout for a pipeline execution.
	DefaultPipelineTimeoutDurationStr = "100ms"

	// DefaultStepTimeoutDurationStr is the default timeout for a single step.
	DefaultStepTimeoutDurationStr = "10ms"

	// DefaultSamplingRate is the default number at which messages will be sampled
	// per SamplingIntervalSeconds if SamplingEnabled is set to true and SampleRate
	// is not specified in the config.
	DefaultSamplingRate = 1

	// DefaultSamplingIntervalSeconds is the default interval at which sampling will be done
	// if SamplingEnabled is set to true and SamplingIntervalSeconds is not specified in the config.
	DefaultSamplingIntervalSeconds = 1

	// DefaultModeAsyncWorkers is the default number of workers that will be spawned
	// by the SDK when config.Mode is set to ModeAsync.
	//
	// Higher amount == improved .Process() speeds but heavier resource usage.
	DefaultModeAsyncWorkers = 2

	// DefaultMetricsNumWorkers is the default number of workers that the SDK will
	// spawn for collecting and sending metrics to the configured Streamdal server.
	//
	// NOTE: Metrics are NOT optional. The server uses metrics sent by the SDK
	// clients to determine if clients are alive, display usage stats and many
	// other features.
	DefaultMetricsNumWorkers = 1

	// ReconnectSleep determines the length of time the SDK will wait between
	// reconnect attempts to streamdal server after the streamdal server goes away.
	ReconnectSleep = time.Second * 5

	// MaxWASMPayloadSize is the maximum size of data that can be sent to the WASM module
	MaxWASMPayloadSize = 1024 * 1024 // 1Mi

	// ClientTypeSDK & ClientTypeShim are referenced by shims and SDKs to indicate
	// in what context this SDK is being used.
	ClientTypeSDK  ClientType = 1
	ClientTypeShim ClientType = 2

	// OperationTypeConsumer and OperationTypeProducer are used to indicate the
	// type of operation the Process() call is performing.
	OperationTypeConsumer OperationType = 1
	OperationTypeProducer OperationType = 2

	ModeSync  SDKMode = SDKMode(protos.SDKMode_SDK_MODE_SYNC)
	ModeAsync SDKMode = SDKMode(protos.SDKMode_SDK_MODE_ASYNC)

	AbortAllStr     = "aborted all pipelines"
	AbortCurrentStr = "aborted current pipeline"
	AbortNoneStr    = "no abort condition"

	// ExecStatusTrue ExecStatusFalse ExecStatusError are used to indicate
	// the execution status of the _last_ step in the _last_ pipeline.
	ExecStatusTrue     = protos.ExecStatus_EXEC_STATUS_TRUE
	ExecStatusFalse    = protos.ExecStatus_EXEC_STATUS_FALSE
	ExecStatusError    = protos.ExecStatus_EXEC_STATUS_ERROR
	ExecStatusAsync    = protos.ExecStatus_EXEC_STATUS_ASYNC
	ExecStatusSampling = protos.ExecStatus_EXEC_STATUS_SAMPLING

	WazeroExecutionModeCompiler    WazeroExecutionMode = 1
	WazeroExecutionModeInterpreter WazeroExecutionMode = 2

	DefaultWazeroExecutionMode = WazeroExecutionModeCompiler
)

var (
	ErrEmptyConfig          = errors.New("config cannot be empty")
	ErrEmptyServiceName     = errors.New("data source cannot be empty")
	ErrEmptyOperationName   = errors.New("operation name cannot be empty")
	ErrInvalidOperationType = errors.New("operation type must be set to either OperationTypeConsumer or OperationTypeProducer")
	ErrEmptyComponentName   = errors.New("component name cannot be empty")
	ErrEmptyCommand         = errors.New("command cannot be empty")
	ErrEmptyProcessRequest  = errors.New("process request cannot be empty")

	// ErrMaxPayloadSizeExceeded is returned when the payload is bigger than MaxWASMPayloadSize
	ErrMaxPayloadSizeExceeded = fmt.Errorf("payload size exceeds maximum of '%d' bytes", MaxWASMPayloadSize)

	// ErrPipelineTimeout is returned when a pipeline exceeds the configured timeout
	ErrPipelineTimeout = errors.New("pipeline timeout exceeded")

	// ErrClosedClient is returned when .Process() is called after .Close()
	ErrClosedClient = errors.New("client is closed")
)

type IStreamdal interface {
	// Process is used to run data pipelines against data
	Process(ctx context.Context, req *ProcessRequest) *ProcessResponse

	// Close should be called when streamdal client is no longer being used;
	// will stop all active goroutines and clean up resources; client should NOT
	// be re-used after Close() is called.
	Close()
}

// Streamdal is the main struct for this library
type Streamdal struct {
	config         *Config
	functions      map[int]map[string]*function
	functionsMtx   *sync.RWMutex
	pipelines      map[string][]*protos.Pipeline // k: audienceStr
	pipelinesMtx   *sync.RWMutex
	serverClient   server.IServerClient
	metrics        metrics.IMetrics
	audiences      map[string]struct{} // k: audienceStr
	audiencesMtx   *sync.RWMutex
	sessionID      string
	kv             kv.IKV
	hf             *hostfunc.HostFunc
	tailsMtx       *sync.RWMutex
	tails          map[string]map[string]*Tail // k1: audienceStr k2: tailID
	pausedTailsMtx *sync.RWMutex
	pausedTails    map[string]map[string]*Tail // k1: audienceStr k2: tailID
	schemas        map[string]*protos.Schema   // k: audienceStr
	schemasMtx     *sync.RWMutex
	cancelFunc     context.CancelFunc
	wasmCache      map[string][]byte
	wasmCacheMtx   *sync.RWMutex
	funcCreateMtx  *sync.Mutex
	funcCreate     map[string]*sync.Mutex
	wazeroCache    wazero.CompilationCache

	// Sampling rate limiter, uses token bucket algo
	limiter *rate.Limiter

	asyncCh chan *ProcessRequest

	// Set to true when .Close() is called; used to prevent calling .Process()
	// when library instance is stopped.
	closed bool
}

type Config struct {
	// ServerURL the hostname and port for the gRPC API of Streamdal Server.
	// If this value is left empty, the library will not attempt to connect to
	// the server and New() will return nil.
	//
	// REQUIRED
	ServerURL string

	// ServerToken is the authentication token for the gRPC API of the Streamdal server
	// If this value is left empty, the library will not attempt to connect to the server
	// and New() will return nil.
	//
	// REQUIRED
	ServerToken string

	// ServiceName is the name that this library will identify as in the UI.
	//
	// REQUIRED
	ServiceName string

	// PipelineTimeout defines how long this library will allow a pipeline to run.
	//
	// Optional; default: 100ms
	PipelineTimeout time.Duration

	// StepTimeout defines how long this library will allow a single step to run.
	//
	// Optional; default: 10ms
	StepTimeout time.Duration

	// IgnoreStartupError defines how to handle an error on initial startup via
	// New(). If left as false, failure to complete startup (such as bad auth)
	// will cause New() to return an error. If true, the library will block and
	// continue trying to initialize. You may want to adjust this if you want
	// your application to behave a certain way on startup when the server
	// is unavailable.
	//
	// Optional; default: false
	IgnoreStartupError bool

	// If specified, library will connect to the server but won't apply any
	// pipelines.
	//
	// Optional; default: false
	DryRun bool

	// ShutdownCtx is a context that the library will listen to for cancellation
	// notices. Upon cancelling this context, SDK will stop all active goroutines
	// and free up all used resources.
	//
	// Optional; default: nil
	ShutdownCtx context.Context

	// Logger is a logger you can inject (such as logrus) to allow this library
	// to log output.
	//
	// Optional; default: nil
	Logger logger.Logger

	// Audiences is a list of audiences you can specify at registration time.
	// This is useful if you know your audiences in advance and want to populate
	// service groups in the Streamdal UI _before_ your code executes any .Process()
	// calls.
	//
	// NOTE: If you do not specify audiences here, they will only show up in
	// the UI _after_ the first call to .Process() completes!
	//
	// Optional; default: nil
	Audiences []*Audience

	// ClientType specifies whether this of the SDK is used in a shim library or
	// as a standalone SDK. This information is used for both debug info and to
	// help the library determine whether ServerURL and ServerToken should be
	// optional or required.
	//
	// Optional; default: ClientTypeSDK
	ClientType ClientType

	// EnableStdout enables ability for wasm modules to write to stdout
	EnableStdout bool

	// EnableStderr enables ability for wasm modules to write to stderr
	EnableStderr bool

	// SamplingEnabled enables sampling of data. This will cause Process() to
	// ignore some messages if the rate exceeds the SamplingRate per SamplingIntervalSeconds.
	// This is useful for high-throughput systems where you cannot .Process()
	// every message.
	SamplingEnabled bool

	// SamplingRate is the rate at which messages will be sampled per SamplingIntervalSeconds
	//
	// Default: 1
	SamplingRate int

	// SamplingIntervalSeconds is the interval at which sampling will be done.
	//
	// Default: 1
	SamplingIntervalSeconds int

	// SDKMode is how Process() will handle messages.
	// ModeSync = Process() will block until the pipeline is complete
	// ModeAsync = Process() will return immediately and the pipeline will run in the background via workers
	//
	// Default: ModeSync
	Mode SDKMode

	// ModeAsyncNumWorkers is the number of workers SDK will spawn when Mode is
	// set to ModeAsync.
	//
	// Default: 4
	ModeAsyncNumWorkers int

	// Number of workers that the SDK will run for collecting and sending metrics
	//
	// Default: 1
	MetricsNumWorkers int

	// WazeroExecutionMode defines how the SDK will execute wasm modules.
	//
	// WazeroExecutionModeCompiler = slow instantiation, faster execution
	// WazeroExecutionModeInterpreter = fast instantiation, slower execution
	//
	// Default: WazeroExecutionModeCompiler
	WazeroExecutionMode WazeroExecutionMode
}

// ProcessRequest is used to maintain a consistent API for the Process() call
type ProcessRequest struct {
	ComponentName string
	OperationType OperationType
	OperationName string
	Data          []byte
}

func New(cfg *Config) (*Streamdal, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	// We instantiate this library based on whether we have a Client URL+token or not.
	// If these are not provided, the wrapper library will not perform rule checks and
	// will act as normal
	if cfg.ServerURL == "" || cfg.ServerToken == "" {
		return nil, nil
	}

	// Derive a new cancellable context from the provided shutdown context. We
	// do this so that .Close() can cancel all running goroutines.
	ctx, cancelFunc := context.WithCancel(cfg.ShutdownCtx)
	cfg.ShutdownCtx = ctx

	serverClient, err := server.New(cfg.ServerURL, cfg.ServerToken)
	if err != nil {
		cancelFunc()
		return nil, errors.Wrapf(err, "failed to connect to streamdal server© '%s'", cfg.ServerURL)
	}

	m, err := metrics.New(&metrics.Config{
		ServerClient:   serverClient,
		WorkerPoolSize: cfg.MetricsNumWorkers,
		ShutdownCtx:    cfg.ShutdownCtx,
		Log:            cfg.Logger,
	})
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "failed to start metrics service")
	}

	kvInstance, err := kv.New(&kv.Config{
		Logger: cfg.Logger,
	})
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "failed to start kv service")
	}

	hf, err := hostfunc.New(kvInstance, cfg.Logger)
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "failed to create hostfunc instance")
	}

	s := &Streamdal{
		functions:      make(map[int]map[string]*function),
		functionsMtx:   &sync.RWMutex{},
		serverClient:   serverClient,
		pipelines:      make(map[string][]*protos.Pipeline),
		pipelinesMtx:   &sync.RWMutex{},
		audiences:      map[string]struct{}{},
		audiencesMtx:   &sync.RWMutex{},
		config:         cfg,
		metrics:        m,
		sessionID:      uuid.New().String(),
		kv:             kvInstance,
		hf:             hf,
		tailsMtx:       &sync.RWMutex{},
		tails:          make(map[string]map[string]*Tail),
		pausedTailsMtx: &sync.RWMutex{},
		pausedTails:    make(map[string]map[string]*Tail),
		schemasMtx:     &sync.RWMutex{},
		schemas:        make(map[string]*protos.Schema),
		cancelFunc:     cancelFunc,
		wasmCache:      make(map[string][]byte),
		wasmCacheMtx:   &sync.RWMutex{},

		// Used for blocking getFunctionFromCache() calls when a function is being created
		funcCreate:    make(map[string]*sync.Mutex),
		funcCreateMtx: &sync.Mutex{},
		wazeroCache:   wazero.NewCompilationCache(),
	}

	if cfg.DryRun {
		cfg.Logger.Warn("data pipelines running in dry run mode")
	}

	if cfg.SamplingEnabled {
		interval := time.Duration(cfg.SamplingIntervalSeconds/cfg.SamplingRate) * time.Second
		s.limiter = rate.NewLimiter(rate.Every(interval), cfg.SamplingRate)
	}

	if err := s.pullInitialPipelines(cfg.ShutdownCtx); err != nil {
		return nil, err
	}

	errCh := make(chan error)

	// Start register
	go func() {
		if err := s.register(director.NewFreeLooper(director.FOREVER, make(chan error, 1))); err != nil {
			errCh <- errors.Wrap(err, "register error")
		}
	}()

	// Start heartbeat
	go s.heartbeat(director.NewTimedLooper(director.FOREVER, time.Second, make(chan error, 1)))

	// Watch for shutdown so we can properly stop tails
	go s.watchForShutdown()

	// Make sure we were able to start without issues
	select {
	case err := <-errCh:
		return nil, errors.Wrap(err, "received error on startup")
	case <-time.After(time.Second * 5):
		// Start async workers if necessary
		if cfg.Mode == ModeAsync {
			s.asyncCh = make(chan *ProcessRequest, cfg.ModeAsyncNumWorkers*10)
			s.startAsyncWorkers()
		}

		return s, nil
	}
}

func (s *Streamdal) Close() {
	s.cancelFunc()
	s.closed = true
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return ErrEmptyConfig
	}

	if cfg.ShutdownCtx == nil {
		cfg.ShutdownCtx = context.Background()
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = os.Getenv("STREAMDAL_SERVICE_NAME")
		if cfg.ServiceName == "" {
			return ErrEmptyServiceName
		}
	}

	// Can be specified in config for lib use, or via envar for shim use
	if cfg.ServerURL == "" {
		cfg.ServerURL = os.Getenv("STREAMDAL_URL")
	}

	// Can be specified in config for lib use, or via envar for shim use
	if cfg.ServerToken == "" {
		cfg.ServerToken = os.Getenv("STREAMDAL_TOKEN")
	}

	// Can be specified in config for lib use, or via envar for shim use
	if os.Getenv("STREAMDAL_DRY_RUN") == "true" {
		cfg.DryRun = true
	}

	// Can be specified in config for lib use, or via envar for shim use
	if cfg.StepTimeout == 0 {
		to := os.Getenv("STREAMDAL_STEP_TIMEOUT")
		if to == "" {
			to = DefaultStepTimeoutDurationStr
		}

		timeout, err := time.ParseDuration(to)
		if err != nil {
			return errors.Wrapf(err, "unable to parse StepTimeout '%s'", to)
		}

		cfg.StepTimeout = timeout
	}

	// Can be specified in config for lib use, or via envar for shim use
	if cfg.PipelineTimeout == 0 {
		to := os.Getenv("STREAMDAL_PIPELINE_TIMEOUT")
		if to == "" {
			to = DefaultPipelineTimeoutDurationStr
		}

		timeout, err := time.ParseDuration(to)
		if err != nil {
			return errors.Wrapf(err, "unable to parse PipelineTimeout '%s'", to)
		}

		cfg.PipelineTimeout = timeout
	}

	// Default to NOOP logger if none is provided
	if cfg.Logger == nil {
		cfg.Logger = &logger.TinyLogger{}
	}

	// Default to ClientTypeSDK
	if cfg.ClientType != ClientTypeShim && cfg.ClientType != ClientTypeSDK {
		cfg.ClientType = ClientTypeSDK
	}

	if cfg.SamplingEnabled {
		if cfg.SamplingRate == 0 {
			cfg.SamplingRate = DefaultSamplingRate
		}

		if cfg.SamplingIntervalSeconds == 0 {
			cfg.SamplingIntervalSeconds = DefaultSamplingIntervalSeconds
		}
	}

	// Default to sync mode if none is specified
	if cfg.Mode == 0 {
		cfg.Mode = ModeSync
	}

	if cfg.ModeAsyncNumWorkers == 0 {
		cfg.ModeAsyncNumWorkers = DefaultModeAsyncWorkers
	}

	if cfg.MetricsNumWorkers == 0 {
		cfg.MetricsNumWorkers = DefaultMetricsNumWorkers
	}

	if cfg.WazeroExecutionMode == 0 {
		cfg.WazeroExecutionMode = DefaultWazeroExecutionMode
	}

	return nil
}

func validateProcessRequest(req *ProcessRequest) error {
	if req == nil {
		return ErrEmptyProcessRequest
	}

	if req.OperationName == "" {
		return ErrEmptyOperationName
	}

	if req.ComponentName == "" {
		return ErrEmptyComponentName
	}

	if req.OperationType != OperationTypeProducer && req.OperationType != OperationTypeConsumer {
		return ErrInvalidOperationType
	}

	return nil
}

func (s *Streamdal) watchForShutdown() {
	<-s.config.ShutdownCtx.Done()

	// Shut down all tails
	s.tailsMtx.RLock()
	defer s.tailsMtx.RUnlock()
	for _, tails := range s.tails {
		for reqID, tail := range tails {
			aud := audToStr(tail.Request.GetTail().Request.Audience)
			s.config.Logger.Debugf("Shutting down tail '%s' for audience '%s'", reqID, aud)
			tail.CancelFunc()
		}
	}

	// Stop this instance from being used again
	s.closed = true

	s.config.Logger.Debug("watchForShutdown() exit")
}

func (s *Streamdal) pullInitialPipelines(ctx context.Context) error {
	cmds, err := s.serverClient.GetSetPipelinesCommandByService(ctx, s.config.ServiceName)
	if err != nil {
		return errors.Wrap(err, "unable to pull initial pipelines")
	}

	for _, cmd := range cmds.SetPipelineCommands {
		// Both GetSetPipelinesCommandsByServiceResponse and GetSetPipelinesCommand have the field WasmModules
		// But in this case, the individual GetSetPipelinesCommand will not have WasmModules populated because
		// this response will have multiple GetSetPipelinesCommand messages inside of it, and we don't want
		// to duplicate the data. When Register() receives GetSetPipelinesCommand, it will be streaming, so
		// each GetSetPipelinesCommand will have its own WasmModules field populated
		cmd.GetSetPipelines().WasmModules = cmds.WasmModules

		if err := s.setPipelines(ctx, cmd); err != nil {
			s.config.Logger.Errorf("failed to attach pipeline: %s", err)
		}
	}

	return nil
}

func (s *Streamdal) heartbeat(loop *director.TimedLooper) {
	var quit bool
	loop.Loop(func() error {
		if quit {
			time.Sleep(time.Millisecond * 50)
			return nil
		}

		select {
		case <-s.config.ShutdownCtx.Done():
			quit = true
			loop.Quit()
			return nil
		default:
			// NOOP
		}

		hb := &protos.HeartbeatRequest{
			SessionId:   s.sessionID,
			Audiences:   s.getCurrentAudiences(),
			ClientInfo:  s.genClientInfo(),
			ServiceName: s.config.ServiceName,
		}

		if err := s.serverClient.HeartBeat(s.config.ShutdownCtx, hb); err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				// Streamdal server went away, log, sleep, and wait for reconnect
				s.config.Logger.Warn("failed to send heartbeat, streamdal server© went away, waiting for reconnect")
				time.Sleep(ReconnectSleep)
				return nil
			}
			s.config.Logger.Errorf("failed to send heartbeat: %s", err)
		}

		return nil
	})

	s.config.Logger.Debug("heartbeat() exit")
}

// TODO: refactor this signature
func (s *Streamdal) runStep(ctx context.Context, df protos.PipelineDataFormat, aud *protos.Audience, step *protos.PipelineStep, data []byte, isr *protos.InterStepResult, workerID int) (*protos.WASMResponse, error) {
	s.config.Logger.Debugf("Running step '%s'", step.Name)

	// Get WASM module
	f, err := s.getFunction(ctx, step, workerID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get wasm data")
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	// Don't need this anymore, and don't want to send it to the wasm function
	step.XWasmBytes = nil

	req := &protos.WASMRequest{
		InputPayload:    data,
		Step:            step,
		InterStepResult: isr,
		DataFormat:      df,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal WASM request")
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, s.config.StepTimeout)
	defer cancel()

	// Run WASM module
	respBytes, err := f.Exec(timeoutCtx, reqBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute wasm module")
	}

	resp := &protos.WASMResponse{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal WASM response")
	}

	// Don't use parent context here since it will be cancelled by the time
	// the goroutine in handleSchema runs
	s.handleSchema(context.Background(), aud, step, resp)

	return resp, nil
}

func (s *Streamdal) getPipelines(ctx context.Context, aud *protos.Audience) []*protos.Pipeline {
	s.pipelinesMtx.RLock()
	defer s.pipelinesMtx.RUnlock()

	s.addAudience(ctx, aud)

	pipelines, ok := s.pipelines[audToStr(aud)]
	if !ok {
		return make([]*protos.Pipeline, 0)
	}

	return pipelines
}

func (s *Streamdal) getCounterLabels(req *ProcessRequest, pipeline *protos.Pipeline) map[string]string {
	l := map[string]string{
		"service":       s.config.ServiceName,
		"component":     req.ComponentName,
		"operation":     req.OperationName,
		"pipeline_name": "",
		"pipeline_id":   "",
	}

	if pipeline != nil {
		l["pipeline_name"] = pipeline.Name
		l["pipeline_id"] = pipeline.Id
	}

	return l
}

func newAudience(req *ProcessRequest, cfg *Config) *protos.Audience {
	if req == nil || cfg == nil {
		panic("BUG: newAudience() called with nil arguments")
	}

	return &protos.Audience{
		ServiceName:   cfg.ServiceName,
		ComponentName: req.ComponentName,
		OperationType: protos.OperationType(req.OperationType),
		OperationName: req.OperationName,
	}
}
func (s *Streamdal) Process(ctx context.Context, req *ProcessRequest) *ProcessResponse {
	switch s.config.Mode {
	case ModeSync:
		// workerID is "1" here since we only have one worker in sync mode
		return s.processSync(ctx, req, 1)
	case ModeAsync:
		s.asyncCh <- req
		return &ProcessResponse{
			Status:         ExecStatusAsync,
			StatusMessage:  proto.String("queued message for async processing"),
			Data:           req.Data,
			PipelineStatus: make([]*protos.PipelineStatus, 0),
			Metadata:       make(map[string]string),
			SdkMode:        protos.SDKMode(s.config.Mode),
		}
	default:
		return &ProcessResponse{
			Status:         ExecStatusError,
			StatusMessage:  proto.String(fmt.Sprintf("unknown SDK mode '%d'", s.config.Mode)),
			Data:           req.Data,
			PipelineStatus: make([]*protos.PipelineStatus, 0),
			Metadata:       make(map[string]string),
			SdkMode:        protos.SDKMode(s.config.Mode),
		}
	}
}

func (s *Streamdal) startAsyncWorkers() {
	for i := 1; i <= s.config.ModeAsyncNumWorkers; i++ {
		go s.runAsyncWorker(i)
	}
}

func (s *Streamdal) runAsyncWorker(workerID int) {
	s.config.Logger.Debugf("starting async worker '%d'", workerID)

	for {
		select {
		case <-s.config.ShutdownCtx.Done():
			s.config.Logger.Debugf("async worker '%d' shutting down", workerID)
			return
		case req := <-s.asyncCh:
			s.config.Logger.Debugf("async worker '%d' processing request", workerID)
			s.processSync(s.config.ShutdownCtx, req, workerID)
		}
	}
}

func (s *Streamdal) processSync(ctx context.Context, req *ProcessRequest, workerID int) *ProcessResponse {
	resp := &ProcessResponse{
		PipelineStatus: make([]*protos.PipelineStatus, 0),
		Metadata:       make(map[string]string),
		SdkMode:        protos.SDKMode(s.config.Mode),
	}

	if s.closed {
		resp.Status = ExecStatusError
		resp.StatusMessage = proto.String(ErrClosedClient.Error())
		return resp
	}

	if !s.shouldProcess() {
		resp.Data = req.Data
		resp.Status = ExecStatusSampling
		resp.StatusMessage = proto.String("skipped processing due to sampling rate")

		return resp
	}

	if err := validateProcessRequest(req); err != nil {
		resp.Status = protos.ExecStatus_EXEC_STATUS_ERROR
		resp.StatusMessage = proto.String(err.Error())

		return resp
	}

	resp.Data = req.Data

	payloadSize := int64(len(resp.Data))
	aud := newAudience(req, s.config)

	// Always send tail output
	defer func() {
		s.sendTail(aud, "", req.Data, resp.Data)
	}()

	// TODO: DRY this up
	counterError := types.ConsumeErrorCount
	counterProcessed := types.ConsumeProcessedCount
	counterBytes := types.ConsumeBytes
	rateBytes := types.ConsumeBytesRate
	rateProcessed := types.ConsumeProcessedRate

	if req.OperationType == OperationTypeProducer {
		counterError = types.ProduceErrorCount
		counterProcessed = types.ProduceProcessedCount
		counterBytes = types.ProduceBytes
		rateBytes = types.ProduceBytesRate
		rateProcessed = types.ProduceProcessedRate
	}

	// Rate counters
	_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: rateBytes, Labels: map[string]string{}, Value: payloadSize, Audience: aud})
	_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: rateProcessed, Labels: map[string]string{}, Value: 1, Audience: aud})

	pipelines := s.getPipelines(ctx, aud)

	// WARNING: This case will (usually) only "hit" for the first <100ms of
	// running the SDK - after that, the server will have sent us at least one,
	// "hidden"  pipeline - "infer schema". All of this happens asynchronously
	// (to prevent Register() from blocking).
	//
	// This means that setting resp.StatusMessage here means that it will only
	// survive for the first few messages - after that, StatusMessage might get
	// updated by the infer schema pipeline step.
	if len(pipelines) == 0 {
		// No pipelines for this mode, nothing to do
		resp.Status = protos.ExecStatus_EXEC_STATUS_TRUE

		return resp
	}

	if payloadSize > MaxWASMPayloadSize {
		_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: counterError, Labels: s.getCounterLabels(req, nil), Value: 1, Audience: aud})
		s.config.Logger.Warn(ErrMaxPayloadSizeExceeded)

		resp.Status = protos.ExecStatus_EXEC_STATUS_ERROR
		resp.StatusMessage = proto.String(ErrMaxPayloadSizeExceeded.Error())

		return resp
	}

	totalPipelines := len(pipelines)
	var (
		pIndex int
		sIndex int
	)

PIPELINE:
	for _, pipeline := range pipelines {
		var isr *protos.InterStepResult
		pIndex += 1

		pipelineTimeoutCtx, pipelineTimeoutCxl := context.WithTimeout(ctx, s.config.PipelineTimeout)

		pipelineStatus := &protos.PipelineStatus{
			Id:         pipeline.Id,
			Name:       pipeline.Name,
			StepStatus: make([]*protos.StepStatus, 0),
		}

		_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: counterProcessed, Labels: s.getCounterLabels(req, pipeline), Value: 1, Audience: aud})
		_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: counterBytes, Labels: s.getCounterLabels(req, pipeline), Value: payloadSize, Audience: aud})

		totalSteps := len(pipeline.Steps)

		for _, step := range pipeline.Steps {
			sIndex += 1

			stepTimeoutCtx, stepTimeoutCxl := context.WithTimeout(ctx, s.config.StepTimeout)

			stepStatus := &protos.StepStatus{
				Name: step.Name,
			}

			select {
			case <-pipelineTimeoutCtx.Done():
				pipelineTimeoutCxl()
				stepTimeoutCxl()

				stepStatus.Status = protos.ExecStatus_EXEC_STATUS_ERROR
				stepStatus.StatusMessage = proto.String("Pipeline error: " + ErrPipelineTimeout.Error())

				// Maybe notify, maybe include metadata
				cond := s.handleCondition(ctx, req, resp, step.OnError, step, pipeline, aud, protos.NotifyRequest_CONDITION_TYPE_ON_ERROR)

				// Update the abort condition before we populate statuses in resp
				stepStatus.AbortCondition = cond.abortCondition

				if cond.abortCurrent {
					// Aborting CURRENT, LOCAL step & pipeline status needs to be updated
					if pIndex == totalPipelines {
						s.updateStatus(resp, pipelineStatus, stepStatus)
					} else {
						s.updateStatus(nil, pipelineStatus, stepStatus)
					}

					s.config.Logger.Warnf("exceeded timeout for pipeline '%s' - aborting CURRENT pipeline", pipeline.Name)

					continue PIPELINE
				} else if cond.abortAll {
					s.config.Logger.Warnf("exceeded timeout for pipeline '%s' - aborting ALL pipelines", pipeline.Name)

					// Aborting ALL, RESP should have step & pipeline status updated
					s.updateStatus(resp, pipelineStatus, stepStatus)

					return resp
				}

				// NOT aborting, don't need to update step or pipeline status
				s.config.Logger.Warnf("exceeded timeout for pipeline '%s' but no abort condition defined - continuing execution", step.Name)
			default:
				// NOOP
			}

			// Pipeline timeout either has not occurred OR it occurred and execution was not aborted

			wasmResp, err := s.runStep(stepTimeoutCtx, pipeline.DataFormat, aud, step, resp.Data, isr, workerID)
			if err != nil {
				stepTimeoutCxl()

				err = fmt.Errorf("wasm error during step '%s:%s': %s", pipeline.Name, step.Name, err)

				stepStatus.Status = protos.ExecStatus_EXEC_STATUS_ERROR
				stepStatus.StatusMessage = proto.String("Wasm Error: " + err.Error())

				// Maybe notify, maybe include metadata
				cond := s.handleCondition(ctx, req, resp, step.OnError, step, pipeline, aud, protos.NotifyRequest_CONDITION_TYPE_ON_ERROR)

				// Update the abort condition before we populate statuses in resp
				stepStatus.AbortCondition = cond.abortCondition

				if cond.abortCurrent {
					pipelineTimeoutCxl()

					// Aborting CURRENT, update LOCAL step & pipeline status
					//
					// It is possible that resp won't have its status filled out IF the
					// last pipeline AND last step has an abort condition. To get around
					// that, all steps check to see if they are the last in line to exec
					// and if they are, they will fill the response status.
					if pIndex == totalPipelines && sIndex == totalSteps {
						s.updateStatus(resp, pipelineStatus, stepStatus)
					} else {
						s.updateStatus(nil, pipelineStatus, stepStatus)
					}

					s.config.Logger.Errorf(err.Error() + " (aborting CURRENT pipeline)")

					continue PIPELINE
				} else if cond.abortAll {
					pipelineTimeoutCxl()

					// Aborting ALL, update RESP step & pipeline status
					s.updateStatus(resp, pipelineStatus, stepStatus)

					s.config.Logger.Errorf(err.Error() + " (aborting ALL pipelines)")

					return resp
				}

				// NOT aborting, update LOCAL step & pipeline status
				s.updateStatus(nil, pipelineStatus, stepStatus)

				s.config.Logger.Warnf("Step '%s:%s' failed (no abort condition defined - continuing step execution): %s", pipeline.Name, step.Name, err.Error())

				continue // Move on to the next step in the pipeline
			}

			// Only update working payload if one is returned
			if len(wasmResp.OutputPayload) > 0 {
				resp.Data = wasmResp.OutputPayload
			}

			isr = wasmResp.InterStepResult // Pass inter-step result to next step
			if isr != nil {
				// Ensure audience is set on ISR for HttpRequests
				// ISR might be nil since not all WASM modules return it, so only set if ISR is not nil
				isr.Audience = aud
			}

			var (
				stepCondStr    string
				stepConds      *protos.PipelineStepConditions
				stepExecStatus protos.ExecStatus
				condType       protos.NotifyRequest_ConditionType
			)

			// Execution worked - check wasm exit code
			switch wasmResp.ExitCode {
			case protos.WASMExitCode_WASM_EXIT_CODE_TRUE:
				// Data was potentially modified
				resp.Data = wasmResp.OutputPayload

				stepCondStr = "true"
				stepConds = step.OnTrue
				stepExecStatus = protos.ExecStatus_EXEC_STATUS_TRUE
				condType = protos.NotifyRequest_CONDITION_TYPE_ON_TRUE
			case protos.WASMExitCode_WASM_EXIT_CODE_FALSE:
				// Data was potentially modified
				resp.Data = wasmResp.OutputPayload

				stepCondStr = "false"
				stepConds = step.OnFalse
				stepExecStatus = protos.ExecStatus_EXEC_STATUS_FALSE
				condType = protos.NotifyRequest_CONDITION_TYPE_ON_FALSE
			case protos.WASMExitCode_WASM_EXIT_CODE_ERROR:
				// Ran into an error - return original data
				resp.Data = wasmResp.OutputPayload

				stepCondStr = "error"
				stepConds = step.OnError
				stepExecStatus = protos.ExecStatus_EXEC_STATUS_ERROR
				condType = protos.NotifyRequest_CONDITION_TYPE_ON_ERROR
			default:
				_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: counterError, Labels: s.getCounterLabels(req, pipeline), Value: 1, Audience: aud})
				s.config.Logger.Debugf("Step '%s:%s' returned unknown exit code %d", pipeline.Name, step.Name, wasmResp.ExitCode)

				// TODO: Is an unknown exit code considered an error?
			}

			stepTimeoutCxl()

			statusMsg := fmt.Sprintf("step '%s:%s' returned %s: %s", pipeline.Name, step.Name, stepCondStr, wasmResp.ExitMsg)

			// Maybe notify, maybe include metadata
			cond := s.handleCondition(ctx, req, resp, stepConds, step, pipeline, aud, condType)

			// Update step status bits
			stepStatus.Status = stepExecStatus
			stepStatus.StatusMessage = proto.String(statusMsg)

			stepStatus.AbortCondition = cond.abortCondition

			// Increase error metrics (if wasm returned err)
			if stepExecStatus == protos.ExecStatus_EXEC_STATUS_ERROR {
				_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: counterError, Labels: s.getCounterLabels(req, pipeline), Value: 1, Audience: aud})
			}

			if cond.abortCurrent {
				pipelineTimeoutCxl()

				// Aborting CURRENT, update LOCAL step & pipeline status
				//
				// It is possible that resp won't have its status filled out IF the
				// last pipeline AND last step has an abort condition. To get around
				// that, all steps check to see if they are the last in line to exec
				// and if they are, they will fill the response status.
				if pIndex == totalPipelines && sIndex == totalSteps {
					s.updateStatus(resp, pipelineStatus, stepStatus)
				} else {
					s.updateStatus(nil, pipelineStatus, stepStatus)
				}

				s.config.Logger.Debug(statusMsg + " (aborting CURRENT pipeline)")

				continue PIPELINE
			} else if cond.abortAll {
				pipelineTimeoutCxl()

				// Aborting ALL, update RESP step & pipeline status
				s.updateStatus(resp, pipelineStatus, stepStatus)

				s.config.Logger.Debug(statusMsg + " (aborting ALL pipelines)")

				return resp
			}

			// NO abort condition, update LOCAL step & pipeline status
			s.updateStatus(nil, pipelineStatus, stepStatus)

			s.config.Logger.Debug(statusMsg + " (no abort condition defined - continuing execution)")

			// END step loop
		}

		pipelineTimeoutCxl()

		// Pipeline completed, update RESP pipeline status (step status already updated)
		s.updateStatus(resp, pipelineStatus, nil)

		// END pipeline loop
	}

	// Dry run should not modify anything, but we must allow pipeline to
	// mutate internal state in order to function properly
	if s.config.DryRun {
		resp.Data = req.Data
	}

	return resp
}

type condition struct {
	abortCurrent bool
	abortAll     bool

	// This is here to make it easier to perform assignment in stepStatus
	abortCondition protos.AbortCondition
}

// handleCondition is a wrapper for inspecting the step condition and potentially
// performing a notification and injecting metadata back into the response.
// TODO: simplify these params
func (s *Streamdal) handleCondition(
	ctx context.Context,
	req *ProcessRequest,
	resp *ProcessResponse,
	stepCond *protos.PipelineStepConditions,
	step *protos.PipelineStep,
	pipeline *protos.Pipeline,
	aud *protos.Audience,
	condType protos.NotifyRequest_ConditionType,
) condition {
	// If no condition is set, we don't need to do anything
	if stepCond == nil {
		return condition{}
	}

	// Should we notify?
	if stepCond.Notification != nil && !s.config.DryRun {
		s.config.Logger.Debugf("Performing 'notify' condition for step '%s'", step.Name)

		if err := s.serverClient.Notify(ctx, pipeline, step, aud, resp.Data, condType); err != nil {
			s.config.Logger.Errorf("failed to notify condition: %s", err)
		}

		labels := map[string]string{
			"service":       s.config.ServiceName,
			"component":     req.ComponentName,
			"operation":     req.OperationName,
			"pipeline_name": pipeline.Name,
			"pipeline_id":   pipeline.Id,
		}
		_ = s.metrics.Incr(ctx, &types.CounterEntry{Name: types.NotifyCount, Labels: labels, Value: 1, Audience: aud})
	}

	// Should we pass back metadata?
	if len(stepCond.Metadata) > 0 {
		s.config.Logger.Debugf("Performing 'metadata' condition for step '%s'", step.Name)
		s.populateMetadata(resp, stepCond.Metadata)
	}

	// Should we abort current or ALL pipelines?
	if stepCond.Abort == protos.AbortCondition_ABORT_CONDITION_ABORT_CURRENT {
		s.config.Logger.Debugf("Abort condition set to 'current' for step '%s'", step.Name)
		return condition{
			abortCurrent:   true,
			abortCondition: protos.AbortCondition_ABORT_CONDITION_ABORT_CURRENT,
		}
	} else if stepCond.Abort == protos.AbortCondition_ABORT_CONDITION_ABORT_ALL {
		s.config.Logger.Debugf("Abort condition set to 'all' for step '%s'", step.Name)
		return condition{
			abortAll:       true,
			abortCondition: protos.AbortCondition_ABORT_CONDITION_ABORT_ALL,
		}
	}

	s.config.Logger.Debugf("No abort conditions set for step '%s'", step.Name)

	// Don't abort anything - continue as-is
	return condition{}
}

func (s *Streamdal) populateMetadata(resp *ProcessResponse, metadata map[string]string) {
	if resp == nil || metadata == nil {
		return
	}

	for k, v := range metadata {
		if resp.Metadata == nil {
			resp.Metadata = make(map[string]string)
		}

		resp.Metadata[k] = v
	}
}

// updateStatus is a wrapper for updating step, pipeline and resp statuses.
//
// This method allows resp OR step status to be nil. This is because we are not
// always returning a final response - we are still going through more pipelines
// and steps.
//
// Similarly, step status can be nil because the FINAL response will only have
// a pipeline status and no step status.
func (s *Streamdal) updateStatus(resp *ProcessResponse, pipelineStatus *protos.PipelineStatus, stepStatus *protos.StepStatus) {
	// Pipeline status is ALWAYS required
	if pipelineStatus == nil {
		s.config.Logger.Warn("BUG: pipelineStatus cannot be nil in updateStatus()")
	}

	// If resp is nil, there should ALWAYS be a step and pipeline status
	if resp == nil && (stepStatus == nil || pipelineStatus == nil) {
		s.config.Logger.Warn("BUG: stepStatus and pipelineStatus cannot be nil when resp is nil in updateStatus()")
		return
	}

	if stepStatus != nil {
		// When returning final response, we won't have a step status
		pipelineStatus.StepStatus = append(pipelineStatus.StepStatus, stepStatus)

		// Prettify status message with abort info
		var abortStatusStr string

		switch stepStatus.AbortCondition {
		case protos.AbortCondition_ABORT_CONDITION_ABORT_CURRENT:
			abortStatusStr = AbortCurrentStr
		case protos.AbortCondition_ABORT_CONDITION_ABORT_ALL:
			abortStatusStr = AbortAllStr
		default:
			abortStatusStr = AbortNoneStr
		}

		// StepStatusMessage can be nil because it is optional
		if stepStatus.StatusMessage != nil {
			// If the message is NOT empty, we want to append the append abort status
			if *stepStatus.StatusMessage != "" {
				stepStatus.StatusMessage = proto.String(fmt.Sprintf("%s (%s)", *stepStatus.StatusMessage, abortStatusStr))
			} else {
				// Otherwise, just display the abort status
				stepStatus.StatusMessage = proto.String(abortStatusStr)
			}
		}
	}

	// Response could be nil if we are NOT aborting all pipelines; resp will NOT
	// be nil if this is the LAST pipeline and LAST step.
	if resp != nil {
		resp.PipelineStatus = append(resp.PipelineStatus, pipelineStatus)

		// Resp status should be the status of the LAST step
		resp.Status = pipelineStatus.StepStatus[len(pipelineStatus.StepStatus)-1].Status
		resp.StatusMessage = pipelineStatus.StepStatus[len(pipelineStatus.StepStatus)-1].StatusMessage
	}
}

// shouldProcess determines if a message should be processed based on
// whether sampling is enabled and the rate limiter allows the message
func (s *Streamdal) shouldProcess() bool {
	if s.limiter == nil {
		return true
	}

	return s.limiter.Allow()
}
