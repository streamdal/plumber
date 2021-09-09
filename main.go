package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/plumber"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/stats"
)

func main() {
	kongCtx, opts, err := options.New(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	// TODO: STDIN write should be continuous; punting for now.
	// readFromStdin(opts)

	switch {
	case opts.Global.Debug:
		logrus.SetLevel(logrus.DebugLevel)
	case opts.Global.Quiet:
		logrus.SetLevel(logrus.ErrorLevel)
	}

	serviceCtx, serviceShutdownFunc := context.WithCancel(context.Background())
	mainCtx, mainShutdownFunc := context.WithCancel(context.Background())

	// We only want to intercept interrupt signals in relay or server mode
	if opts.Global.XAction == "relay" || opts.Global.XAction == "server" {
		logrus.Debug("Intercepting signals")

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, syscall.SIGTERM)

		go func() {
			sig := <-c
			logrus.Debugf("Received system call: %+v", sig)

			serviceShutdownFunc()
		}()

		// Create prometheus counters/gauges
		stats.InitPrometheusMetrics()
	}

	// Launch a dedicated goroutine if stats display is enabled
	if opts.Read != nil && opts.Read.XCliConfig != nil {
		if opts.Read.XCliConfig.StatsEnable {
			stats.Start(opts.Read.XCliConfig.StatsReportIntervalSec)
		}
	}

	// Force JSON and don't print logo when NOT in terminal (ie. in container, detached)
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		printer.PrintLogo()
	}

	p, err := plumber.New(&plumber.Config{
		PersistentConfig:   getConfig(),
		ServiceShutdownCtx: serviceCtx,
		MainShutdownFunc:   mainShutdownFunc,
		MainShutdownCtx:    mainCtx,
		KongCtx:            kongCtx,
		CLIOptions:         opts,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	p.Run()
}

//// readFromStdin reads data piped into stdin
//func readFromStdin(opts *protos.CLIOptions) {
//	info, err := os.Stdin.Stat()
//	if err != nil {
//		logrus.Fatal(err)
//	}
//
//	if info.Mode()&os.ModeCharDevice != 0 || info.Size() <= 0 {
//		return
//	}
//
//	inputData := make([]string, 0)
//
//	reader := bufio.NewReader(os.Stdin)
//	for {
//		line, err := reader.ReadBytes('\n')
//		if err != nil && err == io.EOF {
//			break
//		}
//		inputData = append(inputData, strings.Trim(string(line), "\n"))
//	}
//
//	// Treat input as a JSON array
//	if opts.Write.XCliConfig.InputAsJsonArray {
//		opts.Write.InputData = convertJSONInput(inputData[0])
//		return
//	}
//
//	// Treat input as new object on each line
//	opts.Write.InputData = inputData
//}

//// convertJSONInput converts a JSON array to a slice of strings for the writer to consume
//func convertJSONInput(value string) []string {
//	inputData := make([]string, 0)
//	jsonArray := gjson.Parse(value)
//	if !jsonArray.IsArray() {
//		logrus.Fatal("--json-array option was passed, but input data is not a valid JSON array")
//	}
//
//	jsonArray.ForEach(func(key, value gjson.Result) bool {
//		inputData = append(inputData, value.Raw)
//		return true
//	})
//
//	return inputData
//}

// getConfig returns either a stored config if there is one, or a fresh config
func getConfig() *config.Config {
	var cfg *config.Config
	var err error

	// No need to pollute user's FS if they aren't running in server mode
	if config.Exists("config.json") {
		cfg, err = config.ReadConfig("config.json")
		if err != nil {
			logrus.Errorf("unable to load config: %s", err)
		}
	}

	if cfg == nil {
		cfg = &config.Config{
			Connections:      make(map[string]*protos.Connection),
			Relays:           make(map[string]*types.Relay),
			Schemas:          make(map[string]*protos.Schema),
			Services:         make(map[string]*protos.Service),
			Reads:            make(map[string]*types.Read),
			ConnectionsMutex: &sync.RWMutex{},
			ServicesMutex:    &sync.RWMutex{},
			ReadsMutex:       &sync.RWMutex{},
			RelaysMutex:      &sync.RWMutex{},
			SchemasMutex:     &sync.RWMutex{},
		}
	}

	return cfg
}
