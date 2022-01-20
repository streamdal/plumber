package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/plumber"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
)

func main() {
	kongCtx, cliOpts, err := options.New(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	// TODO: STDIN write should be continuous; punting for now.
	readFromStdin(cliOpts)

	switch {
	case cliOpts.Global.Debug:
		logrus.SetLevel(logrus.DebugLevel)
	case cliOpts.Global.Quiet:
		logrus.SetLevel(logrus.ErrorLevel)
	}

	serviceCtx, serviceShutdownFunc := context.WithCancel(context.Background())
	mainCtx, mainShutdownFunc := context.WithCancel(context.Background())

	// We only want to intercept interrupt signals in relay or server mode
	if cliOpts.Global.XAction == "relay" || cliOpts.Global.XAction == "server" {
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
		prometheus.InitPrometheusMetrics()
	}

	// Launch a dedicated goroutine if stats display is enabled
	if cliOpts.Relay != nil && cliOpts.Relay.StatsEnable {
		prometheus.Start(cliOpts.Relay.StatsReportIntervalSec)
	}

	cfg := getConfig()

	// Force JSON and don't print logo when NOT in terminal (ie. in container, detached)
	if !terminal.IsTerminal(int(os.Stderr.Fd())) {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		printer.PrintLogo()

		// Prompt analytics only in CLI mode
		// It is disabled otherwise
		setupAnalytics(cfg)
	}

	p, err := plumber.New(&plumber.Config{
		PersistentConfig:   cfg,
		ServiceShutdownCtx: serviceCtx,
		MainShutdownFunc:   mainShutdownFunc,
		MainShutdownCtx:    mainCtx,
		KongCtx:            kongCtx,
		CLIOptions:         cliOpts,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	p.Run()
}

func setupAnalytics(cfg *config.Config) {
	// Value is already saved to config.json, nothing to do here
	if cfg.EnableAnalytics != "" {
		return
	}

	// Ask user if they want to enable
	answer := promptForAnalytics(os.Stdin)

	if answer == "true" {
		logrus.Info("Analytics enabled")
	} else {
		logrus.Info("Analytics disabled")
	}

	cfg.EnableAnalytics = answer
	cfg.Save()
}

// readUsername reads a password from stdin
func promptForAnalytics(stdin io.Reader) string {
	fmt.Println("\n\nEnable analytics?")
	fmt.Println("------------------------------------------------------------------------------------")
	fmt.Println("Plumber can send anonymous usage analytics to Batch.sh to help us better understand")
	fmt.Println("how our users are using our software. This information is anonymous, and only")
	fmt.Println("contains the following information:")
	fmt.Println("")
	fmt.Println(" * Plumber version")
	fmt.Println(" * Operating system")
	fmt.Println(" * Operation performed: read,write,relay,batch")
	fmt.Println(" * Backend used: kafka, rabbitmq, etc")
	fmt.Println("")
	fmt.Println("------------------------------------------------------------------------------------")
	fmt.Print("\n\nDo you wish to send usage analytics to Batch.sh? (y/N)")

	// int typecast is needed for windows
	reader := bufio.NewReader(stdin)
	answer, err := reader.ReadString('\n')
	if err != nil {
		return "false"
	}

	s := strings.TrimSpace(answer)
	switch s {
	case "y", "Y":
		return "true"
	case "n", "N":
		fallthrough
	default:
		return "false"
	}
}

// readFromStdin reads data piped into stdin
func readFromStdin(opts *opts.CLIOptions) {
	info, err := os.Stdin.Stat()
	if err != nil {
		logrus.Fatal(err)
	}

	if info.Mode()&os.ModeCharDevice != 0 || info.Size() <= 0 {
		return
	}

	inputData := make([]string, 0)

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err == io.EOF {
			break
		}
		inputData = append(inputData, strings.Trim(string(line), "\n"))
	}

	// Treat input as a JSON array
	if opts.Write.XCliOptions.InputAsJsonArray {
		opts.Write.XCliOptions.InputStdin = convertJSONInput(inputData[0])
		return
	}

	// Treat input as new object on each line
	opts.Write.XCliOptions.InputStdin = inputData
}

// convertJSONInput converts a JSON array to a slice of strings for the writer to consume
func convertJSONInput(value string) []string {
	inputData := make([]string, 0)

	jsonArray := gjson.Parse(value)
	if !jsonArray.IsArray() {
		logrus.Fatal("--json-array option was passed, but input data is not a valid JSON array")
	}

	jsonArray.ForEach(func(key, value gjson.Result) bool {
		inputData = append(inputData, value.Raw)
		return true
	})

	return inputData
}

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
			Connections:         make(map[string]*opts.ConnectionOptions),
			Relays:              make(map[string]*types.Relay),
			Schemas:             make(map[string]*protos.Schema),
			Services:            make(map[string]*protos.Service),
			Reads:               make(map[string]*types.Read),
			ImportRequests:      make(map[string]*protos.ImportGithubRequest),
			Validations:         make(map[string]*common.Validation),
			Composites:          make(map[string]*opts.Composite),
			ConnectionsMutex:    &sync.RWMutex{},
			ServicesMutex:       &sync.RWMutex{},
			ReadsMutex:          &sync.RWMutex{},
			RelaysMutex:         &sync.RWMutex{},
			SchemasMutex:        &sync.RWMutex{},
			ImportRequestsMutex: &sync.RWMutex{},
			ValidationsMutex:    &sync.RWMutex{},
			CompositesMutex:     &sync.RWMutex{},
		}
	}

	return cfg
}
