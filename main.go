package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/plumber"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/stats"
)

func main() {
	cmd, opts, err := cli.Handle(os.Args[1:])
	if err != nil {
		logrus.Fatalf("Unable to handle CLI input: %s", err)
	}

	if opts.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if opts.Quiet {
		logrus.SetLevel(logrus.ErrorLevel)
	}

	serviceCtx, serviceShutdownFunc := context.WithCancel(context.Background())
	mainCtx, mainShutdownFunc := context.WithCancel(context.Background())

	// We only want to intercept these in relay mode
	if strings.HasPrefix("cmd", "relay") {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, syscall.SIGTERM)

		go func() {
			signal := <-c
			logrus.Infof("received system call: %+v", signal)

			serviceShutdownFunc()
		}()
	}

	if opts.Stats {
		stats.Start(opts.StatsReportInterval)
	}

	// In container mode, force JSON and don't print logo
	if !terminal.IsTerminal(int(os.Stderr.Fd())) || opts.Batch.OutputType == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		printer.PrintLogo()
	}

	p, err := plumber.New(&plumber.Config{
		ServiceShutdownContext: serviceCtx,
		MainShutdownFunc:       mainShutdownFunc,
		MainShutdownContext:    mainCtx,
		Cmd:                    cmd,
		Options:                opts,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	p.Run()
}
