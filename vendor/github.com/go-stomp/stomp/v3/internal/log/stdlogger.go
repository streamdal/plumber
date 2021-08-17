package log

import (
	"fmt"
	stdlog "log"
)

var (
	debugPrefix = "DEBUG: "
	infoPrefix  = "INFO: "
	warnPrefix  = "WARN: "
	errorPrefix = "ERROR: "
)

func logf(prefix string, format string, value ...interface{}) {
	_ = stdlog.Output(3, fmt.Sprintf(prefix+format+"\n", value...))
}

type StdLogger struct{}

func (s StdLogger) Debugf(format string, value ...interface{}) {
	logf(debugPrefix, format, value...)
}

func (s StdLogger) Debug(message string) {
	logf(debugPrefix, "%s", message)
}

func (s StdLogger) Infof(format string, value ...interface{}) {
	logf(infoPrefix, format, value...)
}

func (s StdLogger) Info(message string) {
	logf(infoPrefix, "%s", message)
}

func (s StdLogger) Warningf(format string, value ...interface{}) {
	logf(warnPrefix, format, value...)
}

func (s StdLogger) Warning(message string) {
	logf(warnPrefix, "%s", message)
}

func (s StdLogger) Errorf(format string, value ...interface{}) {
	logf(errorPrefix, format, value...)
}

func (s StdLogger) Error(message string) {
	logf(errorPrefix, "%s", message)
}
