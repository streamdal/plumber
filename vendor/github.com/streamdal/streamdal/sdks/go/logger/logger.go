package logger

import (
	"log"
)

// Logger is the common interface for user-provided loggers.
//
//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . Logger
type Logger interface {
	// Debug sends out a debug message with the given arguments to the logger.
	Debug(args ...interface{})
	// Debugf formats a debug message using the given arguments and sends it to the logger.
	Debugf(format string, args ...interface{})
	// Info sends out an informational message with the given arguments to the logger.
	Info(args ...interface{})
	// Infof formats an informational message using the given arguments and sends it to the logger.
	Infof(format string, args ...interface{})
	// Warn sends out a warning message with the given arguments to the logger.
	Warn(args ...interface{})
	// Warnf formats a warning message using the given arguments and sends it to the logger.
	Warnf(format string, args ...interface{})
	// Error sends out an error message with the given arguments to the logger.
	Error(args ...interface{})
	// Errorf formats an error message using the given arguments and sends it to the logger.
	Errorf(format string, args ...interface{})
}

// TinyLogger is an _almost_ no-op logger - if the user of the SDK does not
// inject their own logger, this logger will be used instead. This logger will
// print Warning and Error messages to STDOUT and will ignore Debug & Info.
type TinyLogger struct{}

// Debug won't print anything.
func (l *TinyLogger) Debug(args ...interface{}) {}

// Debugf is no-op implementation of Logger's Debugf.
func (l *TinyLogger) Debugf(format string, args ...interface{}) {}

// Info is no-op implementation of Logger's Info.
func (l *TinyLogger) Info(args ...interface{}) {}

// Infof is a no-op implementation of Logger's Infof.
func (l *TinyLogger) Infof(format string, args ...interface{}) {}

// Warn will print the warn message to STDOUT. This will only be used if the
// user has NOT injected their own logger.
func (l *TinyLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

// Warnf will print the warn message to STDOUT. This will only be used if the
// user has NOT injected their own logger.
func (l *TinyLogger) Warnf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

// Error will print the error message to STDOUT. This will only be used if the
// user has NOT injected their own logger.
func (l *TinyLogger) Error(args ...interface{}) {
	log.Println(args...)
}

// Errorf will print the error message to STDOUT. This will only be used if the
// user has NOT injected their own logger.
func (l *TinyLogger) Errorf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
