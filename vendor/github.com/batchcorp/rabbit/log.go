package rabbit

// Logger is the common interface for user-provided loggers.
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

// NoOpLogger is a do-nothing logger; it is used internally
// as the default Logger when none is provided in the Options.
type NoOpLogger struct {
}

// Debug is no-op implementation of Logger's Debug.
func (l *NoOpLogger) Debug(args ...interface{}) {
}

// Debugf is no-op implementation of Logger's Debugf.
func (l *NoOpLogger) Debugf(format string, args ...interface{}) {
}

// Info is no-op implementation of Logger's Info.
func (l *NoOpLogger) Info(args ...interface{}) {
}

// Infof is no-op implementation of Logger's Infof.
func (l *NoOpLogger) Infof(format string, args ...interface{}) {
}

// Warn is no-op implementation of Logger's Warn.
func (l *NoOpLogger) Warn(args ...interface{}) {
}

// Warnf is no-op implementation of Logger's Warnf.
func (l *NoOpLogger) Warnf(format string, args ...interface{}) {
}

// Error is no-op implementation of Logger's Error.
func (l *NoOpLogger) Error(args ...interface{}) {
}

// Errorf is no-op implementation of Logger's Errorf.
func (l *NoOpLogger) Errorf(format string, args ...interface{}) {
}
