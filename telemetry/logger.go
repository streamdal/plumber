package telemetry

type NoopLogger struct{}

func (l *NoopLogger) Logf(format string, args ...interface{}) {
	// NOOP
}

func (l *NoopLogger) Errorf(format string, args ...interface{}) {
	// NOOP
}
