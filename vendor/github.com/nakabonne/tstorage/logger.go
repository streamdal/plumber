package tstorage

// TODO: Think about another abstraction way

// Logger is a logging interface
type Logger interface {
	Printf(format string, v ...interface{})
}

type nopLogger struct{}

func (l *nopLogger) Printf(_ string, _ ...interface{}) {
	// Do nothing
	return
}
