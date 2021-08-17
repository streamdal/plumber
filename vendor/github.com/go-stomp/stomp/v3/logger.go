package stomp

type Logger interface {
	Debugf(format string, value ...interface{})
	Infof(format string, value ...interface{})
	Warningf(format string, value ...interface{})
	Errorf(format string, value ...interface{})

	Debug(message string)
	Info(message string)
	Warning(message string)
	Error(message string)
}
