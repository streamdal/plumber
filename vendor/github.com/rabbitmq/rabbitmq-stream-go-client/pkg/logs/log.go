package logs

import (
	"fmt"
	"log"
)

const (
	INFO  = 0
	DEBUG = 1
)

var LogLevel int8

func LogInfo(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}

func LogError(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

func LogDebug(message string, v ...interface{}) {
	if LogLevel > INFO {
		log.Printf(fmt.Sprintf("[debug] - %s", message), v...)
	}
}

func LogWarn(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[warn] - %s", message), v...)
}
