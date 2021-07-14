package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"time"
)

type responseError struct {
	Err       error
	isTimeout bool
}

func newResponseError(err error, timeout bool) responseError {
	return responseError{
		Err:       err,
		isTimeout: timeout,
	}
}

func uShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

//func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func uShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func waitCodeWithDefaultTimeOut(response *Response) responseError {
	return waitCodeWithTimeOut(response, defaultSocketCallTimeout)
}
func waitCodeWithTimeOut(response *Response, timeout time.Duration) responseError {
	select {
	case code := <-response.code:
		if code.id != responseCodeOk {
			return newResponseError(lookErrorCode(code.id), false)
		}
		return newResponseError(nil, false)
	case <-time.After(timeout):
		logs.LogError("timeout %d ms - waiting Code, operation: %s", defaultSocketCallTimeout, response.commandDescription)

		return newResponseError(
			fmt.Errorf("timeout %d ms - waiting Code, operation: %s ",
				defaultSocketCallTimeout, response.commandDescription), true)
	}
}

func SetLevelInfo(value int8) {
	logs.LogLevel = value
}
