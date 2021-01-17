package servicebus

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/rpc"
	"github.com/Azure/go-amqp"
)

// Error Conditions
const (
	// Service Bus Errors
	errorServerBusy         amqp.ErrorCondition = "com.microsoft:server-busy"
	errorTimeout            amqp.ErrorCondition = "com.microsoft:timeout"
	errorOperationCancelled amqp.ErrorCondition = "com.microsoft:operation-cancelled"
	errorContainerClose     amqp.ErrorCondition = "com.microsoft:container-close"
)

const (
	amqpRetryDefaultTimes    int           = 3
	amqpRetryDefaultDelay    time.Duration = time.Second
	amqpRetryBusyServerDelay time.Duration = 10 * time.Second
)

type (
	// ErrMissingField indicates that an expected property was missing from an AMQP message. This should only be
	// encountered when there is an error with this library, or the server has altered its behavior unexpectedly.
	ErrMissingField string

	// ErrMalformedMessage indicates that a message was expected in the form of []byte was not a []byte. This is likely
	// a bug and should be reported.
	ErrMalformedMessage string

	// ErrIncorrectType indicates that type assertion failed. This should only be encountered when there is an error
	// with this library, or the server has altered its behavior unexpectedly.
	ErrIncorrectType struct {
		Key          string
		ExpectedType reflect.Type
		ActualValue  interface{}
	}

	// ErrAMQP indicates that the server communicated an AMQP error with a particular
	ErrAMQP rpc.Response

	// ErrNoMessages is returned when an operation returned no messages. It is not indicative that there will not be
	// more messages in the future.
	ErrNoMessages struct{}

	// ErrNotFound is returned when an entity is not found (404)
	ErrNotFound struct {
		EntityPath string
	}

	// ErrConnectionClosed indicates that the connection has been closed.
	ErrConnectionClosed string
)

func (e ErrMissingField) Error() string {
	return fmt.Sprintf("missing value %q", string(e))
}

func (e ErrMalformedMessage) Error() string {
	return fmt.Sprintf("message was expected in the form of []byte was not a []byte")
}

// NewErrIncorrectType lets you skip using the `reflect` package. Just provide a variable of the desired type as
// 'expected'.
func newErrIncorrectType(key string, expected, actual interface{}) ErrIncorrectType {
	return ErrIncorrectType{
		Key:          key,
		ExpectedType: reflect.TypeOf(expected),
		ActualValue:  actual,
	}
}

func (e ErrIncorrectType) Error() string {
	return fmt.Sprintf(
		"value at %q was expected to be of type %q but was actually of type %q",
		e.Key,
		e.ExpectedType,
		reflect.TypeOf(e.ActualValue))
}

func (e ErrAMQP) Error() string {
	return fmt.Sprintf("server says (%d) %s", e.Code, e.Description)
}

func (e ErrNoMessages) Error() string {
	return "no messages available"
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("entity at %s not found", e.EntityPath)
}

// IsErrNotFound returns true if the error argument is an ErrNotFound type
func IsErrNotFound(err error) bool {
	_, ok := err.(ErrNotFound)
	return ok
}

func (e ErrConnectionClosed) Error() string {
	return fmt.Sprintf("the connection has been closed: %s", string(e))
}
