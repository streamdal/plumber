package stomp

import (
	"github.com/go-stomp/stomp/frame"
)

// SubscribeOpt contains options for for the Conn.Subscribe function.
var SubscribeOpt struct {
	// Id provides the opportunity to specify the value of the "id" header
	// entry in the STOMP SUBSCRIBE frame.
	//
	// If the client program does specify the value for "id",
	// it is responsible for choosing a unique value.
	Id func(id string) func(*frame.Frame) error

	// Header provides the opportunity to include custom header entries
	// in the SUBSCRIBE frame that the client sends to the server.
	Header func(key, value string) func(*frame.Frame) error
}

func init() {
	SubscribeOpt.Id = func(id string) func(*frame.Frame) error {
		return func(f *frame.Frame) error {
			if f.Command != frame.SUBSCRIBE {
				return ErrInvalidCommand
			}
			f.Header.Set(frame.Id, id)
			return nil
		}
	}

	SubscribeOpt.Header = func(key, value string) func(*frame.Frame) error {
		return func(f *frame.Frame) error {
			if f.Command != frame.SUBSCRIBE &&
				f.Command != frame.UNSUBSCRIBE {
				return ErrInvalidCommand
			}
			f.Header.Add(key, value)
			return nil
		}
	}
}
