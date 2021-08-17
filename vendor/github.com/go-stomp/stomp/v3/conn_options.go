package stomp

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
	"github.com/go-stomp/stomp/v3/internal/log"
)

// ConnOptions is an opaque structure used to collection options
// for connecting to the other server.
type connOptions struct {
	FrameCommand                              string
	Host                                      string
	ReadTimeout                               time.Duration
	WriteTimeout                              time.Duration
	HeartBeatError                            time.Duration
	MsgSendTimeout                            time.Duration
	RcvReceiptTimeout                         time.Duration
	HeartBeatGracePeriodMultiplier            float64
	Login, Passcode                           string
	AcceptVersions                            []string
	Header                                    *frame.Header
	ReadChannelCapacity, WriteChannelCapacity int
	ReadBufferSize, WriteBufferSize           int
	ResponseHeadersCallback                   func(*frame.Header)
	Logger                                    Logger
}

func newConnOptions(conn *Conn, opts []func(*Conn) error) (*connOptions, error) {
	co := &connOptions{
		FrameCommand:                   frame.CONNECT,
		ReadTimeout:                    time.Minute,
		WriteTimeout:                   time.Minute,
		HeartBeatGracePeriodMultiplier: 1.0,
		HeartBeatError:                 DefaultHeartBeatError,
		MsgSendTimeout:                 DefaultMsgSendTimeout,
		RcvReceiptTimeout:              DefaultRcvReceiptTimeout,
		Logger:                         log.StdLogger{},
	}

	// This is a slight of hand, attach the options to the Conn long
	// enough to run the options functions and then detach again.
	// The reason we do this is to allow for future options to be able
	// to modify the Conn object itself, in case that becomes desirable.
	conn.options = co
	defer func() { conn.options = nil }()

	// compatibility with previous version: ignore nil options
	for _, opt := range opts {
		if opt != nil {
			err := opt(conn)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(co.AcceptVersions) == 0 {
		co.AcceptVersions = append(co.AcceptVersions, string(V10), string(V11), string(V12))
	}

	return co, nil
}

func (co *connOptions) NewFrame() (*frame.Frame, error) {
	f := frame.New(co.FrameCommand)
	if co.Host != "" {
		f.Header.Set(frame.Host, co.Host)
	}

	// heart-beat
	{
		send := co.WriteTimeout / time.Millisecond
		recv := co.ReadTimeout / time.Millisecond
		f.Header.Set(frame.HeartBeat, fmt.Sprintf("%d,%d", send, recv))
	}

	// login, passcode
	if co.Login != "" || co.Passcode != "" {
		f.Header.Set(frame.Login, co.Login)
		f.Header.Set(frame.Passcode, co.Passcode)
	}

	// accept-version
	f.Header.Set(frame.AcceptVersion, strings.Join(co.AcceptVersions, ","))

	// custom header entries -- note that these do not override
	// header values already set as they are added to the end of
	// the header array
	f.Header.AddHeader(co.Header)

	return f, nil
}

// Options for connecting to the STOMP server. Used with the
// stomp.Dial and stomp.Connect functions, both of which have examples.
var ConnOpt struct {
	// Login is a connect option that allows the calling program to
	// specify the "login" and "passcode" values to send to the STOMP
	// server.
	Login func(login, passcode string) func(*Conn) error

	// Host is a connect option that allows the calling program to
	// specify the value of the "host" header.
	Host func(host string) func(*Conn) error

	// UseStomp is a connect option that specifies that the client
	// should use the "STOMP" command instead of the "CONNECT" command.
	// Note that using "STOMP" is only valid for STOMP version 1.1 and later.
	UseStomp func(*Conn) error

	// AcceptVersoin is a connect option that allows the client to
	// specify one or more versions of the STOMP protocol that the
	// client program is prepared to accept. If this option is not
	// specified, the client program will accept any of STOMP versions
	// 1.0, 1.1 or 1.2.
	AcceptVersion func(versions ...Version) func(*Conn) error

	// HeartBeat is a connect option that allows the client to specify
	// the send and receive timeouts for the STOMP heartbeat negotiation mechanism.
	// The sendTimeout parameter specifies the maximum amount of time
	// between the client sending heartbeat notifications from the server.
	// The recvTimeout paramter specifies the minimum amount of time between
	// the client expecting to receive heartbeat notifications from the server.
	// If not specified, this option defaults to one minute for both send and receive
	// timeouts.
	HeartBeat func(sendTimeout, recvTimeout time.Duration) func(*Conn) error

	// HeartBeatError is a connect option that will normally only be specified during
	// testing. It specifies a short time duration that is larger than the amount of time
	// that will take for a STOMP frame to be transmitted from one station to the other.
	// When not specified, this value defaults to 5 seconds. This value is set to a much
	// shorter time duration during unit testing.
	HeartBeatError func(errorTimeout time.Duration) func(*Conn) error

	// MsgSendTimeout is a connect option that allows the client to specify
	// the timeout for the Conn.Send function.
	// The msgSendTimeout parameter specifies maximum blocking time for calling
	// the Conn.Send function.
	// If not specified, this option defaults to 10 seconds.
	// Less than or equal to zero means infinite
	MsgSendTimeout func(msgSendTimeout time.Duration) func(*Conn) error

	// RcvReceiptTimeout is a connect option that allows the client to specify
	// how long to wait for a receipt in the Conn.Send function. This helps
	// avoid deadlocks. If this is not specified, the default is 10 seconds.
	RcvReceiptTimeout func(rcvReceiptTimeout time.Duration) func(*Conn) error

	// HeartBeatGracePeriodMultiplier is used to calculate the effective read heart-beat timeout
	// the broker will enforce for each client’s connection. The multiplier is applied to
	// the read-timeout interval the client specifies in its CONNECT frame
	HeartBeatGracePeriodMultiplier func(multiplier float64) func(*Conn) error

	// Header is a connect option that allows the client to specify a custom
	// header entry in the STOMP frame. This connect option can be specified
	// multiple times for multiple custom headers.
	Header func(key, value string) func(*Conn) error

	// ReadChannelCapacity is the number of messages that can be on the read channel at the
	// same time. A high number may affect memory usage while a too low number may lock the
	// system up. Default is set to 20.
	ReadChannelCapacity func(capacity int) func(*Conn) error

	// WriteChannelCapacity is the number of messages that can be on the write channel at the
	// same time. A high number may affect memory usage while a too low number may lock the
	// system up. Default is set to 20.
	WriteChannelCapacity func(capacity int) func(*Conn) error

	// ReadBufferSize specifies number of bytes that can be used to read the message
	// A high number may affect memory usage while a too low number may lock the
	// system up. Default is set to 4096.
	ReadBufferSize func(size int) func(*Conn) error

	// WriteBufferSize specifies number of bytes that can be used to write the message
	// A high number may affect memory usage while a too low number may lock the
	// system up. Default is set to 4096.
	WriteBufferSize func(size int) func(*Conn) error

	// ResponseHeaders lets you provide a callback function to get the headers from the CONNECT response
	ResponseHeaders func(func(*frame.Header)) func(*Conn) error

	// Logger lets you provide a callback function that sets the logger used by a connection
	Logger func(logger Logger) func(*Conn) error
}

func init() {
	ConnOpt.Login = func(login, passcode string) func(*Conn) error {
		return func(c *Conn) error {
			c.options.Login = login
			c.options.Passcode = passcode
			return nil
		}
	}

	ConnOpt.Host = func(host string) func(*Conn) error {
		return func(c *Conn) error {
			c.options.Host = host
			return nil
		}
	}

	ConnOpt.UseStomp = func(c *Conn) error {
		c.options.FrameCommand = frame.STOMP
		return nil
	}

	ConnOpt.AcceptVersion = func(versions ...Version) func(*Conn) error {
		return func(c *Conn) error {
			for _, version := range versions {
				if err := version.CheckSupported(); err != nil {
					return err
				}
				c.options.AcceptVersions = append(c.options.AcceptVersions, string(version))
			}
			return nil
		}
	}

	ConnOpt.HeartBeat = func(sendTimeout, recvTimeout time.Duration) func(*Conn) error {
		return func(c *Conn) error {
			c.options.WriteTimeout = sendTimeout
			c.options.ReadTimeout = recvTimeout
			return nil
		}
	}

	ConnOpt.HeartBeatError = func(errorTimeout time.Duration) func(*Conn) error {
		return func(c *Conn) error {
			c.options.HeartBeatError = errorTimeout
			return nil
		}
	}

	ConnOpt.MsgSendTimeout = func(msgSendTimeout time.Duration) func(*Conn) error {
		return func(c *Conn) error {
			c.options.MsgSendTimeout = msgSendTimeout
			return nil
		}
	}

	ConnOpt.RcvReceiptTimeout = func(rcvReceiptTimeout time.Duration) func(*Conn) error {
		return func(c *Conn) error {
			c.options.RcvReceiptTimeout = rcvReceiptTimeout
			return nil
		}
	}

	ConnOpt.HeartBeatGracePeriodMultiplier = func(multiplier float64) func(*Conn) error {
		return func(c *Conn) error {
			c.options.HeartBeatGracePeriodMultiplier = multiplier
			return nil
		}
	}

	ConnOpt.Header = func(key, value string) func(*Conn) error {
		return func(c *Conn) error {
			if c.options.Header == nil {
				c.options.Header = frame.NewHeader(key, value)
			} else {
				c.options.Header.Add(key, value)
			}
			return nil
		}
	}

	ConnOpt.ReadChannelCapacity = func(capacity int) func(*Conn) error {
		return func(c *Conn) error {
			c.options.ReadChannelCapacity = capacity
			return nil
		}
	}

	ConnOpt.WriteChannelCapacity = func(capacity int) func(*Conn) error {
		return func(c *Conn) error {
			c.options.WriteChannelCapacity = capacity
			return nil
		}
	}

	ConnOpt.ReadBufferSize = func(size int) func(*Conn) error {
		return func(c *Conn) error {
			c.options.ReadBufferSize = size
			return nil
		}
	}

	ConnOpt.WriteBufferSize = func(size int) func(*Conn) error {
		return func(c *Conn) error {
			c.options.WriteBufferSize = size
			return nil
		}
	}

	ConnOpt.ResponseHeaders = func(callback func(*frame.Header)) func(*Conn) error {
		return func(c *Conn) error {
			c.options.ResponseHeadersCallback = callback
			return nil
		}
	}

	ConnOpt.Logger = func(log Logger) func(*Conn) error {
		return func(c *Conn) error {
			if log != nil {
				c.log = log
			}

			return nil
		}
	}
}
