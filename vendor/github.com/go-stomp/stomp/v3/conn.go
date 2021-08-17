package stomp

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

// Default time span to add to read/write heart-beat timeouts
// to avoid premature disconnections due to network latency.
const DefaultHeartBeatError = 5 * time.Second

// Default send timeout in Conn.Send function
const DefaultMsgSendTimeout = 10 * time.Second

// Default receipt timeout in Conn.Send function
const DefaultRcvReceiptTimeout = 30 * time.Second

// A Conn is a connection to a STOMP server. Create a Conn using either
// the Dial or Connect function.
type Conn struct {
	conn                    io.ReadWriteCloser
	readCh                  chan *frame.Frame
	writeCh                 chan writeRequest
	version                 Version
	session                 string
	server                  string
	readTimeout             time.Duration
	writeTimeout            time.Duration
	msgSendTimeout          time.Duration
	rcvReceiptTimeout       time.Duration
	hbGracePeriodMultiplier float64
	closed                  bool
	closeMutex              *sync.Mutex
	options                 *connOptions
	log                     Logger
}

type writeRequest struct {
	Frame *frame.Frame      // frame to send
	C     chan *frame.Frame // response channel
}

// Dial creates a network connection to a STOMP server and performs
// the STOMP connect protocol sequence. The network endpoint of the
// STOMP server is specified by network and addr. STOMP protocol
// options can be specified in opts.
func Dial(network, addr string, opts ...func(*Conn) error) (*Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		c.Close()
		return nil, err
	}

	// Add option to set host and make it the first option in list,
	// so that if host has been explicitly specified it will override.
	opts = append([]func(*Conn) error{ConnOpt.Host(host)}, opts...)

	return Connect(c, opts...)
}

// Connect creates a STOMP connection and performs the STOMP connect
// protocol sequence. The connection to the STOMP server has already
// been created by the program. The opts parameter provides the
// opportunity to specify STOMP protocol options.
func Connect(conn io.ReadWriteCloser, opts ...func(*Conn) error) (*Conn, error) {
	reader := frame.NewReader(conn)
	writer := frame.NewWriter(conn)

	c := &Conn{
		conn:       conn,
		closeMutex: &sync.Mutex{},
	}

	options, err := newConnOptions(c, opts)
	if err != nil {
		return nil, err
	}

	c.log = options.Logger

	if options.ReadBufferSize > 0 {
		reader = frame.NewReaderSize(conn, options.ReadBufferSize)
	}

	if options.WriteBufferSize > 0 {
		writer = frame.NewWriterSize(conn, options.ReadBufferSize)
	}

	readChannelCapacity := 20
	writeChannelCapacity := 20

	if options.ReadChannelCapacity > 0 {
		readChannelCapacity = options.ReadChannelCapacity
	}

	if options.WriteChannelCapacity > 0 {
		writeChannelCapacity = options.WriteChannelCapacity
	}

	c.hbGracePeriodMultiplier = options.HeartBeatGracePeriodMultiplier

	c.readCh = make(chan *frame.Frame, readChannelCapacity)
	c.writeCh = make(chan writeRequest, writeChannelCapacity)

	if options.Host == "" {
		// host not specified yet, attempt to get from net.Conn if possible
		if connection, ok := conn.(net.Conn); ok {
			host, _, err := net.SplitHostPort(connection.RemoteAddr().String())
			if err == nil {
				options.Host = host
			}
		}
		// if host is still blank, use default
		if options.Host == "" {
			options.Host = "default"
		}
	}

	connectFrame, err := options.NewFrame()
	if err != nil {
		return nil, err
	}

	err = writer.Write(connectFrame)
	if err != nil {
		return nil, err
	}

	response, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("unexpected empty frame")
	}

	if response.Command != frame.CONNECTED {
		return nil, newError(response)
	}

	c.server = response.Header.Get(frame.Server)
	c.session = response.Header.Get(frame.Session)

	if versionString := response.Header.Get(frame.Version); versionString != "" {
		version := Version(versionString)
		if err = version.CheckSupported(); err != nil {
			return nil, Error{
				Message: err.Error(),
				Frame:   response,
			}
		}
		c.version = version
	} else {
		// no version in the response, so assume version 1.0
		c.version = V10
	}

	if heartBeat, ok := response.Header.Contains(frame.HeartBeat); ok {
		readTimeout, writeTimeout, err := frame.ParseHeartBeat(heartBeat)
		if err != nil {
			return nil, Error{
				Message: err.Error(),
				Frame:   response,
			}
		}

		c.readTimeout = readTimeout
		c.writeTimeout = writeTimeout

		if c.readTimeout > 0 {
			// Add time to the read timeout to account for time
			// delay in other station transmitting timeout
			c.readTimeout += options.HeartBeatError
		}
		if c.writeTimeout > options.HeartBeatError {
			// Reduce time from the write timeout to account
			// for time delay in transmitting to the other station
			c.writeTimeout -= options.HeartBeatError
		}
	}

	c.msgSendTimeout = options.MsgSendTimeout
	c.rcvReceiptTimeout = options.RcvReceiptTimeout

	if options.ResponseHeadersCallback != nil {
		options.ResponseHeadersCallback(response.Header)
	}

	go readLoop(c, reader)
	go processLoop(c, writer)

	return c, nil
}

// Version returns the version of the STOMP protocol that
// is being used to communicate with the STOMP server. This
// version is negotiated with the server during the connect sequence.
func (c *Conn) Version() Version {
	return c.version
}

// Session returns the session identifier, which can be
// returned by the STOMP server during the connect sequence.
// If the STOMP server does not return a session header entry,
// this value will be a blank string.
func (c *Conn) Session() string {
	return c.session
}

// Server returns the STOMP server identification, which can
// be returned by the STOMP server during the connect sequence.
// If the STOMP server does not return a server header entry,
// this value will be a blank string.
func (c *Conn) Server() string {
	return c.server
}

// readLoop is a goroutine that reads frames from the
// reader and places them onto a channel for processing
// by the processLoop goroutine
func readLoop(c *Conn, reader *frame.Reader) {
	for {
		f, err := reader.Read()
		if err != nil {
			close(c.readCh)
			return
		}
		c.readCh <- f
	}
}

// processLoop is a goroutine that handles io with
// the server.
func processLoop(c *Conn, writer *frame.Writer) {
	channels := make(map[string]chan *frame.Frame)

	var readTimeoutChannel <-chan time.Time
	var readTimer *time.Timer
	var writeTimeoutChannel <-chan time.Time
	var writeTimer *time.Timer

	defer c.MustDisconnect()

	for {
		if c.readTimeout > 0 && readTimer == nil {
			readTimer = time.NewTimer(time.Duration(float64(c.readTimeout) * c.hbGracePeriodMultiplier))
			readTimeoutChannel = readTimer.C
		}
		if c.writeTimeout > 0 && writeTimer == nil {
			writeTimer = time.NewTimer(c.writeTimeout)
			writeTimeoutChannel = writeTimer.C
		}

		select {
		case <-readTimeoutChannel:
			// read timeout, close the connection
			err := newErrorMessage("read timeout")
			sendError(channels, err)
			return

		case <-writeTimeoutChannel:
			// write timeout, send a heart-beat frame
			err := writer.Write(nil)
			if err != nil {
				sendError(channels, err)
				return
			}
			writeTimer = nil
			writeTimeoutChannel = nil

		case f, ok := <-c.readCh:
			// stop the read timer
			if readTimer != nil {
				readTimer.Stop()
				readTimer = nil
				readTimeoutChannel = nil
			}

			if !ok {
				err := newErrorMessage("connection closed")
				sendError(channels, err)
				return
			}

			if f == nil {
				// heart-beat received
				continue
			}

			switch f.Command {
			case frame.RECEIPT:
				if id, ok := f.Header.Contains(frame.ReceiptId); ok {
					if ch, ok := channels[id]; ok {
						ch <- f
						delete(channels, id)
						close(ch)
					}
				} else {
					err := &Error{Message: "missing receipt-id", Frame: f}
					sendError(channels, err)
					return
				}

			case frame.ERROR:
				c.log.Error("received ERROR; Closing underlying connection")
				for _, ch := range channels {
					ch <- f
					close(ch)
				}

				c.closeMutex.Lock()
				defer c.closeMutex.Unlock()
				c.closed = true
				c.conn.Close()

				return

			case frame.MESSAGE:
				if id, ok := f.Header.Contains(frame.Subscription); ok {
					if ch, ok := channels[id]; ok {
						ch <- f
					} else {
						c.log.Infof("ignored MESSAGE for subscription: %s", id)
					}
				}
			}

		case req, ok := <-c.writeCh:
			// stop the write timeout
			if writeTimer != nil {
				writeTimer.Stop()
				writeTimer = nil
				writeTimeoutChannel = nil
			}
			if !ok {
				sendError(channels, errors.New("write channel closed"))
				return
			}
			if req.C != nil {
				if receipt, ok := req.Frame.Header.Contains(frame.Receipt); ok {
					// remember the channel for this receipt
					channels[receipt] = req.C
				}
			}

			switch req.Frame.Command {
			case frame.SUBSCRIBE:
				id, _ := req.Frame.Header.Contains(frame.Id)
				channels[id] = req.C
			case frame.UNSUBSCRIBE:
				id, _ := req.Frame.Header.Contains(frame.Id)
				// is this trying to be too clever -- add a receipt
				// header so that when the server responds with a
				// RECEIPT frame, the corresponding channel will be closed
				req.Frame.Header.Set(frame.Receipt, id)
			}

			// frame to send
			err := writer.Write(req.Frame)
			if err != nil {
				sendError(channels, err)
				return
			}
		}
	}
}

// Send an error to all receipt channels.
func sendError(m map[string]chan *frame.Frame, err error) {
	frame := frame.New(frame.ERROR, frame.Message, err.Error())
	for _, ch := range m {
		ch <- frame
	}
}

// Disconnect will disconnect from the STOMP server. This function
// follows the STOMP standard's recommended protocol for graceful
// disconnection: it sends a DISCONNECT frame with a receipt header
// element. Once the RECEIPT frame has been received, the connection
// with the STOMP server is closed and any further attempt to write
// to the server will fail.
func (c *Conn) Disconnect() error {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if c.closed {
		return nil
	}

	ch := make(chan *frame.Frame)
	c.writeCh <- writeRequest{
		Frame: frame.New(frame.DISCONNECT, frame.Receipt, allocateId()),
		C:     ch,
	}

	response := <-ch
	if response.Command != frame.RECEIPT {
		return newError(response)
	}

	c.closed = true
	return c.conn.Close()
}

// MustDisconnect will disconnect 'ungracefully' from the STOMP server.
// This method should be used only as last resort when there are fatal
// network errors that prevent to do a proper disconnect from the server.
func (c *Conn) MustDisconnect() error {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if c.closed {
		return nil
	}

	// just close writeCh
	close(c.writeCh)

	c.closed = true
	return c.conn.Close()
}

// Send sends a message to the STOMP server, which in turn sends the message to the specified destination.
// If the STOMP server fails to receive the message for any reason, the connection will close.
//
// The content type should be specified, according to the STOMP specification, but if contentType is an empty
// string, the message will be delivered without a content-type header entry. The body array contains the
// message body, and its content should be consistent with the specified content type.
//
// Any number of options can be specified in opts. See the examples for usage. Options include whether
// to receive a RECEIPT, should the content-length be suppressed, and sending custom header entries.
func (c *Conn) Send(destination, contentType string, body []byte, opts ...func(*frame.Frame) error) error {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if c.closed {
		return ErrAlreadyClosed
	}

	f, err := createSendFrame(destination, contentType, body, opts)
	if err != nil {
		return err
	}

	if _, ok := f.Header.Contains(frame.Receipt); ok {
		// receipt required
		request := writeRequest{
			Frame: f,
			C:     make(chan *frame.Frame),
		}

		err := sendDataToWriteChWithTimeout(c.writeCh, request, c.msgSendTimeout)
		if err != nil {
			return err
		}

		err = readReceiptWithTimeout(request, c.rcvReceiptTimeout)
		if err != nil {
			return err
		}
	} else {
		// no receipt required
		request := writeRequest{Frame: f}

		err := sendDataToWriteChWithTimeout(c.writeCh, request, c.msgSendTimeout)
		if err != nil {
			return err
		}
	}

	return nil
}

func readReceiptWithTimeout(request writeRequest, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case <-timeoutChan:
		return ErrMsgReceiptTimeout
	case response := <-request.C:
		if response.Command != frame.RECEIPT {
			return newError(response)
		}
		return nil
	}
}

func sendDataToWriteChWithTimeout(ch chan writeRequest, request writeRequest, timeout time.Duration) error {
	if timeout <= 0 {
		ch <- request
		return nil
	}

	timer := time.NewTimer(timeout)
	select {
	case <-timer.C:
		return ErrMsgSendTimeout
	case ch <- request:
		timer.Stop()
		return nil
	}
}

func createSendFrame(destination, contentType string, body []byte, opts []func(*frame.Frame) error) (*frame.Frame, error) {
	// Set the content-length before the options, because this provides
	// an opportunity to remove content-length.
	f := frame.New(frame.SEND, frame.ContentLength, strconv.Itoa(len(body)))
	f.Body = body
	f.Header.Set(frame.Destination, destination)
	if contentType != "" {
		f.Header.Set(frame.ContentType, contentType)
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(f); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (c *Conn) sendFrame(f *frame.Frame) error {
	// Lock our mutex, but don't close it via defer
	// If the frame requests a receipt then we want to release the lock before
	// we block on the response, otherwise we can end up deadlocking
	c.closeMutex.Lock()
	if c.closed {
		c.closeMutex.Unlock()
		c.conn.Close()
		return ErrClosedUnexpectedly
	}

	if _, ok := f.Header.Contains(frame.Receipt); ok {
		// receipt required
		request := writeRequest{
			Frame: f,
			C:     make(chan *frame.Frame),
		}

		c.writeCh <- request

		// Now that we've written to the writeCh channel we can release the
		// close mutex while we wait for our response
		c.closeMutex.Unlock()

		var response *frame.Frame

		if c.writeTimeout > 0 {
			select {
			case response, ok = <-request.C:
			case <-time.After(c.writeTimeout):
				ok = false
			}
		} else {
			response, ok = <-request.C
		}

		if ok {
			if response.Command != frame.RECEIPT {
				return newError(response)
			}
		} else {
			return ErrClosedUnexpectedly
		}
	} else {
		// no receipt required
		request := writeRequest{Frame: f}
		c.writeCh <- request

		// Unlock the mutex now that we're written to the write channel
		c.closeMutex.Unlock()
	}

	return nil
}

// Subscribe creates a subscription on the STOMP server.
// The subscription has a destination, and messages sent to that destination
// will be received by this subscription. A subscription has a channel
// on which the calling program can receive messages.
func (c *Conn) Subscribe(destination string, ack AckMode, opts ...func(*frame.Frame) error) (*Subscription, error) {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if c.closed {
		c.conn.Close()
		return nil, ErrClosedUnexpectedly
	}

	ch := make(chan *frame.Frame)

	subscribeFrame := frame.New(frame.SUBSCRIBE,
		frame.Destination, destination,
		frame.Ack, ack.String())

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		err := opt(subscribeFrame)
		if err != nil {
			return nil, err
		}
	}

	// If the option functions have not specified the "id" header entry,
	// create one.
	id, ok := subscribeFrame.Header.Contains(frame.Id)
	if !ok {
		id = allocateId()
		subscribeFrame.Header.Add(frame.Id, id)
	}

	request := writeRequest{
		Frame: subscribeFrame,
		C:     ch,
	}

	closeMutex := &sync.Mutex{}
	sub := &Subscription{
		id:          id,
		destination: destination,
		conn:        c,
		ackMode:     ack,
		C:           make(chan *Message, 16),
		closeMutex:  closeMutex,
		closeCond:   sync.NewCond(closeMutex),
	}
	go sub.readLoop(ch)

	// TODO is this safe? There is no check if writeCh is actually open.
	c.writeCh <- request
	return sub, nil
}

// TODO check further for race conditions

// Ack acknowledges a message received from the STOMP server.
// If the message was received on a subscription with AckMode == AckAuto,
// then no operation is performed.
func (c *Conn) Ack(m *Message) error {
	f, err := c.createAckNackFrame(m, true)
	if err != nil {
		return err
	}

	if f != nil {
		return c.sendFrame(f)
	}
	return nil
}

// Nack indicates to the server that a message was not received
// by the client. Returns an error if the STOMP version does not
// support the NACK message.
func (c *Conn) Nack(m *Message) error {
	f, err := c.createAckNackFrame(m, false)
	if err != nil {
		return err
	}

	if f != nil {
		return c.sendFrame(f)
	}
	return nil
}

// Begin is used to start a transaction. Transactions apply to sending
// and acknowledging. Any messages sent or acknowledged during a transaction
// will be processed atomically by the STOMP server based on the transaction.
func (c *Conn) Begin() *Transaction {
	t, _ := c.BeginWithError()
	return t
}

// BeginWithError is used to start a transaction, but also returns the error
// (if any) from sending the frame to start the transaction.
func (c *Conn) BeginWithError() (*Transaction, error) {
	id := allocateId()
	f := frame.New(frame.BEGIN, frame.Transaction, id)
	err := c.sendFrame(f)
	return &Transaction{id: id, conn: c}, err
}

// Create an ACK or NACK frame. Complicated by version incompatibilities.
func (c *Conn) createAckNackFrame(msg *Message, ack bool) (*frame.Frame, error) {
	if !ack && !c.version.SupportsNack() {
		return nil, ErrNackNotSupported
	}

	if msg.Header == nil || msg.Subscription == nil || msg.Conn == nil {
		return nil, ErrNotReceivedMessage
	}

	if msg.Subscription.AckMode() == AckAuto {
		if ack {
			// not much point sending an ACK to an auto subscription
			return nil, nil
		} else {
			// sending a NACK for an ack:auto subscription makes no
			// sense
			return nil, ErrCannotNackAutoSub
		}
	}

	var f *frame.Frame
	if ack {
		f = frame.New(frame.ACK)
	} else {
		f = frame.New(frame.NACK)
	}

	switch c.version {
	case V10, V11:
		f.Header.Add(frame.Subscription, msg.Subscription.Id())
		if messageId, ok := msg.Header.Contains(frame.MessageId); ok {
			f.Header.Add(frame.MessageId, messageId)
		} else {
			return nil, missingHeader(frame.MessageId)
		}
	case V12:
		if ack, ok := msg.Header.Contains(frame.Ack); ok {
			f.Header.Add(frame.Id, ack)
		} else {
			return nil, missingHeader(frame.Ack)
		}
	}

	return f, nil
}
