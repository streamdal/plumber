package stomp

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/go-stomp/stomp/frame"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

// The Subscription type represents a client subscription to
// a destination. The subscription is created by calling Conn.Subscribe.
//
// Once a client has subscribed, it can receive messages from the C channel.
type Subscription struct {
	C           chan *Message
	id          string
	destination string
	conn        *Conn
	ackMode     AckMode
	state       int32
	closeMutex  *sync.Mutex
	closeCond   *sync.Cond
}

// BUG(jpj): If the client does not read messages from the Subscription.C
// channel quickly enough, the client will stop reading messages from the
// server.

// Identification for this subscription. Unique among
// all subscriptions for the same Client.
func (s *Subscription) Id() string {
	return s.id
}

// Destination for which the subscription applies.
func (s *Subscription) Destination() string {
	return s.destination
}

// AckMode returns the Acknowledgement mode specified when the
// subscription was created.
func (s *Subscription) AckMode() AckMode {
	return s.ackMode
}

// Active returns whether the subscription is still active.
// Returns false if the subscription has been unsubscribed.
func (s *Subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

// Unsubscribes and closes the channel C.
func (s *Subscription) Unsubscribe(opts ...func(*frame.Frame) error) error {
	// transition to the "closing" state
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}

	f := frame.New(frame.UNSUBSCRIBE, frame.Id, s.id)

	for _, opt := range opts {
		if opt == nil {
			return ErrNilOption
		}
		err := opt(f)
		if err != nil {
			return err
		}
	}

	s.conn.sendFrame(f)

	// UNSUBSCRIBE is a bit weird in that it is tagged with a "receipt" header
	// on the I/O goroutine, so the above call to sendFrame() will not wait
	// for the resulting RECEIPT.
	//
	// We don't want to interfere with `s.C` since we might be "stealing"
	// MESSAGEs or ERRORs from another goroutine, so use a sync.Cond to
	// wait for the terminal state transition instead.
	s.closeMutex.Lock()
	for atomic.LoadInt32(&s.state) != subStateClosed {
		s.closeCond.Wait()
	}
	s.closeMutex.Unlock()
	return nil
}

// Read a message from the subscription. This is a convenience
// method: many callers will prefer to read from the channel C
// directly.
func (s *Subscription) Read() (*Message, error) {
	if !s.Active() {
		return nil, ErrCompletedSubscription
	}
	msg, ok := <-s.C
	if !ok {
		return nil, ErrCompletedSubscription
	}
	if msg.Err != nil {
		return nil, msg.Err
	}
	return msg, nil
}

func (s *Subscription) closeChannel(msg *Message) {
	if msg != nil {
		s.C <- msg
	}
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.C)
	s.closeCond.Broadcast()
}

func (s *Subscription) readLoop(ch chan *frame.Frame) {
	for {
		f, ok := <-ch
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				msg := &Message{
					Err: &Error{
						Message: fmt.Sprintf("Subscription %s: %s: channel read failed", s.id, s.destination),
					},
				}
				s.closeChannel(msg)
			}
			return
		}

		if f.Command == frame.MESSAGE {
			destination := f.Header.Get(frame.Destination)
			contentType := f.Header.Get(frame.ContentType)
			msg := &Message{
				Destination:  destination,
				ContentType:  contentType,
				Conn:         s.conn,
				Subscription: s,
				Header:       f.Header,
				Body:         f.Body,
			}
			s.C <- msg
		} else if f.Command == frame.ERROR {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				message, _ := f.Header.Contains(frame.Message)
				text := fmt.Sprintf("Subscription %s: %s: ERROR message:%s",
					s.id,
					s.destination,
					message)
				log.Println(text)
				contentType := f.Header.Get(frame.ContentType)
				msg := &Message{
					Err: &Error{
						Message: f.Header.Get(frame.Message),
						Frame:   f,
					},
					ContentType:  contentType,
					Conn:         s.conn,
					Subscription: s,
					Header:       f.Header,
					Body:         f.Body,
				}
				s.closeChannel(msg)
			}
			return
		} else if f.Command == frame.RECEIPT {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeChannel(nil)
			}
			return
		} else {
			log.Printf("Subscription %s: %s: unsupported frame type: %+v\n", s.id, s.destination, f)
		}
	}
}
