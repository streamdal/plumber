package servicebus

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"errors"
	"sync"
	"time"

	common "github.com/Azure/azure-amqp-common-go/v3"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	"github.com/Azure/go-amqp"
	"github.com/devigned/tab"
)

type (
	// Sender provides connection, session and link handling for an sending to an entity path
	Sender struct {
		namespace          *Namespace
		client             *amqp.Client
		clientMu           sync.RWMutex
		session            *session
		sender             *amqp.Sender
		entityPath         string
		Name               string
		sessionID          *string
		doneRefreshingAuth func()
	}

	// SendOption provides a way to customize a message on sending
	SendOption func(event *Message) error

	eventer interface {
		toMsg() (*amqp.Message, error)
		GetKeyValues() map[string]interface{}
		Set(key string, value interface{})
	}

	// SenderOption provides a way to customize a Sender
	SenderOption func(*Sender) error
)

// NewSender creates a new Service Bus message Sender given an AMQP client and entity path
func (ns *Namespace) NewSender(ctx context.Context, entityPath string, opts ...SenderOption) (*Sender, error) {
	ctx, span := ns.startSpanFromContext(ctx, "sb.Namespace.NewSender")
	defer span.End()

	s := &Sender{
		namespace:  ns,
		entityPath: entityPath,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			tab.For(ctx).Error(err)
			return nil, err
		}
	}

	// no need to take the write lock as we're creating a new Sender
	err := s.newSessionAndLink(ctx)
	if err != nil {
		tab.For(ctx).Error(err)
	}

	s.periodicallyRefreshAuth()

	return s, err
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *Sender) Recover(ctx context.Context) error {
	ctx, span := s.startProducerSpanFromContext(ctx, "sb.Sender.Recover")
	defer span.End()

	// we expect the Sender, session or client is in an error state, ignore errors
	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	closeCtx = tab.NewContext(closeCtx, span)
	defer cancel()
	// we must close then rebuild the session/link atomically
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	_ = s.close(closeCtx)
	return s.newSessionAndLink(ctx)
}

// Close will close the AMQP connection, session and link of the Sender
func (s *Sender) Close(ctx context.Context) error {
	ctx, span := s.startProducerSpanFromContext(ctx, "sb.Sender.Close")
	defer span.End()

	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	return s.close(ctx)
}

// closes the connection.  callers *must* hold the client write lock before calling!
func (s *Sender) close(ctx context.Context) error {
	if s.doneRefreshingAuth != nil {
		s.doneRefreshingAuth()
	}

	var lastErr error
	if s.sender != nil {
		if lastErr = s.sender.Close(ctx); lastErr != nil {
			tab.For(ctx).Error(lastErr)
		}
	}

	if s.session != nil {
		if err := s.session.Close(ctx); err != nil {
			tab.For(ctx).Error(err)
			lastErr = err
		}
	}

	if s.client != nil {
		if err := s.client.Close(); err != nil {
			tab.For(ctx).Error(err)
			lastErr = err
		}
	}

	s.sender = nil
	s.session = nil
	s.client = nil

	return lastErr
}

// Send will send a message to the entity path with options
//
// This will retry sending the message if the server responds with a busy error.
func (s *Sender) Send(ctx context.Context, msg *Message, opts ...SendOption) error {
	ctx, span := s.startProducerSpanFromContext(ctx, "sb.Sender.Send")
	defer span.End()

	if msg.SessionID == nil {
		s.clientMu.RLock()
		if s.session == nil {
			// another goroutine has closed the connection
			s.clientMu.RUnlock()
			return s.connClosedError(ctx)
		}
		msg.SessionID = &s.session.SessionID
		next := s.session.getNext()
		s.clientMu.RUnlock()
		msg.GroupSequence = &next
	}

	if msg.ID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			tab.For(ctx).Error(err)
			return err
		}
		msg.ID = id.String()
	}

	for _, opt := range opts {
		err := opt(msg)
		if err != nil {
			tab.For(ctx).Error(err)
			return err
		}
	}

	return s.trySend(ctx, msg)
}

func (s *Sender) trySend(ctx context.Context, evt eventer) error {
	ctx, sp := s.startProducerSpanFromContext(ctx, "sb.Sender.trySend")
	defer sp.End()

	err := sp.Inject(evt)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	msg, err := evt.toMsg()
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	if msg.Properties != nil {
		sp.AddAttributes(tab.StringAttribute("sb.message.id", msg.Properties.MessageID.(string)))
	}

	for {
		select {
		case <-ctx.Done():
			if err = ctx.Err(); err != nil {
				tab.For(ctx).Error(err)
			}
			return err
		default:
			// try as long as the context is not dead
			s.clientMu.RLock()
			if s.sender == nil {
				// another goroutine has closed the connection
				s.clientMu.RUnlock()
				return s.connClosedError(ctx)
			}
			err = s.sender.Send(ctx, msg)
			s.clientMu.RUnlock()
			if err == nil {
				// successful send
				return err
			}

			switch err.(type) {
			case *amqp.Error, *amqp.DetachError:
				err = s.handleAMQPError(ctx, err)
				if err != nil {
					tab.For(ctx).Error(err)
					return err
				}
			default:
				tab.For(ctx).Error(err)
				return err
			}
		}
	}
}

// handleAMQPError is called internally when an event has failed to send so we
// can parse the error to determine whether we should attempt to retry sending the event again.
func (s *Sender) handleAMQPError(ctx context.Context, err error) error {
	var amqpError *amqp.Error
	if errors.As(err, &amqpError) {
		switch amqpError.Condition {
		case errorServerBusy:
			return s.retryRetryableAmqpError(ctx, amqpRetryDefaultTimes, amqpRetryBusyServerDelay)
		case errorTimeout:
			return s.retryRetryableAmqpError(ctx, amqpRetryDefaultTimes, amqpRetryDefaultDelay)
		case errorOperationCancelled:
			return s.retryRetryableAmqpError(ctx, amqpRetryDefaultTimes, amqpRetryDefaultDelay)
		case errorContainerClose:
			return s.retryRetryableAmqpError(ctx, amqpRetryDefaultTimes, amqpRetryDefaultDelay)
		default:
			return err
		}
	}
	return s.retryRetryableAmqpError(ctx, amqpRetryDefaultTimes, amqpRetryDefaultDelay)
}

func (s *Sender) retryRetryableAmqpError(ctx context.Context, times int, delay time.Duration) error {
	tab.For(ctx).Debug("recovering sender connection")
	_, retryErr := common.Retry(times, delay, func() (interface{}, error) {
		ctx, sp := s.startProducerSpanFromContext(ctx, "sb.Sender.trySend.tryRecover")
		defer sp.End()

		err := s.Recover(ctx)
		if err == nil {
			tab.For(ctx).Debug("recovered connection")
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, common.Retryable(err.Error())
		}
	})
	if retryErr != nil {
		tab.For(ctx).Debug("sender recovering retried, but error was unrecoverable")
		return retryErr
	}
	return nil
}

func (s *Sender) connClosedError(ctx context.Context) error {
	name := "Sender"
	if s.Name != "" {
		name = s.Name
	}
	err := ErrConnectionClosed(name)
	tab.For(ctx).Error(err)
	return err
}

func (s *Sender) String() string {
	return s.Name
}

func (s *Sender) getAddress() string {
	return s.entityPath
}

func (s *Sender) getFullIdentifier() string {
	return s.namespace.getEntityAudience(s.getAddress())
}

// newSessionAndLink will replace the existing session and link
// NOTE: this does *not* take the write lock, callers must hold it as required!
func (s *Sender) newSessionAndLink(ctx context.Context) error {
	ctx, span := s.startProducerSpanFromContext(ctx, "sb.Sender.newSessionAndLink")
	defer span.End()

	connection, err := s.namespace.newClient(ctx)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}
	s.client = connection

	err = s.namespace.negotiateClaim(ctx, connection, s.getAddress())
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	amqpSender, err := amqpSession.NewSender(
		amqp.LinkSenderSettle(amqp.ModeMixed),
		amqp.LinkReceiverSettle(amqp.ModeFirst),
		amqp.LinkTargetAddress(s.getAddress()))
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}

	s.session, err = newSession(amqpSession)
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}
	if s.sessionID != nil {
		s.session.SessionID = *s.sessionID
	}

	s.sender = amqpSender
	return nil
}

func (s *Sender) periodicallyRefreshAuth() {
	ctx, done := context.WithCancel(context.Background())
	s.doneRefreshingAuth = done

	ctx, span := s.startProducerSpanFromContext(ctx, "sb.Sender.periodicallyRefreshAuth")
	defer span.End()

	doNegotiateClaimLocked := func(ctx context.Context, r *Sender) {
		r.clientMu.RLock()
		defer r.clientMu.RUnlock()

		if r.client != nil {
			if err := r.namespace.negotiateClaim(ctx, r.client, r.entityPath); err != nil {
				tab.For(ctx).Error(err)
			}
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(5 * time.Minute)
				doNegotiateClaimLocked(ctx, s)
			}
		}
	}()
}

// SenderWithSession configures the message to send with a specific session and sequence. By default, a Sender has a
// default session (uuid.NewV4()) and sequence generator.
func SenderWithSession(sessionID *string) SenderOption {
	return func(sender *Sender) error {
		sender.sessionID = sessionID
		return nil
	}
}
